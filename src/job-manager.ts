import { spawn, type ChildProcess } from "node:child_process";
import { setTimeout as delay } from "node:timers/promises";

export const MAX_TIMEOUT_MS = 2_147_483_647;
export const MAX_COMMAND_LENGTH = 100_000;
export const MAX_CWD_LENGTH = 4_096;
export const MAX_LABEL_LENGTH = 240;

export type JobState =
	| "running"
	| "completed"
	| "failed"
	| "stopped"
	| "timed_out";
export type StopReason = "user" | "timeout" | "shutdown";

export interface BackgroundJob {
	id: string;
	command: string;
	cwd: string;
	label?: string;
	state: JobState;
	startedAt: number;
	endedAt?: number;
	exitCode?: number | null;
	signal?: NodeJS.Signals | null;
	timeoutMs?: number;
	stopReason?: StopReason;
	bytesCaptured: number;
	outputTruncated: boolean;
}

export interface StartOptions {
	command: string;
	cwd?: string;
	label?: string;
	timeoutMs?: number;
}
export interface JobManagerOptions {
	maxTailBytes?: number;
	maxRecent?: number;
	killGraceMs?: number;
	shell?: string;
	now?: () => number;
	/** Receives exceptions thrown by change or completion listeners. */
	onListenerError?: (error: unknown) => void;
}

export interface JobsChangedPayload {
	runningCount: number;
	terminalRecentCount: number;
	oldestStart?: number;
	primary?: { id: string; label?: string; command: string; startedAt: number };
}

type InternalJob = BackgroundJob & {
	child?: ChildProcess;
	tail: Buffer;
	utf8Pending: Buffer;
	timeoutTimer?: ReturnType<typeof setTimeout>;
	killTimer?: ReturnType<typeof setTimeout>;
	processGroupId?: number;
	finishing?: boolean;
};

const defaults = {
	maxTailBytes: 16 * 1024,
	maxRecent: 40,
	killGraceMs: 750,
};

type LifecycleState = "open" | "shutting-down" | "closed";
type Listener<Args extends unknown[]> = (...args: Args) => void | Promise<void>;

/** A process manager deliberately limited to non-interactive `shell -c` jobs. */
export class JobManager {
	private readonly options: Required<
		Omit<JobManagerOptions, "shell" | "now" | "onListenerError">
	> &
		Pick<JobManagerOptions, "shell" | "now" | "onListenerError">;
	private jobs = new Map<string, InternalJob>();
	private sequence = 0;
	private changeListeners = new Set<Listener<[JobsChangedPayload]>>();
	private completionListeners = new Set<Listener<[BackgroundJob, string]>>();
	private lifecycleState: LifecycleState = "open";
	private lifecycleQueue: Promise<void> = Promise.resolve();
	private shutdownPromise?: Promise<void>;

	constructor(options: JobManagerOptions = {}) {
		this.options = {
			...defaults,
			...options,
			maxTailBytes: validateNonNegativeSafeInteger(
				options.maxTailBytes ?? defaults.maxTailBytes,
				"maxTailBytes",
			),
			maxRecent: validateNonNegativeSafeInteger(
				options.maxRecent ?? defaults.maxRecent,
				"maxRecent",
			),
			killGraceMs: validateNonNegativeSafeInteger(
				options.killGraceMs ?? defaults.killGraceMs,
				"killGraceMs",
			),
		};
	}

	onChanged(listener: Listener<[JobsChangedPayload]>): () => void {
		this.changeListeners.add(listener);
		return () => this.changeListeners.delete(listener);
	}

	onCompleted(listener: Listener<[BackgroundJob, string]>): () => void {
		this.completionListeners.add(listener);
		return () => this.completionListeners.delete(listener);
	}

	async start(input: StartOptions): Promise<BackgroundJob> {
		const snapshot = Object.freeze({ ...input });
		this.validateStart(snapshot);
		return this.enqueueLifecycle(() => {
			if (this.lifecycleState !== "open")
				throw new Error("Background jobs manager has been shut down");
			return this.startNow(snapshot);
		});
	}

	list(): BackgroundJob[] {
		return [...this.jobs.values()]
			.sort((a, b) => b.startedAt - a.startedAt)
			.map((job) => this.snapshot(job));
	}
	get(id: string): BackgroundJob | undefined {
		const job = this.jobs.get(id);
		return job && this.snapshot(job);
	}

	async read(
		id: string,
		maxBytes = this.options.maxTailBytes,
	): Promise<{ job: BackgroundJob; output: string }> {
		validateNonNegativeSafeInteger(maxBytes, "maxBytes");
		const job = this.jobs.get(id);
		if (!job) throw new Error(`Unknown background job: ${id}`);
		return {
			job: this.snapshot(job),
			output: decodeUtf8Tail(job.tail, maxBytes),
		};
	}

	async stop(id: string, reason: StopReason = "user"): Promise<BackgroundJob> {
		const job = this.jobs.get(id);
		if (!job) throw new Error(`Unknown background job: ${id}`);
		if (job.state !== "running" || job.finishing || job.killTimer)
			return this.snapshot(job);
		job.stopReason ??= reason;
		this.signal(job, "SIGTERM");
		job.killTimer = setTimeout(() => {
			job.killTimer = undefined;
			if (job.state === "running" && !job.finishing)
				this.signal(job, "SIGKILL");
		}, this.options.killGraceMs);
		job.killTimer.unref?.();
		this.emitChanged();
		return this.snapshot(job);
	}

	shutdown(): Promise<void> {
		if (this.shutdownPromise) return this.shutdownPromise;
		this.lifecycleState = "shutting-down";
		this.shutdownPromise = this.enqueueLifecycle(async () => {
			const active = [...this.jobs.values()].filter(
				(job) => job.state === "running",
			);
			await Promise.all(active.map((job) => this.stop(job.id, "shutdown")));
			if (active.length > 0) {
				const deadline = Date.now() + this.options.killGraceMs + 50;
				while (
					Date.now() < deadline &&
					active.some((job) => job.state === "running")
				) {
					await delay(Math.min(25, Math.max(1, deadline - Date.now())));
				}
				for (const job of active) {
					if (job.state === "running") this.signal(job, "SIGKILL");
				}
				await delay(25);
			}
			this.lifecycleState = "closed";
			this.emitChanged();
		});
		return this.shutdownPromise;
	}

	payload(): JobsChangedPayload {
		const all = this.list();
		const running = all.filter((job) => job.state === "running");
		const terminal = all.filter((job) => job.state !== "running");
		const primary = running[0] || terminal[0];
		return {
			runningCount: running.length,
			terminalRecentCount: terminal.length,
			oldestStart: running.length
				? Math.min(...running.map((job) => job.startedAt))
				: undefined,
			primary: primary && {
				id: primary.id,
				label: primary.label && sanitizeText(primary.label, 120),
				command: sanitizeText(primary.command, 120),
				startedAt: primary.startedAt,
			},
		};
	}

	dismiss(id: string): boolean {
		const job = this.jobs.get(id);
		if (!job || job.state === "running") return false;
		this.jobs.delete(id);
		this.emitChanged();
		return true;
	}

	private validateStart(input: StartOptions): void {
		if (!input.command.trim()) throw new Error("command is required");
		if (input.command.length > MAX_COMMAND_LENGTH)
			throw new Error(
				`command must be at most ${MAX_COMMAND_LENGTH} characters`,
			);
		if (input.cwd && input.cwd.length > MAX_CWD_LENGTH)
			throw new Error(`cwd must be at most ${MAX_CWD_LENGTH} characters`);
		if (input.label && input.label.length > MAX_LABEL_LENGTH)
			throw new Error(`label must be at most ${MAX_LABEL_LENGTH} characters`);
		if (
			input.timeoutMs !== undefined &&
			(!Number.isInteger(input.timeoutMs) ||
				input.timeoutMs < 1 ||
				input.timeoutMs > MAX_TIMEOUT_MS)
		) {
			throw new Error(
				`timeoutMs must be an integer between 1 and ${MAX_TIMEOUT_MS}`,
			);
		}
	}
	private startNow(input: StartOptions): BackgroundJob {
		const id = (++this.sequence).toString(36).padStart(2, "0");
		const startedAt = this.now();
		const cwd = input.cwd || process.cwd();
		const shell = this.options.shell || process.env.SHELL || "/bin/sh";
		const child = spawn(shell, ["-c", input.command], {
			cwd,
			detached: process.platform !== "win32",
			stdio: ["ignore", "pipe", "pipe"],
		});
		const job: InternalJob = {
			id,
			command: input.command,
			cwd,
			label: input.label?.trim() || undefined,
			state: "running",
			startedAt,
			timeoutMs: input.timeoutMs,
			bytesCaptured: 0,
			outputTruncated: false,
			child,
			processGroupId: process.platform === "win32" ? undefined : child.pid,
			tail: Buffer.alloc(0),
			utf8Pending: Buffer.alloc(0),
		};
		this.jobs.set(id, job);
		child.stdout?.on("data", (data: Buffer) => this.capture(job, data));
		child.stderr?.on("data", (data: Buffer) => this.capture(job, data));
		child.once(
			"error",
			(error) =>
				void this.finish(
					job,
					null,
					null,
					`Unable to spawn shell: ${error.message}\n`,
				).catch((error) => this.reportListenerError(error)),
		);
		child.once(
			"close",
			(code, signal) =>
				void this.finish(job, code, signal).catch((error) =>
					this.reportListenerError(error),
				),
		);
		if (input.timeoutMs) {
			job.timeoutTimer = setTimeout(() => {
				void this.stop(id, "timeout").catch((error) =>
					this.reportListenerError(error),
				);
			}, input.timeoutMs);
			job.timeoutTimer.unref?.();
		}
		this.trimRecent();
		this.emitChanged();
		return this.snapshot(job);
	}
	private enqueueLifecycle<T>(operation: () => T | Promise<T>): Promise<T> {
		const result = this.lifecycleQueue.then(operation, operation);
		this.lifecycleQueue = result.then(
			() => undefined,
			() => undefined,
		);
		return result;
	}
	private now(): number {
		return this.options.now?.() ?? Date.now();
	}
	private snapshot(job: InternalJob): BackgroundJob {
		const {
			child: _child,
			tail: _tail,
			utf8Pending: _utf8Pending,
			timeoutTimer: _timeout,
			killTimer: _kill,
			processGroupId: _processGroupId,
			finishing: _finishing,
			...copy
		} = job;
		return { ...copy };
	}
	private capture(job: InternalJob, data: Buffer): void {
		job.bytesCaptured += data.length;
		const combined = Buffer.concat([job.tail, job.utf8Pending, data]);
		const { complete, pending } = splitIncompleteUtf8(combined);
		job.utf8Pending = pending;
		if (complete.length > this.options.maxTailBytes) job.outputTruncated = true;
		job.tail = utf8Tail(complete, this.options.maxTailBytes);
	}
	private signal(job: InternalJob, signal: NodeJS.Signals): void {
		const target =
			process.platform === "win32" ? job.child?.pid : job.processGroupId;
		if (!target) return;
		try {
			process.kill(process.platform === "win32" ? target : -target, signal);
		} catch {
			try {
				job.child?.kill(signal);
			} catch {
				/* already gone */
			}
		}
	}
	private processGroupAlive(job: InternalJob): boolean {
		if (process.platform === "win32" || !job.processGroupId) return false;
		try {
			process.kill(-job.processGroupId, 0);
			return true;
		} catch {
			return false;
		}
	}
	private async cleanupProcessGroup(job: InternalJob): Promise<void> {
		// Let the just-reaped shell's process group disappear before deciding that
		// descendants remain. This keeps ordinary short jobs from paying the grace period.
		await delay(10);
		if (!this.processGroupAlive(job)) return;
		this.signal(job, "SIGTERM");
		// The leader has already exited, so any remaining group members are
		// descendants. Give them a short courtesy window, not the full user-stop grace.
		const deadline = Date.now() + Math.min(this.options.killGraceMs, 50);
		while (Date.now() < deadline) {
			await delay(Math.min(25, Math.max(1, deadline - Date.now())));
			if (!this.processGroupAlive(job)) return;
		}
		this.signal(job, "SIGKILL");
		await delay(25);
	}
	private async finish(
		job: InternalJob,
		exitCode: number | null,
		signal: NodeJS.Signals | null,
		extra = "",
	): Promise<void> {
		if (job.state !== "running" || job.finishing) return;
		job.finishing = true;
		if (extra) this.capture(job, Buffer.from(extra));
		if (job.timeoutTimer) clearTimeout(job.timeoutTimer);
		job.timeoutTimer = undefined;
		if (job.killTimer) clearTimeout(job.killTimer);
		job.killTimer = undefined;
		await this.cleanupProcessGroup(job);
		// stop()/shutdown() may race the asynchronous group cleanup. Clear any
		// timer they managed to install before this job becomes terminal.
		if (job.killTimer) clearTimeout(job.killTimer);
		job.killTimer = undefined;
		job.exitCode = exitCode;
		job.signal = signal;
		job.endedAt = this.now();
		job.child = undefined;
		job.state =
			job.stopReason === "timeout"
				? "timed_out"
				: job.stopReason
					? "stopped"
					: exitCode === 0
						? "completed"
						: "failed";
		job.finishing = false;
		this.trimRecent();
		const snap = this.snapshot(job);
		const tail = job.tail.toString("utf8");
		this.notify(this.completionListeners, snap, tail);
		this.emitChanged();
	}
	private trimRecent(): void {
		const terminal = [...this.jobs.values()]
			.filter((job) => job.state !== "running")
			.sort((a, b) => (b.endedAt || 0) - (a.endedAt || 0));
		for (const job of terminal.slice(this.options.maxRecent))
			this.jobs.delete(job.id);
	}
	private emitChanged(): void {
		this.notify(this.changeListeners, this.payload());
	}
	private notify<T extends unknown[]>(
		listeners: Set<Listener<T>>,
		...args: T
	): void {
		for (const listener of listeners) {
			try {
				const result = listener(...args);
				if (result)
					void result.catch((error) => this.reportListenerError(error));
			} catch (error) {
				this.reportListenerError(error);
			}
		}
	}
	private reportListenerError(error: unknown): void {
		try {
			this.options.onListenerError?.(error);
		} catch {
			// Error reporters must not become another asynchronous failure path.
		}
	}
}

function validateNonNegativeSafeInteger(value: number, name: string): number {
	if (!Number.isSafeInteger(value) || value < 0)
		throw new Error(`${name} must be a non-negative safe integer`);
	return value;
}

/** Keeps the newest complete UTF-8 code points within a byte limit. */
function utf8Tail(value: Buffer, maxBytes: number): Buffer {
	let start = Math.max(0, value.length - maxBytes);
	while (start < value.length && (value[start]! & 0xc0) === 0x80) start += 1;
	let end = value.length;
	while (end > start) {
		let codePointStart = end - 1;
		while (codePointStart > start && (value[codePointStart]! & 0xc0) === 0x80)
			codePointStart -= 1;
		const leading = value[codePointStart]!;
		const expectedLength =
			leading >= 0xc2 && leading <= 0xdf
				? 2
				: leading >= 0xe0 && leading <= 0xef
					? 3
					: leading >= 0xf0 && leading <= 0xf4
						? 4
						: 1;
		if (end - codePointStart >= expectedLength) break;
		end = codePointStart;
	}
	return value.subarray(start, end);
}

function decodeUtf8Tail(value: Buffer, maxBytes: number): string {
	return utf8Tail(value, maxBytes).toString("utf8");
}

function splitIncompleteUtf8(value: Buffer): {
	complete: Buffer;
	pending: Buffer;
} {
	if (!value.length) return { complete: value, pending: value };
	let start = value.length - 1;
	while (start > 0 && (value[start]! & 0xc0) === 0x80) start -= 1;
	const leading = value[start]!;
	const expectedLength =
		leading >= 0xc2 && leading <= 0xdf
			? 2
			: leading >= 0xe0 && leading <= 0xef
				? 3
				: leading >= 0xf0 && leading <= 0xf4
					? 4
					: 1;
	if (value.length - start < expectedLength)
		return {
			complete: value.subarray(0, start),
			pending: value.subarray(start),
		};
	return { complete: value, pending: Buffer.alloc(0) };
}

function normalizedLimit(limit: number): number {
	if (limit === Number.POSITIVE_INFINITY) return Number.MAX_SAFE_INTEGER;
	return Number.isFinite(limit) ? Math.max(0, Math.floor(limit)) : 0;
}

function truncateCodePoints(
	value: string,
	limit: number,
	fromEnd: boolean,
): string {
	const points = Array.from(value);
	if (points.length <= limit) return value;
	if (limit === 0) return "";
	if (limit === 1) return "…";
	const retained = fromEnd
		? points.slice(-(limit - 1))
		: points.slice(0, limit - 1);
	return fromEnd ? `…${retained.join("")}` : `${retained.join("")}…`;
}

export function sanitizeOutput(value: string, limit = 16_384): string {
	const cleaned = value
		.replace(/\u001b\][^\u0007\u001b]*(?:\u0007|\u001b\\)/g, "")
		.replace(/\u001b\[[0-?]*[ -/]*[@-~]/g, "")
		.replace(/\u009b[0-?]*[ -/]*[@-~]/g, "")
		.replace(/\r(?!\n)/g, "")
		.replace(/[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]/g, "�");
	return truncateCodePoints(cleaned, normalizedLimit(limit), true);
}

export function sanitizeText(value: string, limit = 240): string {
	const safeLimit = normalizedLimit(limit);
	const cleaned = sanitizeOutput(value, Math.max(safeLimit * 4, 1024))
		.replace(/[\n\t]+/g, " ")
		.replace(/\s+/g, " ")
		.trim();
	return truncateCodePoints(cleaned, safeLimit, false);
}
