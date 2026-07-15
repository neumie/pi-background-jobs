import { spawn, type ChildProcess } from "node:child_process";
import { mkdtemp, rm, writeFile, appendFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
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
	logPath: string;
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
	maxLogBytes?: number;
	maxTailBytes?: number;
	maxRecent?: number;
	killGraceMs?: number;
	shell?: string;
	now?: () => number;
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
	timeoutTimer?: ReturnType<typeof setTimeout>;
	killTimer?: ReturnType<typeof setTimeout>;
	processGroupId?: number;
	finishing?: boolean;
};

const defaults = {
	maxLogBytes: 256 * 1024,
	maxTailBytes: 16 * 1024,
	maxRecent: 40,
	killGraceMs: 750,
};

/** A process manager deliberately limited to non-interactive `shell -c` jobs. */
export class JobManager {
	private readonly options: Required<Omit<JobManagerOptions, "shell" | "now">> &
		Pick<JobManagerOptions, "shell" | "now">;
	private jobs = new Map<string, InternalJob>();
	private sequence = 0;
	private logDir?: string;
	private changeListeners = new Set<(payload: JobsChangedPayload) => void>();
	private completionListeners = new Set<
		(job: BackgroundJob, tail: string) => void
	>();
	private cleaned = false;

	constructor(options: JobManagerOptions = {}) {
		this.options = { ...defaults, ...options };
	}

	onChanged(listener: (payload: JobsChangedPayload) => void): () => void {
		this.changeListeners.add(listener);
		return () => this.changeListeners.delete(listener);
	}

	onCompleted(
		listener: (job: BackgroundJob, tail: string) => void,
	): () => void {
		this.completionListeners.add(listener);
		return () => this.completionListeners.delete(listener);
	}

	async start(input: StartOptions): Promise<BackgroundJob> {
		if (this.cleaned)
			throw new Error("Background jobs manager has been shut down");
		if (!input.command.trim()) throw new Error("command is required");
		if (input.command.length > MAX_COMMAND_LENGTH)
			throw new Error(`command must be at most ${MAX_COMMAND_LENGTH} characters`);
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
			throw new Error(`timeoutMs must be an integer between 1 and ${MAX_TIMEOUT_MS}`);
		}
		if (!this.logDir)
			this.logDir = await mkdtemp(join(tmpdir(), "pi-background-jobs-"));
		const id = (++this.sequence).toString(36).padStart(2, "0");
		const startedAt = this.now();
		const cwd = input.cwd || process.cwd();
		const logPath = join(this.logDir, `${id}.log`);
		await writeFile(logPath, "");
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
			logPath,
			bytesCaptured: 0,
			outputTruncated: false,
			child,
			processGroupId: process.platform === "win32" ? undefined : child.pid,
			tail: Buffer.alloc(0),
		};
		this.jobs.set(id, job);
		child.stdout?.on("data", (data: Buffer) => this.capture(job, data));
		child.stderr?.on("data", (data: Buffer) => this.capture(job, data));
		child.once("error", (error) =>
			void this.finish(job, null, null, `Unable to spawn shell: ${error.message}\n`),
		);
		child.once("close", (code, signal) => void this.finish(job, code, signal));
		if (input.timeoutMs) {
			job.timeoutTimer = setTimeout(
				() => void this.stop(id, "timeout"),
				input.timeoutMs,
			);
			job.timeoutTimer.unref?.();
		}
		this.trimRecent();
		this.emitChanged();
		return this.snapshot(job);
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
		const job = this.jobs.get(id);
		if (!job) throw new Error(`Unknown background job: ${id}`);
		// Disk logs are recoverable across renderer/UI refreshes; the in-memory ring includes newest live output.
		// The ring is updated synchronously with stream data, unlike the asynchronous disk append,
		// so it is the authoritative live tail. The on-disk log remains recoverable for the retained job.
		return {
			job: this.snapshot(job),
			output: job.tail
				.subarray(Math.max(0, job.tail.length - maxBytes))
				.toString("utf8"),
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
			if (job.state === "running" && !job.finishing) this.signal(job, "SIGKILL");
		}, this.options.killGraceMs);
		job.killTimer.unref?.();
		this.emitChanged();
		return this.snapshot(job);
	}

	async shutdown(): Promise<void> {
		if (this.cleaned) return;
		this.cleaned = true;
		const active = [...this.jobs.values()].filter((job) => job.state === "running");
		await Promise.all(active.map((job) => this.stop(job.id, "shutdown")));
		if (active.length > 0) {
			const deadline = Date.now() + this.options.killGraceMs + 50;
			while (Date.now() < deadline && active.some((job) => job.state === "running")) {
				await delay(Math.min(25, Math.max(1, deadline - Date.now())));
			}
			for (const job of active) {
				if (job.state === "running") this.signal(job, "SIGKILL");
			}
			await delay(25);
		}
		if (this.logDir) await rm(this.logDir, { recursive: true, force: true });
		this.logDir = undefined;
		this.emitChanged();
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
		void rm(job.logPath, { force: true });
		this.emitChanged();
		return true;
	}

	private now(): number {
		return this.options.now?.() ?? Date.now();
	}
	private snapshot(job: InternalJob): BackgroundJob {
		const {
			child: _child,
			tail: _tail,
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
		job.tail = Buffer.concat([job.tail, data]).subarray(
			Math.max(0, job.tail.length + data.length - this.options.maxTailBytes),
		);
		if (job.bytesCaptured <= this.options.maxLogBytes)
			void appendFile(job.logPath, data).catch(() => {});
		else job.outputTruncated = true;
	}
	private signal(job: InternalJob, signal: NodeJS.Signals): void {
		const target = process.platform === "win32" ? job.child?.pid : job.processGroupId;
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
		for (const listener of this.completionListeners) listener(snap, tail);
		this.emitChanged();
	}
	private trimRecent(): void {
		const terminal = [...this.jobs.values()]
			.filter((job) => job.state !== "running")
			.sort((a, b) => (b.endedAt || 0) - (a.endedAt || 0));
		for (const job of terminal.slice(this.options.maxRecent)) {
			this.jobs.delete(job.id);
			void rm(job.logPath, { force: true });
		}
	}
	private emitChanged(): void {
		const payload = this.payload();
		for (const listener of this.changeListeners) listener(payload);
	}
}

export function sanitizeOutput(value: string, limit = 16_384): string {
	const cleaned = value
		.replace(/\u001b\][^\u0007\u001b]*(?:\u0007|\u001b\\)/g, "")
		.replace(/\u001b\[[0-?]*[ -/]*[@-~]/g, "")
		.replace(/\u009b[0-?]*[ -/]*[@-~]/g, "")
		.replace(/\r(?!\n)/g, "")
		.replace(/[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]/g, "�");
	return cleaned.length > limit ? `…${cleaned.slice(-Math.max(0, limit - 1))}` : cleaned;
}

export function sanitizeText(value: string, limit = 240): string {
	const cleaned = sanitizeOutput(value, Math.max(limit * 4, 1024))
		.replace(/[\n\t]+/g, " ")
		.replace(/\s+/g, " ")
		.trim();
	return cleaned.length > limit
		? `${cleaned.slice(0, Math.max(0, limit - 1))}…`
		: cleaned;
}
