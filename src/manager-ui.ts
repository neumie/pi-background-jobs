import type { Theme } from "@earendil-works/pi-coding-agent";
import { matchesKey, truncateToWidth, visibleWidth } from "@earendil-works/pi-tui";
import type { BackgroundJob, JobManager } from "./job-manager.js";
import { sanitizeOutput, sanitizeText } from "./job-manager.js";

const LIST_ROWS = 10;
const OUTPUT_ROWS = 12;

const status = (job: BackgroundJob): string =>
	({
		running: "RUN",
		completed: "OK",
		failed: "FAIL",
		stopped: "STOP",
		timed_out: "TIME",
	})[job.state];

const elapsed = (job: BackgroundJob, now = Date.now()): string => {
	const ms = Math.max(0, (job.endedAt || now) - job.startedAt);
	return ms < 1000
		? `${ms}ms`
		: ms < 60_000
			? `${(ms / 1000).toFixed(1)}s`
			: `${Math.floor(ms / 60_000)}m${Math.floor((ms % 60_000) / 1000)}s`;
};

function frame(width: number, title: string, body: string[], footer?: string): string[] {
	const safeWidth = Math.max(1, width);
	if (safeWidth < 4) return [truncateToWidth(title, safeWidth, "")];
	const innerWidth = safeWidth - 2;
	const fit = (value: string): string => {
		const clipped = truncateToWidth(value, innerWidth, "…");
		return `${clipped}${" ".repeat(Math.max(0, innerWidth - visibleWidth(clipped)))}`;
	};
	const safeTitle = truncateToWidth(` ${title} `, innerWidth, "…");
	const top = `╭${safeTitle}${"─".repeat(Math.max(0, innerWidth - visibleWidth(safeTitle)))}╮`;
	const lines = [top, ...body.map((value) => `│${fit(value)}│`)];
	if (footer) {
		const safeFooter = truncateToWidth(` ${footer} `, innerWidth, "…");
		lines.push(`├${safeFooter}${"─".repeat(Math.max(0, innerWidth - visibleWidth(safeFooter)))}┤`);
	}
	lines.push(`╰${"─".repeat(innerWidth)}╯`);
	return lines;
}

function listWindow(jobs: BackgroundJob[], selected: number): { start: number; jobs: BackgroundJob[] } {
	const maxStart = Math.max(0, jobs.length - LIST_ROWS);
	const start = Math.min(maxStart, Math.max(0, selected - Math.floor(LIST_ROWS / 2)));
	return { start, jobs: jobs.slice(start, start + LIST_ROWS) };
}

/** Pure, escape-safe manager lines. Kept separate so render bounds are testable. */
export function renderJobsLines(
	jobs: BackgroundJob[],
	selected: number,
	detail: boolean,
	output: string,
	width: number,
	theme?: Theme,
	outputScrollFromEnd = 0,
	following = false,
): string[] {
	const color = (
		name: "accent" | "dim" | "success" | "error" | "warning",
		text: string,
	) => (theme ? theme.fg(name, text) : text);
	const body: string[] = [];
	const window = listWindow(jobs, selected);
	if (window.start > 0) body.push(color("dim", `  … ${window.start} newer jobs`));
	if (!jobs.length) body.push(` ${color("dim", "No jobs in this Pi session.")}`);
	window.jobs.forEach((job, visibleIndex) => {
		const index = window.start + visibleIndex;
		const marker = index === selected ? color("accent", "›") : " ";
		const stateColor =
			job.state === "completed"
				? "success"
				: job.state === "running"
					? "warning"
					: job.state === "failed" || job.state === "timed_out"
						? "error"
						: "dim";
		const label = sanitizeText(job.label || job.command, Math.max(12, width - 25));
		body.push(` ${marker} ${color(stateColor, status(job).padEnd(4))} ${elapsed(job).padStart(6)} ${job.id} ${label}`);
	});
	const hiddenOlder = jobs.length - window.start - window.jobs.length;
	if (hiddenOlder > 0) body.push(color("dim", `  … ${hiddenOlder} older jobs`));

	const active = jobs[selected];
	if (detail && active) {
		body.push(color("dim", "─".repeat(Math.max(1, width - 4))));
		body.push(`${color("accent", "Command:")} ${sanitizeText(active.command, Math.max(1, width - 13))}`);
		body.push(color("dim", `cwd: ${sanitizeText(active.cwd, Math.max(1, width - 8))}`));
		body.push(
			color(
				"dim",
				`exit: ${active.exitCode ?? "—"} · signal: ${active.signal ?? "—"} · captured: ${active.bytesCaptured}B${active.outputTruncated ? " (bounded)" : ""}`,
			),
		);
		body.push(`${color("accent", "Output")} ${following ? color("warning", "· FOLLOW") : ""}`.trimEnd());
		const outputLines = sanitizeOutput(output || "(no output captured yet)")
			.split("\n");
		const end = Math.max(0, outputLines.length - Math.max(0, outputScrollFromEnd));
		const start = Math.max(0, end - OUTPUT_ROWS);
		if (start > 0) body.push(color("dim", `… ${start} earlier lines`));
		for (const value of outputLines.slice(start, end)) body.push(` ${sanitizeText(value, Math.max(1, width - 5))}`);
		const later = outputLines.length - end;
		if (later > 0) body.push(color("dim", `… ${later} later lines`));
	}

	const footer = detail
		? "PgUp/PgDn scroll · ←/→ switch · End follow · s stop · B list · ? help"
		: "↑/↓ select · Enter detail · s stop · d dismiss · ? help · Esc close";
	return frame(width, `Jobs · ${jobs.filter((job) => job.state === "running").length} running`, body, footer);
}

function renderHelpLines(width: number, theme: Theme): string[] {
	return frame(
		width,
		"Jobs help",
		[
			" Enter: inspect selected job and its output",
			" ↑/↓: select in list · ←/→: switch in detail",
			" PgUp/PgDn: scroll output · End or f: follow newest",
			" s: stop with TERM, then KILL after the grace period",
			" d: dismiss a terminal job and remove its retained log",
			" B/Backspace: return to list · Esc: close",
		].map((line) => theme.fg("dim", line)),
		"? close help",
	);
}

export class JobsComponent {
	private selectedId?: string;
	private selectedIndexHint = 0;
	private detail = false;
	private help = false;
	private output = "";
	private outputScrollFromEnd = 0;
	private followTimer?: ReturnType<typeof setInterval>;
	private unsubscribe?: () => void;
	private width = 0;
	private lines: string[] = [];

	constructor(
		private readonly manager: JobManager,
		private readonly theme: Theme,
		private readonly requestRender: () => void,
		private readonly done: () => void,
	) {
		this.unsubscribe = manager.onChanged(() => {
			if (this.detail) void this.loadOutput();
			else {
				this.invalidate();
				this.requestRender();
			}
		});
	}

	handleInput(data: string): void {
		const jobs = this.manager.list();
		const selected = this.resolveSelection(jobs);
		const lower = data.toLowerCase();
		if (matchesKey(data, "escape") || matchesKey(data, "ctrl+c")) return this.done();
		if (this.help && (lower === "h" || lower === "?")) this.help = false;
		else if (lower === "h" || lower === "?") this.help = true;
		else if (this.detail) this.handleDetailInput(data, lower, jobs);
		else if (matchesKey(data, "up") || lower === "k") this.selectIndex(jobs, Math.max(0, selected - 1));
		else if (matchesKey(data, "down") || lower === "j") this.selectIndex(jobs, Math.min(Math.max(0, jobs.length - 1), selected + 1));
		else if (matchesKey(data, "return")) {
			this.detail = true;
			this.outputScrollFromEnd = 0;
			void this.loadOutput();
		} else if (lower === "s") {
			const job = jobs[selected];
			if (job) void this.manager.stop(job.id);
		} else if (lower === "d") {
			const job = jobs[selected];
			if (job && this.manager.dismiss(job.id)) {
				this.selectedId = undefined;
				this.selectedIndexHint = Math.min(selected, Math.max(0, this.manager.list().length - 1));
			}
		}
		this.invalidate();
		this.requestRender();
	}

	private handleDetailInput(data: string, lower: string, jobs: BackgroundJob[]): void {
		if (lower === "b" || matchesKey(data, "backspace") || matchesKey(data, "return")) {
			this.detail = false;
			this.stopFollowing();
			return;
		}
		if (matchesKey(data, "left") || matchesKey(data, "up")) this.changeSelection(-1, jobs);
		else if (matchesKey(data, "right") || matchesKey(data, "down") || matchesKey(data, "tab")) this.changeSelection(1, jobs);
		else if (matchesKey(data, "pageUp")) this.outputScrollFromEnd += OUTPUT_ROWS;
		else if (matchesKey(data, "pageDown")) this.outputScrollFromEnd = Math.max(0, this.outputScrollFromEnd - OUTPUT_ROWS);
		else if (matchesKey(data, "end") || lower === "f") {
			this.outputScrollFromEnd = 0;
			this.startFollowing();
		} else if (lower === "s") {
			const job = jobs[this.resolveSelection(jobs)];
			if (job) void this.manager.stop(job.id);
		}
		void this.loadOutput();
	}

	private resolveSelection(jobs: BackgroundJob[]): number {
		if (!jobs.length) {
			this.selectedId = undefined;
			this.selectedIndexHint = 0;
			return 0;
		}
		const byId = this.selectedId ? jobs.findIndex((job) => job.id === this.selectedId) : -1;
		const index = byId >= 0 ? byId : Math.min(this.selectedIndexHint, jobs.length - 1);
		this.selectedId = jobs[index]?.id;
		this.selectedIndexHint = index;
		return index;
	}

	private selectIndex(jobs: BackgroundJob[], index: number): void {
		if (!jobs.length) return;
		const bounded = Math.max(0, Math.min(index, jobs.length - 1));
		this.selectedId = jobs[bounded]?.id;
		this.selectedIndexHint = bounded;
	}

	private changeSelection(delta: number, jobs: BackgroundJob[]): void {
		if (!jobs.length) return;
		const selected = this.resolveSelection(jobs);
		this.selectIndex(jobs, (selected + delta + jobs.length) % jobs.length);
		this.output = "";
		this.outputScrollFromEnd = 0;
	}

	render(width: number): string[] {
		if (this.width !== width || !this.lines.length) {
			this.width = width;
			const jobs = this.manager.list();
			const selected = this.resolveSelection(jobs);
			this.lines = this.help
				? renderHelpLines(width, this.theme)
				: renderJobsLines(
						jobs,
						selected,
						this.detail,
						this.output,
						width,
						this.theme,
						this.outputScrollFromEnd,
						Boolean(this.followTimer),
					);
		}
		return this.lines;
	}

	invalidate(): void {
		this.width = 0;
		this.lines = [];
	}

	dispose(): void {
		this.stopFollowing();
		this.unsubscribe?.();
		this.unsubscribe = undefined;
	}

	private startFollowing(): void {
		if (this.followTimer) return;
		this.followTimer = setInterval(() => void this.loadOutput(), 500);
	}

	private stopFollowing(): void {
		if (this.followTimer) clearInterval(this.followTimer);
		this.followTimer = undefined;
	}

	private async loadOutput(): Promise<void> {
		const jobs = this.manager.list();
		const job = jobs[this.resolveSelection(jobs)];
		if (!job) return;
		const id = job.id;
		try {
			const read = await this.manager.read(id);
			if (this.selectedId !== id) return;
			this.output = read.output;
			if (read.job.state !== "running") this.stopFollowing();
		} catch {
			this.output = "(log is no longer available)";
		}
		this.invalidate();
		this.requestRender();
	}
}
