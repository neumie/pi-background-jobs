import { StringEnum } from "@earendil-works/pi-ai";
import type {
	ExtensionAPI,
	ExtensionContext,
	Theme,
} from "@earendil-works/pi-coding-agent";
import { keyHint } from "@earendil-works/pi-coding-agent";
import { Text } from "@earendil-works/pi-tui";
import { Type } from "typebox";
import {
	JobManager,
	MAX_COMMAND_LENGTH,
	MAX_CWD_LENGTH,
	MAX_LABEL_LENGTH,
	MAX_TIMEOUT_MS,
	sanitizeOutput,
	sanitizeText,
	type BackgroundJob,
	type JobsChangedPayload,
} from "./job-manager.js";
import { JobsComponent } from "./manager-ui.js";

const COMPLETION_TYPE = "background-jobs:completion";

interface Completion {
	job: BackgroundJob;
	output: string;
}
interface ToolDetails {
	action: "start" | "list" | "read" | "stop";
	job?: BackgroundJob;
	jobs?: BackgroundJob[];
	output?: string;
}
interface Runtime {
	manager: JobManager;
	pi?: ExtensionAPI;
	ctx?: ExtensionContext;
	changedUnsub?: () => void;
	completedUnsub?: () => void;
	pending: Completion[];
	completionTimer?: ReturnType<typeof setTimeout>;
	reloading?: boolean;
}

declare global {
	var __piBackgroundJobsRuntime: Runtime | undefined;
}

function runtime(): Runtime {
	if (!globalThis.__piBackgroundJobsRuntime)
		globalThis.__piBackgroundJobsRuntime = {
			manager: new JobManager(),
			pending: [],
		};
	return globalThis.__piBackgroundJobsRuntime;
}

function completionLine(job: BackgroundJob): string {
	if (job.state === "completed")
		return `Background job ${job.id} completed (exit ${job.exitCode ?? 0}).`;
	if (job.state === "timed_out")
		return `Background job ${job.id} timed out and was stopped.`;
	if (job.state === "stopped") return `Background job ${job.id} stopped.`;
	return `Background job ${job.id} failed (exit ${job.exitCode ?? "unknown"}${job.signal ? `, ${job.signal}` : ""}).`;
}

function statusText(payload: JobsChangedPayload): string | undefined {
	if (!payload.runningCount) return undefined;
	const primary = payload.primary;
	return `${payload.runningCount} background job${payload.runningCount === 1 ? "" : "s"}: ${sanitizeText(primary?.label || primary?.command || "running", 36)}`;
}

function scheduleCompletions(current: Runtime): void {
	if (current.completionTimer || current.pending.length === 0) return;
	current.completionTimer = setTimeout(() => {
		current.completionTimer = undefined;
		if (!current.pi || current.reloading) {
			scheduleCompletions(current);
			return;
		}
		const completions = current.pending.splice(0);
		const content = completions
			.map(
				({ job, output }) =>
					`${completionLine(job)}\nTail:\n${output.slice(-4000) || "(no output)"}`,
			)
			.join("\n\n");
		current.pi.sendMessage(
			{
				customType: COMPLETION_TYPE,
				content,
				display: true,
				details: completions,
			},
			{ triggerTurn: true, deliverAs: "followUp" },
		);
	}, 100);
	current.completionTimer.unref?.();
}

function bind(
	current: Runtime,
	pi: ExtensionAPI,
	ctx?: ExtensionContext,
): void {
	current.pi = pi;
	current.reloading = false;
	if (ctx) current.ctx = ctx;
	current.changedUnsub?.();
	current.completedUnsub?.();
	current.changedUnsub = current.manager.onChanged((payload) => {
		current.pi?.events.emit("background-jobs:changed", payload);
		current.ctx?.ui.setStatus("background-jobs", statusText(payload));
	});
	current.completedUnsub = current.manager.onCompleted((job, output) => {
		current.pending.push({ job, output: sanitizeOutput(output) });
		scheduleCompletions(current);
	});
	scheduleCompletions(current);
}

const Params = Type.Object({
	action: StringEnum(["start", "list", "read", "stop"] as const),
	command: Type.Optional(
		Type.String({
			description: "Non-interactive shell command (required for start).",
			maxLength: MAX_COMMAND_LENGTH,
		}),
	),
	cwd: Type.Optional(
		Type.String({ description: "Working directory for start.", maxLength: MAX_CWD_LENGTH }),
	),
	label: Type.Optional(
		Type.String({ description: "Short human-readable job label.", maxLength: MAX_LABEL_LENGTH }),
	),
	timeoutMs: Type.Optional(
		Type.Integer({
			description: "Timeout in milliseconds.",
			minimum: 1,
			maximum: MAX_TIMEOUT_MS,
		}),
	),
	id: Type.Optional(
		Type.String({ description: "Job id (required for read and stop).", maxLength: 64 }),
	),
});

function toolText(job: BackgroundJob): string {
	return `${job.id} ${job.state} ${sanitizeText(job.label || job.command, 100)}`;
}

export default function backgroundJobs(pi: ExtensionAPI): void {
	const current = runtime();
	bind(current, pi);

	pi.registerTool<typeof Params, ToolDetails>({
		name: "background_job",
		label: "Background job",
		description:
			"Run and manage non-interactive background shell jobs. start returns immediately; use list/read/stop or /jobs to manage. Never use this for interactive programs or stdin input.",
		promptSnippet:
			"Run non-interactive shell work in the background and inspect it later.",
		promptGuidelines: [
			"Use background_job start for long non-interactive commands.",
			"Use read only when output is needed; completed jobs are delivered automatically.",
		],
		parameters: Params,
		executionMode: "parallel",
		async execute(_toolCallId, params) {
			try {
				if (params.action === "start") {
					if (!params.command) throw new Error("command is required for start");
					const job = await current.manager.start({
						command: params.command,
						cwd: params.cwd,
						label: params.label,
						timeoutMs: params.timeoutMs,
					});
					return {
						content: [
							{
								type: "text" as const,
								text: `Started background job ${job.id}.`,
							},
						],
						details: { action: "start", job },
					};
				}
				if (params.action === "list") {
					const jobs = current.manager.list();
					return {
						content: [
							{
								type: "text" as const,
								text: jobs.length
									? jobs.map(toolText).join("\n")
									: "No background jobs.",
							},
						],
						details: { action: "list", jobs },
					};
				}
				if (!params.id) throw new Error(`id is required for ${params.action}`);
				if (params.action === "stop") {
					const job = await current.manager.stop(params.id);
					return {
						content: [
							{
								type: "text" as const,
								text: `Stopping background job ${job.id}.`,
							},
						],
						details: { action: "stop", job },
					};
				}
				const read = await current.manager.read(params.id);
				const output = sanitizeOutput(read.output);
				return {
					content: [
						{
							type: "text" as const,
							text: output || "(no output captured)",
						},
					],
					details: { action: "read", job: read.job, output },
				};
			} catch (error) {
				throw error instanceof Error ? error : new Error(String(error));
			}
		},
		renderCall(args, theme) {
			const action = sanitizeText(args.action, 16);
			const subject =
				args.action === "start"
					? sanitizeText(args.command || "", 180)
					: sanitizeText(args.id || "", 64);
			return new Text(
				`${theme.fg("toolTitle", theme.bold("background_job "))}${theme.fg("accent", action)}${subject ? ` ${theme.fg("dim", subject)}` : ""}`,
				0,
				0,
			);
		},
		renderResult(result, { expanded }, theme) {
			const details = result.details as
				| {
						action?: string;
						job?: BackgroundJob;
						jobs?: BackgroundJob[];
						output?: string;
				  }
				| undefined;
			if (details?.action === "start") {
				let text = theme.fg(
					"warning",
					"Running in background (/jobs to manage)",
				);
				if (expanded && details.job)
					text += `\n${theme.fg(
						"dim",
						`${sanitizeText(details.job.id, 64)}: ${sanitizeText(details.job.command, 2_000)}`,
					)}`;
				return new Text(text, 0, 0);
			}
			if (details?.action === "read") {
				const output = details.output || "(no output captured)";
				return new Text(
					expanded
						? theme.fg("dim", sanitizeOutput(output.slice(-8000), 8_000))
						: `${theme.fg("dim", "Output available")} (${keyHint("app.tools.expand", "expand")})`,
					0,
					0,
				);
			}
			const text =
				result.content[0]?.type === "text" ? result.content[0].text : "Done";
			return new Text(
				expanded
					? theme.fg("dim", text)
					: theme.fg("dim", sanitizeText(text, 180)),
				0,
				0,
			);
		},
	});

	pi.registerMessageRenderer<Completion[]>(
		COMPLETION_TYPE,
		(message, { expanded }, theme: Theme) => {
			const completions = (message.details || []) as Completion[];
			const first = completions[0];
			if (!first)
				return new Text(theme.fg("dim", sanitizeOutput(String(message.content), 4_000)), 0, 0);
			const heading =
				completions.length === 1
					? completionLine(first.job)
					: `${completions.length} background jobs finished: ${completions.map(({ job }) => job.id).join(", ")}.`;
			const detail = completions
				.map(
					({ job, output }) =>
						`${completionLine(job)}\n${sanitizeOutput(output.slice(-4000)) || "(no output)"}`,
				)
				.join("\n\n");
			return new Text(
				expanded
					? `${theme.fg("accent", heading)}\n${theme.fg("dim", detail)}`
					: theme.fg(
							first.job.state === "completed" ? "success" : "warning",
							heading,
						),
				0,
				0,
			);
		},
	);

	pi.registerCommand("jobs", {
		description: "Manage non-interactive background jobs for this Pi session",
		handler: async (_args, ctx) => {
			bind(current, pi, ctx);
			await ctx.ui.custom<void>((tui, theme, _keys, done) => {
				const component = new JobsComponent(
					current.manager,
					theme,
					() => tui.requestRender(),
					done,
				);
				return component;
			});
		},
	});

	pi.on("session_start", (_event, ctx) => {
		bind(current, pi, ctx);
		const payload = current.manager.payload();
		ctx.ui.setStatus("background-jobs", statusText(payload));
		pi.events.emit("background-jobs:changed", payload);
	});
	pi.on("session_shutdown", async (event) => {
		current.changedUnsub?.();
		current.changedUnsub = undefined;
		current.ctx = undefined;
		if (event.reason === "reload") {
			current.reloading = true;
			current.pi = undefined;
			return;
		}
		current.completedUnsub?.();
		current.completedUnsub = undefined;
		if (current.completionTimer) clearTimeout(current.completionTimer);
		current.completionTimer = undefined;
		current.pending = [];
		await current.manager.shutdown();
		if (globalThis.__piBackgroundJobsRuntime === current)
			globalThis.__piBackgroundJobsRuntime = undefined;
	});
}

/** Stable event bus payload emitted on `background-jobs:changed`: runningCount, terminalRecentCount, oldestStart, and primary job identity. */
export type BackgroundJobsChangedEvent = JobsChangedPayload;
