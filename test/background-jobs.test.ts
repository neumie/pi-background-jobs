import assert from "node:assert/strict";
import test from "node:test";
import { setTimeout as delay } from "node:timers/promises";
import {
	JobManager,
	MAX_TIMEOUT_MS,
	sanitizeOutput,
	sanitizeText,
} from "../src/job-manager.js";
import { JobsComponent, renderJobsLines } from "../src/manager-ui.js";
import { visibleWidth } from "@earendil-works/pi-tui";

async function waitFor(manager: JobManager, id: string, ms = 3000) {
	const end = Date.now() + ms;
	while (Date.now() < end) {
		const job = manager.get(id);
		if (job && job.state !== "running") return job;
		await delay(15);
	}
	throw new Error(`job ${id} did not finish`);
}

test("starts immediately, captures combined output, and records exact success state", async () => {
	const manager = new JobManager();
	const job = await manager.start({
		command: "printf out; printf err >&2",
		label: "test",
	});
	assert.equal(job.state, "running");
	const done = await waitFor(manager, job.id);
	assert.equal(done.state, "completed");
	assert.equal(done.exitCode, 0);
	assert.ok(done.endedAt && done.endedAt >= done.startedAt);
	const read = await manager.read(job.id);
	assert.match(read.output, /out/);
	assert.match(read.output, /err/);
	await manager.shutdown();
});

test("rejects timer overflow and non-integer timeout values", async () => {
	const manager = new JobManager();
	await assert.rejects(
		manager.start({ command: "true", timeoutMs: MAX_TIMEOUT_MS + 1 }),
		/integer between/,
	);
	await assert.rejects(
		manager.start({ command: "true", timeoutMs: 1.5 }),
		/integer between/,
	);
	const job = await manager.start({ command: "sleep 5", timeoutMs: MAX_TIMEOUT_MS });
	await manager.stop(job.id);
	await waitFor(manager, job.id);
	await manager.shutdown();
});

test("records a nonzero exit as failure", async () => {
	const manager = new JobManager();
	const job = await manager.start({ command: "printf nope >&2; exit 7" });
	const done = await waitFor(manager, job.id);
	assert.equal(done.state, "failed");
	assert.equal(done.exitCode, 7);
	await manager.shutdown();
});

test("timeout sends process-group stop and terminal state is timed_out", async () => {
	const manager = new JobManager({ killGraceMs: 40 });
	const job = await manager.start({ command: "sleep 5", timeoutMs: 30 });
	const done = await waitFor(manager, job.id);
	assert.equal(done.state, "timed_out");
	assert.equal(done.stopReason, "timeout");
	await manager.shutdown();
});

test("stubborn process receives KILL fallback after TERM", async () => {
	const manager = new JobManager({ killGraceMs: 50 });
	const job = await manager.start({
		command: "trap '' TERM; while :; do sleep 0.02; done",
	});
	await delay(40);
	await manager.stop(job.id);
	const done = await waitFor(manager, job.id);
	assert.equal(done.state, "stopped");
	assert.ok(done.signal === "SIGKILL" || done.signal === "SIGTERM");
	await manager.shutdown();
});

test("repeated stop calls leave no delayed signals after terminal state", async () => {
	const manager = new JobManager({ killGraceMs: 40 });
	const signals: string[] = [];
	const originalSignal = (manager as any).signal.bind(manager);
	(manager as any).signal = (job: unknown, signal: string) => {
		signals.push(signal);
		originalSignal(job, signal);
	};
	const job = await manager.start({ command: "trap '' TERM; while :; do sleep 0.02; done" });
	await delay(20);
	await Promise.all([manager.stop(job.id), manager.stop(job.id), manager.stop(job.id)]);
	await waitFor(manager, job.id);
	const terminalSignalCount = signals.length;
	await delay(100);
	assert.equal(signals.length, terminalSignalCount);
	assert.equal((manager as any).jobs.get(job.id).killTimer, undefined);
	await manager.shutdown();
});

test("shutdown clears stop timers before returning", async () => {
	const manager = new JobManager({ killGraceMs: 40 });
	const signals: string[] = [];
	const originalSignal = (manager as any).signal.bind(manager);
	(manager as any).signal = (job: unknown, signal: string) => {
		signals.push(signal);
		originalSignal(job, signal);
	};
	const job = await manager.start({ command: "trap '' TERM; while :; do sleep 0.02; done" });
	await delay(20);
	await manager.stop(job.id);
	await manager.shutdown();
	const shutdownSignalCount = signals.length;
	await delay(100);
	assert.equal(signals.length, shutdownSignalCount);
});

test("cleans up background descendants before marking the job terminal", { skip: process.platform === "win32" }, async () => {
	const manager = new JobManager({ killGraceMs: 60 });
	const job = await manager.start({ command: "sleep 5 >/dev/null 2>&1 & echo $!" });
	await waitFor(manager, job.id);
	const pid = Number((await manager.read(job.id)).output.trim().split(/\s+/).at(-1));
	assert.ok(Number.isInteger(pid));
	assert.throws(() => process.kill(pid, 0));
	await manager.shutdown();
});

test("changed payload is sanitized and bounded logs retain a tail", async () => {
	const manager = new JobManager({ maxLogBytes: 10, maxTailBytes: 30 });
	const events: unknown[] = [];
	manager.onChanged((payload) => events.push(payload));
	const job = await manager.start({
		command: "printf '12345678901234567890TAIL'",
		label: "bad\u001b[label",
	});
	await waitFor(manager, job.id);
	const payload = manager.payload();
	assert.equal(payload.runningCount, 0);
	assert.equal(payload.terminalRecentCount, 1);
	assert.equal(payload.primary?.label, "badabel");
	assert.ok(events.length >= 2);
	assert.match((await manager.read(job.id)).output, /TAIL/);
	await manager.shutdown();
});

test("manager renderer is width-safe and strips terminal controls", () => {
	const lines = renderJobsLines(
		[
			{
				id: "01",
				command: "echo \u001b[31mred",
				cwd: "/tmp",
				state: "failed",
				startedAt: 0,
				endedAt: 5,
				exitCode: 1,
				signal: null,
				logPath: "/tmp/x",
				bytesCaptured: 0,
				outputTruncated: false,
			},
		],
		0,
		true,
		"\u001b[2Junsafe",
		19,
	);
	assert.ok(lines.every((line) => visibleWidth(line) <= 19));
	assert.match(lines[0] ?? "", /^╭.*╮$/);
	assert.match(lines.at(-1) ?? "", /^╰.*╯$/);
	assert.ok(lines.slice(1, -1).every((line) => line.startsWith("│") || line.startsWith("├")));
	assert.ok(!lines.join("\n").includes("\u001b[31m"));
	assert.ok(!lines.join("\n").includes("\u001b[2J"));
	assert.equal(sanitizeText("a\n\u0000b"), "a �b");
	assert.equal(sanitizeOutput("\u001b[31mred\u001b[0m"), "red");
});

test("manager list uses bounded windows with omission markers", () => {
	const jobs = Array.from({ length: 25 }, (_, index) => ({
		id: String(index),
		command: `echo ${index}`,
		cwd: "/tmp",
		state: "completed" as const,
		startedAt: index,
		endedAt: index + 1,
		exitCode: 0,
		signal: null,
		logPath: `/tmp/${index}`,
		bytesCaptured: 0,
		outputTruncated: false,
	}));
	const lines = renderJobsLines(jobs, 12, false, "", 60);
	assert.ok(lines.length <= 15);
	assert.match(lines.join("\n"), /newer jobs/);
	assert.match(lines.join("\n"), /older jobs/);
});

test("manager keeps selection stable when a newer job arrives", async () => {
	const manager = new JobManager({ killGraceMs: 30 });
	const selected = await manager.start({ command: "sleep 5 # selected-old", label: "selected-old" });
	const component = new JobsComponent(
		manager,
		{ fg: (_name: string, value: string) => value } as any,
		() => {},
		() => {},
	);
	component.render(80);
	component.handleInput("\r");
	await manager.start({ command: "sleep 5 # new-arrival", label: "new-arrival" });
	await delay(20);
	const detail = component.render(80).join("\n");
	assert.match(detail, new RegExp(selected.id));
	assert.match(detail, /Command: sleep 5 # selected-old/);
	assert.doesNotMatch(detail, /Command: sleep 5 # new-arrival/);
	component.dispose();
	await manager.shutdown();
});

test("extension renderers sanitize parameters and keep results collapsed", async () => {
	const pi = fakePi();
	const { default: extension } = await import("../src/index.js");
	extension(pi as any);
	const tool = pi.tools[0];
	const theme = {
		fg: (_name: string, value: string) => value,
		bold: (value: string) => value,
	};
	const call = tool.renderCall(
		{ action: "read", id: "bad\u001b[31m\nidentifier" },
		theme,
	).render(120).join("\n");
	assert.ok(!call.includes("\u001b"));
	assert.equal(call.split("\n").length, 1);
	const result = tool.renderResult(
		{
			content: [{ type: "text", text: "ignored" }],
			details: {
				action: "start",
				job: {
					id: "01",
					command: "printf '\u001b[31mred\nnext'",
				},
			},
		},
		{ expanded: true, isPartial: false },
		theme,
	).render(120).join("\n");
	assert.ok(!result.includes("\u001b"));
	assert.equal(result.split("\n").length, 2);
	await pi.handlers.session_shutdown({ reason: "quit" });
});

test("extension coalesces completions and reuses singleton on reload", async () => {
	const { default: extension } = await import("../src/index.js");
	const first = fakePi();
	extension(first as any);
	const tool = first.tools[0];
	await tool.execute("x", { action: "start", command: "printf one" });
	await tool.execute("y", { action: "start", command: "printf two" });
	await delay(250);
	assert.equal(first.sent.length, 1);
	assert.match(first.sent[0].message.content, /Background job/);
	assert.deepEqual(first.sent[0].options, { triggerTurn: true, deliverAs: "followUp" });
	await assert.rejects(tool.execute("missing", { action: "read", id: "nope" }), /Unknown background job/);

	const messagesBeforeReload = first.sent.length;
	const running = await tool.execute("z", {
		action: "start",
		command: "sleep 0.15; printf late",
	});
	const id = running.details.job.id;
	await first.handlers.session_shutdown({ reason: "reload" });
	await delay(350);
	assert.equal(first.sent.length, messagesBeforeReload);
	const second = fakePi();
	extension(second as any);
	await delay(180);
	assert.equal(second.sent.length, 1);
	assert.match(second.sent[0].message.content, new RegExp(`job ${id} completed`));
	const list = await second.tools[0].execute("list", { action: "list" });
	assert.ok(list.details.jobs.some((job: { id: string; state: string }) => job.id === id && job.state === "completed"));
	await second.handlers.session_shutdown({ reason: "quit" });
});

function fakePi() {
	const tools: any[] = [];
	const handlers: Record<string, any> = {};
	const sent: any[] = [];
	return {
		tools,
		handlers,
		sent,
		events: {
			emit() {},
			on() {
				return () => {};
			},
		},
		registerTool(tool: any) {
			tools.push(tool);
		},
		registerMessageRenderer() {},
		registerCommand() {},
		on(name: string, handler: any) {
			handlers[name] = handler;
		},
		sendMessage(message: any, options: any) {
			sent.push({ message, options });
		},
	};
}
