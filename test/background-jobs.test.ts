import assert from "node:assert/strict";
import test from "node:test";
import { readdir } from "node:fs/promises";
import { tmpdir } from "node:os";
import { setTimeout as delay } from "node:timers/promises";
import {
	JobManager,
	MAX_TIMEOUT_MS,
	sanitizeOutput,
	sanitizeText,
} from "../src/job-manager.js";
import {
	JobsComponent,
	renderActiveJobsLine,
	renderJobsLines,
} from "../src/manager-ui.js";
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

function hasUnpairedSurrogate(value: string): boolean {
	for (let index = 0; index < value.length; index += 1) {
		const code = value.charCodeAt(index);
		if (code >= 0xd800 && code <= 0xdbff) {
			if (
				value.charCodeAt(index + 1) < 0xdc00 ||
				value.charCodeAt(index + 1) > 0xdfff
			)
				return true;
			index += 1;
		} else if (code >= 0xdc00 && code <= 0xdfff) return true;
	}
	return false;
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
	const job = await manager.start({
		command: "sleep 5",
		timeoutMs: MAX_TIMEOUT_MS,
	});
	await manager.stop(job.id);
	await waitFor(manager, job.id);
	await manager.shutdown();
});

test("snapshots start options before queued lifecycle work", async () => {
	const manager = new JobManager();
	const input = { command: "printf original", label: "original" };
	const started = manager.start(input);
	input.command = "printf mutated";
	input.label = "x".repeat(241);
	const job = await started;
	assert.equal(job.command, "printf original");
	assert.equal(job.label, "original");
	await waitFor(manager, job.id);
	await manager.shutdown();
});

test("validates manager numeric bounds and read limits", async () => {
	for (const option of ["maxTailBytes", "maxRecent", "killGraceMs"] as const) {
		for (const value of [-1, 1.5, Number.NaN, Number.POSITIVE_INFINITY]) {
			assert.throws(
				() => new JobManager({ [option]: value }),
				new RegExp(`${option} must be a non-negative safe integer`),
			);
		}
	}
	assert.doesNotThrow(
		() => new JobManager({ maxTailBytes: 0, maxRecent: 0, killGraceMs: 0 }),
	);
	const manager = new JobManager({
		maxTailBytes: 0,
		maxRecent: 1,
		killGraceMs: 0,
	});
	const job = await manager.start({ command: "printf output" });
	await waitFor(manager, job.id);
	assert.equal((await manager.read(job.id)).output, "");
	for (const value of [-1, 1.5, Number.NaN, Number.POSITIVE_INFINITY]) {
		await assert.rejects(
			manager.read(job.id, value),
			/maxBytes must be a non-negative safe integer/,
		);
	}
	await manager.shutdown();
});

test("serializes parallel starts and makes shutdown idempotent", async () => {
	const manager = new JobManager({ killGraceMs: 30 });
	const jobs = await Promise.all(
		Array.from({ length: 6 }, () => manager.start({ command: "sleep 5" })),
	);
	assert.equal(new Set(jobs.map((job) => job.id)).size, jobs.length);
	assert.equal(
		manager.list().filter((job) => job.state === "running").length,
		jobs.length,
	);
	const firstShutdown = manager.shutdown();
	assert.equal(manager.shutdown(), firstShutdown);
	await firstShutdown;
	assert.equal(
		manager.list().filter((job) => job.state === "running").length,
		0,
	);
	await assert.rejects(manager.start({ command: "true" }), /shut down/);
});

test("shutdown racing queued starts leaves no running job", async () => {
	const manager = new JobManager({ killGraceMs: 30 });
	const starts = Array.from({ length: 6 }, () =>
		manager.start({ command: "sleep 5" }),
	);
	const shutdown = manager.shutdown();
	const outcomes = await Promise.allSettled([...starts, shutdown]);
	assert.ok(
		outcomes.slice(0, -1).every((result) => result.status === "rejected"),
	);
	assert.equal(
		manager.list().filter((job) => job.state === "running").length,
		0,
	);
});

test("listener failures do not change operation results or create unhandled rejections", async () => {
	const errors: unknown[] = [];
	const manager = new JobManager({
		killGraceMs: 30,
		onListenerError: (error) => errors.push(error),
	});
	let changed = 0;
	let completed = 0;
	const unhandled: unknown[] = [];
	const onUnhandled = (reason: unknown) => unhandled.push(reason);
	process.on("unhandledRejection", onUnhandled);
	manager.onChanged(async () => {
		await delay(1);
		throw new Error("changed listener failed");
	});
	manager.onChanged(() => {
		changed += 1;
	});
	manager.onCompleted(async () => {
		await delay(1);
		throw new Error("completion listener failed");
	});
	manager.onCompleted(() => {
		completed += 1;
	});
	try {
		const job = await manager.start({ command: "sleep 5" });
		assert.equal(job.state, "running");
		const stopping = await manager.stop(job.id);
		assert.equal(stopping.state, "running");
		await waitFor(manager, job.id);
		await delay(30);
		assert.ok(changed >= 2);
		assert.equal(completed, 1);
		assert.ok(
			errors.some((error) => String(error).includes("changed listener failed")),
		);
		assert.ok(
			errors.some((error) =>
				String(error).includes("completion listener failed"),
			),
		);
		assert.deepEqual(unhandled, []);
	} finally {
		process.off("unhandledRejection", onUnhandled);
		await manager.shutdown();
	}
});

test("memory-only tails preserve complete UTF-8 across chunks and byte boundaries", async () => {
	const splitManager = new JobManager({ maxTailBytes: 16, killGraceMs: 20 });
	const splitJob = await splitManager.start({ command: "sleep 5" });
	const splitInternal = (splitManager as any).jobs.get(splitJob.id);
	for (const [bytes, character] of [
		[[0xc2, 0xa2], "¢"],
		[[0xe2, 0x82, 0xac], "€"],
		[[0xf0, 0x9f, 0x98, 0x80], "😀"],
	] as const) {
		(splitManager as any).capture(
			splitInternal,
			Buffer.from(bytes.slice(0, -1)),
		);
		assert.doesNotMatch((await splitManager.read(splitJob.id)).output, /�/);
		(splitManager as any).capture(splitInternal, Buffer.from(bytes.slice(-1)));
		assert.ok(
			(await splitManager.read(splitJob.id)).output.includes(character),
		);
	}
	await splitManager.shutdown();

	for (const character of ["¢", "€", "😀"]) {
		const manager = new JobManager({
			maxTailBytes: Buffer.byteLength(character) - 1,
			killGraceMs: 20,
		});
		const job = await manager.start({ command: "sleep 5" });
		const internal = (manager as any).jobs.get(job.id);
		(manager as any).capture(internal, Buffer.from(`x${character}`));
		const output = (await manager.read(job.id)).output;
		assert.doesNotMatch(output, /�/);
		assert.equal(output, "");
		await manager.shutdown();
	}
});

test("starts no temporary disk-log directory", async () => {
	const before = new Set(
		(await readdir(tmpdir())).filter((name) =>
			name.startsWith("pi-background-jobs-"),
		),
	);
	const manager = new JobManager();
	const job = await manager.start({ command: "printf memory" });
	await waitFor(manager, job.id);
	const after = (await readdir(tmpdir())).filter((name) =>
		name.startsWith("pi-background-jobs-"),
	);
	assert.deepEqual(
		after.filter((name) => !before.has(name)),
		[],
	);
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
	const job = await manager.start({
		command: "trap '' TERM; while :; do sleep 0.02; done",
	});
	await delay(20);
	await Promise.all([
		manager.stop(job.id),
		manager.stop(job.id),
		manager.stop(job.id),
	]);
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
	const job = await manager.start({
		command: "trap '' TERM; while :; do sleep 0.02; done",
	});
	await delay(20);
	await manager.stop(job.id);
	await manager.shutdown();
	const shutdownSignalCount = signals.length;
	await delay(100);
	assert.equal(signals.length, shutdownSignalCount);
});

test(
	"cleans up background descendants before marking the job terminal",
	{ skip: process.platform === "win32" },
	async () => {
		const manager = new JobManager({ killGraceMs: 60 });
		const job = await manager.start({
			command: "sleep 5 >/dev/null 2>&1 & echo $!",
		});
		await waitFor(manager, job.id);
		const pid = Number(
			(await manager.read(job.id)).output.trim().split(/\s+/).at(-1),
		);
		assert.ok(Number.isInteger(pid));
		assert.throws(() => process.kill(pid, 0));
		await manager.shutdown();
	},
);

test("changed payload is sanitized and bounded memory tails retain output", async () => {
	const manager = new JobManager({ maxTailBytes: 10 });
	const events: unknown[] = [];
	manager.onChanged((payload) => {
		events.push(payload);
	});
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

test("active job summary lists running labels in one width-bounded row", () => {
	const jobs = [
		["Typecheck", "running"],
		["Test suite", "running"],
		["Build", "running"],
		["Dev server", "running"],
		["Already done", "completed"],
	].map(([label, state], index) => ({
		id: String(index),
		command: `command ${index}`,
		cwd: "/tmp",
		label: `${label}${index === 0 ? "\u001b[31m" : ""}`,
		state: state as "running" | "completed",
		startedAt: index,
		endedAt: state === "completed" ? index + 1 : undefined,
		exitCode: state === "completed" ? 0 : undefined,
		bytesCaptured: 0,
		outputTruncated: false,
	}));
	const line = renderActiveJobsLine(jobs, 46);
	assert.equal(line, "  Background: Typecheck · Test suite · +2 more");
	assert.ok(visibleWidth(line ?? "") <= 46);
	assert.ok(visibleWidth(renderActiveJobsLine(jobs, 28) ?? "") <= 28);
	assert.equal(
		renderActiveJobsLine(
			jobs.map((job) => ({ ...job, state: "completed" as const })),
			46,
		),
		undefined,
	);
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
	assert.ok(
		lines
			.slice(1, -1)
			.every((line) => line.startsWith("│") || line.startsWith("├")),
	);
	assert.ok(!lines.join("\n").includes("\u001b[31m"));
	assert.ok(!lines.join("\n").includes("\u001b[2J"));
	assert.equal(sanitizeText("a\n\u0000b"), "a �b");
	assert.equal(sanitizeOutput("\u001b[31mred\u001b[0m"), "red");
});

test("sanitizers truncate Unicode code points within zero and one-character limits", () => {
	for (const limit of [0, 1, 2]) {
		const output = sanitizeOutput("ab😀", limit);
		const text = sanitizeText("😀ab", limit);
		assert.ok(Array.from(output).length <= limit);
		assert.ok(Array.from(text).length <= limit);
		assert.equal(hasUnpairedSurrogate(output), false);
		assert.equal(hasUnpairedSurrogate(text), false);
	}
	assert.equal(sanitizeOutput("ab😀", 0), "");
	assert.equal(sanitizeOutput("ab😀", 1), "…");
	assert.equal(sanitizeOutput("ab😀", 2), "…😀");
	assert.equal(sanitizeText("😀ab", 0), "");
	assert.equal(sanitizeText("😀ab", 1), "…");
	assert.equal(sanitizeText("😀ab", 2), "😀…");
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
	const selected = await manager.start({
		command: "sleep 5 # selected-old",
		label: "selected-old",
	});
	const component = new JobsComponent(
		manager,
		{ fg: (_name: string, value: string) => value } as any,
		() => {},
		() => {},
	);
	component.render(80);
	component.handleInput("\r");
	await manager.start({
		command: "sleep 5 # new-arrival",
		label: "new-arrival",
	});
	await delay(20);
	const detail = component.render(80).join("\n");
	assert.match(detail, new RegExp(selected.id));
	assert.match(detail, /Command: sleep 5 # selected-old/);
	assert.doesNotMatch(detail, /Command: sleep 5 # new-arrival/);
	component.dispose();
	await manager.shutdown();
});

test("extension mounts running labels above the editor and clears them on completion", async () => {
	const pi = fakePi();
	const { default: extension } = await import("../src/index.js");
	extension(pi as any);
	const widgetWrites: Array<{ key: string; value: unknown; options?: unknown }> = [];
	await pi.handlers.session_start(
		{},
		{
			ui: {
				setStatus() {},
				setWidget(key: string, value: unknown, options?: unknown) {
					widgetWrites.push({ key, value, options });
				},
			},
		},
	);
	const started = await pi.tools[0].execute("widget", {
		action: "start",
		command: "sleep 0.1",
		label: "Run widget test",
	});
	const mounted = [...widgetWrites]
		.reverse()
		.find((write) => typeof write.value === "function");
	assert.ok(mounted);
	assert.equal(mounted.key, "background-jobs-active");
	assert.deepEqual(mounted.options, { placement: "aboveEditor" });
	const widgetFactory = mounted.value as (
		tui: unknown,
		theme: { fg(name: string, value: string): string },
	) => { render(width: number): string[] };
	const component = widgetFactory(
		{},
		{ fg: (_name: string, value: string) => value },
	);
	assert.match(component.render(80).join("\n"), /Background: Run widget test/);
	await waitFor(
		(globalThis as any).__piBackgroundJobsRuntime.manager,
		started.details.job.id,
	);
	assert.equal(widgetWrites.at(-1)?.value, undefined);
	await pi.handlers.session_shutdown({ reason: "quit" });
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
	const call = tool
		.renderCall({ action: "read", id: "bad\u001b[31m\nidentifier" }, theme)
		.render(120)
		.join("\n");
	assert.ok(!call.includes("\u001b"));
	assert.equal(call.split("\n").length, 1);
	const result = tool
		.renderResult(
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
		)
		.render(120)
		.join("\n");
	assert.ok(!result.includes("\u001b"));
	assert.equal(result.split("\n").length, 2);
	await pi.handlers.session_shutdown({ reason: "quit" });
});

test("extension coalesces completions and reuses singleton on reload", async () => {
	const { default: extension } = await import("../src/index.js");
	const first = fakePi();
	extension(first as any);
	const tool = first.tools[0];
	await assert.rejects(
		tool.execute("missing-label", { action: "start", command: "printf nope" }),
		/label is required for start/,
	);
	const firstStart = await tool.execute("x", {
		action: "start",
		command: "printf one",
		label: "Print first result",
	});
	assert.match(
		firstStart.content[0].text,
		new RegExp(`Started Print first result \\(job ${firstStart.details.job.id}\\)`),
	);
	await tool.execute("y", {
		action: "start",
		command: "printf two",
		label: "Print second result",
	});
	await delay(250);
	assert.equal(first.sent.length, 1);
	assert.match(first.sent[0].message.content, /Print first result \(job /);
	assert.match(first.sent[0].message.content, /Print second result \(job /);
	assert.deepEqual(first.sent[0].options, {
		triggerTurn: true,
		deliverAs: "followUp",
	});
	await assert.rejects(
		tool.execute("missing", { action: "read", id: "nope" }),
		/Unknown background job/,
	);

	const messagesBeforeReload = first.sent.length;
	const running = await tool.execute("z", {
		action: "start",
		command: "sleep 0.15; printf late",
		label: "Print delayed result",
	});
	const id = running.details.job.id;
	await first.handlers.session_shutdown({ reason: "reload" });
	await delay(350);
	assert.equal(first.sent.length, messagesBeforeReload);
	assert.equal(
		(globalThis as any).__piBackgroundJobsRuntime.completionTimer,
		undefined,
	);
	const second = fakePi();
	extension(second as any);
	await delay(180);
	assert.equal(second.sent.length, 1);
	assert.match(
		second.sent[0].message.content,
		new RegExp(`Print delayed result \\(job ${id}\\) completed`),
	);
	const list = await second.tools[0].execute("list", { action: "list" });
	assert.ok(
		list.details.jobs.some(
			(job: { id: string; state: string }) =>
				job.id === id && job.state === "completed",
		),
	);
	await second.handlers.session_shutdown({ reason: "quit" });
});

test("extension bounds parked completion queues and delivery batches", async () => {
	const { default: extension } = await import("../src/index.js");
	const first = fakePi();
	extension(first as any);
	await first.handlers.session_shutdown({ reason: "reload" });
	const current = (globalThis as any).__piBackgroundJobsRuntime;
	const jobs = await Promise.all(
		Array.from({ length: 66 }, () =>
			current.manager.start({ command: "printf queued" }),
		),
	);
	const deadline = Date.now() + 10_000;
	while (
		current.pending.length + current.pendingOmitted < jobs.length &&
		Date.now() < deadline
	)
		await delay(25);
	assert.equal(current.pending.length + current.pendingOmitted, jobs.length);
	assert.equal(current.pending.length, 64);
	assert.equal(current.pendingOmitted, 2);
	assert.equal(current.completionTimer, undefined);

	const second = fakePi();
	extension(second as any);
	await delay(180);
	assert.ok(second.sent.length >= 1);
	const delivered = second.sent[0].message;
	assert.ok(delivered.details.length <= 8);
	assert.ok(
		JSON.stringify({ content: delivered.content, details: delivered.details })
			.length <= 16_000,
	);
	assert.match(delivered.content, /2 additional completions were omitted/);
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
