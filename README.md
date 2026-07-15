# pi-background-jobs

A Pi extension for **non-interactive** background shell jobs. It is intentionally not a PTY, terminal multiplexer, or interactive-shell replacement: jobs receive no stdin and run through `SHELL -c` (falling back to `/bin/sh`). Node.js 22.19 or newer is required.

## Install

Install as a Pi package, or put this directory in your Pi package configuration. Pi loads `src/index.ts` as the extension entrypoint:

```json
{ "packages": ["/absolute/path/to/pi-background-jobs"] }
```

For development, install the locked dependency graph with `npm ci`, then run Pi with `pi -e ./src/index.ts` from this directory. The extension starts no processes or timers until `background_job` is invoked.

## Tool

`background_job` has four actions:

- `start` — requires `command` and a concise human-readable `label`; accepts optional `cwd` and positive `timeoutMs`. Returns a short process-local id immediately.
- `list` — lists active and retained recent jobs.
- `read` — requires `id`; returns a bounded output tail.
- `stop` — requires `id`; sends TERM to the process group and KILL after a short bounded grace period if needed.

stdout and stderr are captured together in a bounded in-memory UTF-8 tail. Each job records start/end timestamps, exit code, signal, state, and bounded output metadata. Recent terminal jobs are bounded to 40 and the output tail to 16 KiB by default. Output is sanitised before display; no terminal escape sequences or hidden model reasoning are forwarded.

Start rows show the human-readable label and collapse to `Running in background (/jobs to manage)`. Expand a tool row with Pi's configured tool-expand shortcut to inspect available detail. Completion messages lead with that label and retain the short process-local id only as secondary context—for example, `Validate configuration (job 0c) completed (exit 0).` They are coalesced briefly, provide a factual bounded tail to the model, and request a follow-up turn.

## `/jobs`

`/jobs` opens a session-scoped manager. Use Up/Down to select, Enter for detail/output, `f` to follow output, `s` to stop, `d` to dismiss terminal jobs, `h` for help, and Escape to close. The manager has width-bounded, sanitized output and disposes its follow timer on close.

## Event bus

The extension emits `background-jobs:changed` through `pi.events` whenever aggregate state changes. The stable sanitized payload is:

```ts
{
  runningCount: number;
  terminalRecentCount: number;
  oldestStart?: number; // epoch milliseconds; only when a job runs
  primary?: { id: string; label?: string; command: string; startedAt: number };
}
```

`primary.command` is a sanitized, length-bounded display value, and `primary.startedAt` belongs to that same job so elapsed labels remain accurate. This payload is suitable for a custom footer; it contains no output or filesystem paths.

## Lifecycle

A global process-local singleton preserves running jobs and pending completions across `/reload`, then rebinds callbacks without duplicate notifications, timers, or listeners. Pending completions park while unbound and resume on rebind; their queue and delivery batches are bounded with an omission notice. Ordinary session shutdown serializes with starts, stops active process groups, and clears timers. Child process groups are cleaned after their shell leader exits so ordinary `command &` descendants cannot silently escape; independently re-daemonized processes outside the original process group are outside this extension's ownership. Jobs do not survive a Pi process restart.

## Development

```bash
npm ci
npm run check
```

The tests exercise completion/failure/timeout/stopping, output bounds, event payloads, manager rendering safety, completion coalescing, reload reuse, and shutdown cleanup.

## License

MIT. See [LICENSE](LICENSE).
