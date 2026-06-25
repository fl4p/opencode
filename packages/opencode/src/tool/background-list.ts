import * as Tool from "./tool"
import { BackgroundJob } from "@/background/job"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { Effect, Schema } from "effect"
import { TYPE as MONITOR_TYPE } from "./monitor"
import { TYPE as BASH_BG_TYPE } from "./bash-background"

const id = "background_list"

const LISTABLE = new Set<string>([MONITOR_TYPE, BASH_BG_TYPE])

export const Parameters = Schema.Struct({})

const fmtAge = (ms: number) => {
  const s = Math.max(0, Math.floor(ms / 1000))
  if (s < 60) return `${s}s`
  const m = Math.floor(s / 60)
  if (m < 60) return `${m}m${s % 60}s`
  const h = Math.floor(m / 60)
  return `${h}h${m % 60}m`
}

export const BackgroundListTool = Tool.define(
  id,
  Effect.gen(function* () {
    const jobs = yield* BackgroundJob.Service
    const flags = yield* RuntimeFlags.Service

    const run = Effect.fn("BackgroundListTool.execute")(function* (
      _params: Schema.Schema.Type<typeof Parameters>,
      ctx: Tool.Context,
    ) {
      if (!flags.experimentalMonitor && !flags.experimentalBackgroundRun) {
        return yield* Effect.die(
          new Error("background_list tool requires OPENCODE_EXPERIMENTAL_MONITOR or OPENCODE_EXPERIMENTAL_BACKGROUND_RUN"),
        )
      }

      const now = Date.now()
      // Running monitors + background runs armed in THIS session, oldest first.
      const running = (yield* jobs.list())
        .filter((j) => LISTABLE.has(j.type) && j.status === "running" && j.metadata?.["sessionId"] === ctx.sessionID)
        .sort((a, b) => a.started_at - b.started_at)

      const rows = running.map((j) => ({
        id: j.id,
        kind: j.type === MONITOR_TYPE ? ("monitor" as const) : ("background_run" as const),
        description: (j.metadata?.["description"] as string | undefined) ?? j.title ?? "",
        logPath: (j.metadata?.["logPath"] as string | undefined) ?? undefined,
        ageMs: now - j.started_at,
      }))

      if (rows.length === 0) {
        return {
          title: "background_list",
          metadata: { count: 0, jobs: rows },
          output: "No active monitors or background runs in this session.",
        }
      }

      const lines = rows.map((r) => {
        const kind = r.kind === "monitor" ? "monitor" : "background run"
        const log = r.logPath ? ` (log: ${r.logPath})` : ""
        return `- [${kind}] ${r.id} "${r.description}" — running ${fmtAge(r.ageMs)}${log}`
      })
      return {
        title: `${rows.length} active background job${rows.length === 1 ? "" : "s"}`,
        metadata: { count: rows.length, jobs: rows },
        output:
          `Active background jobs in this session (${rows.length}). Stop any with background_stop ` +
          `(by id or description):\n${lines.join("\n")}`,
      }
    })

    return {
      parameters: Parameters,
      description:
        "List the monitors (from `monitor`) and background runs (from `bash_background`) currently running in " +
        "this session, with their id, kind, description, and how long they've been running. Use this to see " +
        "what's active before stopping one with background_stop — e.g. to find a stale or duplicate watch.",
      execute: (params: Schema.Schema.Type<typeof Parameters>, ctx: Tool.Context) =>
        run(params, ctx).pipe(Effect.orDie),
    }
  }),
)
