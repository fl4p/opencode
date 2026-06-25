import * as Tool from "./tool"
import { BackgroundJob } from "@/background/job"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { Effect, Schema } from "effect"
import { TYPE as MONITOR_TYPE } from "./monitor"
import { TYPE as BASH_BG_TYPE } from "./bash-background"

const id = "background_stop"

const STOPPABLE = new Set<string>([MONITOR_TYPE, BASH_BG_TYPE])

export const Parameters = Schema.Struct({
  id: Schema.optional(
    Schema.String.annotate({
      description:
        "The id returned when the monitor or background run was armed (e.g. job_...). Stops exactly that job.",
    }),
  ),
  description: Schema.optional(
    Schema.String.annotate({
      description:
        "Alternatively, the exact description it was armed with. Stops every running monitor / background run " +
        "with that description in this session. Use this when you don't have the id handy.",
    }),
  ),
})

export const BackgroundStopTool = Tool.define(
  id,
  Effect.gen(function* () {
    const jobs = yield* BackgroundJob.Service
    const flags = yield* RuntimeFlags.Service

    const run = Effect.fn("BackgroundStopTool.execute")(function* (
      params: Schema.Schema.Type<typeof Parameters>,
      ctx: Tool.Context,
    ) {
      if (!flags.experimentalMonitor && !flags.experimentalBackgroundRun) {
        return yield* Effect.die(
          new Error("background_stop tool requires OPENCODE_EXPERIMENTAL_MONITOR or OPENCODE_EXPERIMENTAL_BACKGROUND_RUN"),
        )
      }

      if (!params.id && !params.description) {
        return {
          title: "background_stop",
          metadata: { stopped: false, count: 0, ids: [] as string[] },
          output: "Provide either the id or the exact description of the monitor / background run to stop.",
        }
      }

      // Only ever stop RUNNING monitor/bash_background jobs armed in THIS session. The id is
      // globally unique across both job types, so an id stop is type-agnostic; a description
      // stop matches whichever running jobs in this session carry that description.
      const running = (yield* jobs.list()).filter(
        (j) => STOPPABLE.has(j.type) && j.status === "running" && j.metadata?.["sessionId"] === ctx.sessionID,
      )
      const targets = params.id
        ? running.filter((j) => j.id === params.id)
        : running.filter((j) => j.metadata?.["description"] === params.description)

      if (targets.length === 0) {
        const ref = params.id ? `id "${params.id}"` : `description "${params.description}"`
        return {
          title: params.id ?? params.description ?? "background_stop",
          metadata: { stopped: false, count: 0, ids: [] as string[] },
          output: `No active monitor or background run matching ${ref} in this session.`,
        }
      }

      yield* Effect.forEach(targets, (j) => jobs.cancel(j.id), { concurrency: "unbounded", discard: true })

      const labels = targets.map((j) => (j.metadata?.["description"] as string | undefined) ?? j.id)
      const kind = (t: string) => (t === MONITOR_TYPE ? "monitor" : "background run")
      return {
        title: labels[0] ?? "background_stop",
        metadata: { stopped: true, count: targets.length, ids: targets.map((j) => j.id) as string[] },
        output:
          targets.length === 1
            ? `Stopped ${kind(targets[0]!.type)} ${targets[0]!.id} ("${labels[0]}").`
            : `Stopped ${targets.length} jobs: ${labels.map((l) => `"${l}"`).join(", ")}.`,
      }
    })

    return {
      parameters: Parameters,
      description:
        "Stop a running monitor (armed with `monitor`) or background run (armed with `bash_background`) by its " +
        "id (returned when armed) or its exact description. Use this to retire a watch or run you no longer " +
        "need — e.g. a monitor armed with a wrong path, a duplicate from a reworded re-arm, or a long job " +
        "you want to cancel. Works for BOTH tool types; you don't need to know which one armed it.",
      execute: (params: Schema.Schema.Type<typeof Parameters>, ctx: Tool.Context) =>
        run(params, ctx).pipe(Effect.orDie),
    }
  }),
)
