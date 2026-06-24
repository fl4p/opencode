import * as Tool from "./tool"
import { BackgroundJob } from "@/background/job"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { Effect, Schema } from "effect"
import { TYPE } from "./bash-background"

const id = "bash_background_stop"

export const Parameters = Schema.Struct({
  id: Schema.String.annotate({
    description: "The background run id returned by bash_background.",
  }),
})

export const BashBackgroundStopTool = Tool.define(
  id,
  Effect.gen(function* () {
    const jobs = yield* BackgroundJob.Service
    const flags = yield* RuntimeFlags.Service

    const run = Effect.fn("BashBackgroundStopTool.execute")(function* (
      params: Schema.Schema.Type<typeof Parameters>,
      ctx: Tool.Context,
    ) {
      if (!flags.experimentalBackgroundRun) {
        return yield* Effect.die(
          new Error("bash_background_stop tool requires OPENCODE_EXPERIMENTAL_BACKGROUND_RUN=true"),
        )
      }

      const info = yield* jobs.get(params.id)
      // Only allow stopping a running bash_background job from this session.
      if (!info || info.type !== TYPE || info.metadata?.["sessionId"] !== ctx.sessionID || info.status !== "running") {
        return {
          title: params.id,
          metadata: { stopped: false, backgroundId: params.id },
          output: `No active background run with id "${params.id}" in this session.`,
        }
      }

      yield* jobs.cancel(params.id)

      const label = info.title ?? (info.metadata?.["description"] as string | undefined) ?? params.id
      return {
        title: label,
        metadata: { stopped: true, backgroundId: params.id },
        output: `Stopped background run ${params.id} ("${label}").`,
      }
    })

    return {
      parameters: Parameters,
      description: "Stop a background run previously started with bash_background, by its id.",
      execute: (params: Schema.Schema.Type<typeof Parameters>, ctx: Tool.Context) =>
        run(params, ctx).pipe(Effect.orDie),
    }
  }),
)
