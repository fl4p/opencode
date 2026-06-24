import * as Tool from "./tool"
import { BackgroundMonitorManager } from "@/background/monitor"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { Effect, Schema } from "effect"

const id = "bash_background_stop"

export const Parameters = Schema.Struct({
  id: Schema.String.annotate({
    description: "The background run id returned by bash_background (e.g. \"monitor-3\").",
  }),
})

export const BashBackgroundStopTool = Tool.define(
  id,
  Effect.gen(function* () {
    const manager = yield* BackgroundMonitorManager.Service
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

      const info = yield* manager.get(params.id)
      // Only allow stopping a background run that belongs to this session.
      if (!info || info.sessionID !== ctx.sessionID || info.kind !== "background") {
        return {
          title: params.id,
          metadata: { stopped: false, backgroundId: params.id },
          output: `No active background run with id "${params.id}" in this session.`,
        }
      }

      yield* manager.stop(params.id)

      return {
        title: info.description,
        metadata: { stopped: true, backgroundId: params.id },
        output: `Stopped background run ${params.id} ("${info.description}").`,
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
