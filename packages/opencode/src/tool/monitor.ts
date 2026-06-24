import * as Tool from "./tool"
import DESCRIPTION from "./monitor.txt"
import { BackgroundMonitorManager } from "@/background/monitor"
import { Session } from "@/session/session"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { Effect, Schema } from "effect"

const id = "monitor"

export const Parameters = Schema.Struct({
  command: Schema.String.annotate({
    description: "The shell command to run. It must keep running and emit one stdout line per actual event.",
  }),
  description: Schema.String.annotate({
    description: "A short description of what is being watched. Used in event messages.",
  }),
  timeoutMs: Schema.optional(Schema.Number).annotate({
    description: "Optional maximum lifetime of the monitor in milliseconds. If omitted, it runs until the process exits or the session ends.",
  }),
})

type MonitorOps = {
  prompt: (input: {
    sessionID: string
    agent: string
    parts: Array<{ type: "text"; synthetic: boolean; text: string }>
    noReply?: boolean
  }) => Effect.Effect<void>
  wake: (sessionID: string) => Effect.Effect<void>
}

export const MonitorTool = Tool.define(
  id,
  Effect.gen(function* () {
    const manager = yield* BackgroundMonitorManager.Service
    const sessions = yield* Session.Service
    const flags = yield* RuntimeFlags.Service

    const run = Effect.fn("MonitorTool.execute")(function* (
      params: Schema.Schema.Type<typeof Parameters>,
      ctx: Tool.Context,
    ) {
      if (!flags.experimentalMonitor) {
        return yield* Effect.die(
          new Error("Monitor tool requires OPENCODE_EXPERIMENTAL_MONITOR=true"),
        )
      }

      const ops = (ctx.extra?.promptOps ?? undefined) as MonitorOps | undefined
      if (!ops) {
        return yield* Effect.die(new Error("Monitor tool requires promptOps in ctx.extra"))
      }

      yield* ctx.ask({
        permission: id,
        patterns: [params.command],
        always: ["*"],
        metadata: {
          description: params.description,
          command: params.command,
        },
      })

      const session = yield* sessions.get(ctx.sessionID).pipe(Effect.orDie)
      const cwd = session.directory

      // Re-arming replaces the previous watch, but must NOT kill concurrent
      // background runs (bash_background) sharing this manager — stop monitors only.
      yield* manager.stopAllForSessionByKind(ctx.sessionID, "monitor")

      const onEvent = Effect.fn("MonitorTool.onEvent")(function* (line: string) {
        yield* ops
          .prompt({
            sessionID: ctx.sessionID,
            agent: ctx.agent,
            parts: [
              {
                type: "text",
                synthetic: true,
                text: `[Monitor: ${params.description}] Event: ${line}`,
              },
            ],
          })
          .pipe(Effect.ignore)
      })

      const onExit = Effect.fn("MonitorTool.onExit")(function* (reason: string) {
        yield* ops
          .prompt({
            sessionID: ctx.sessionID,
            agent: ctx.agent,
            parts: [
              {
                type: "text",
                synthetic: true,
                text: `[Monitor: ${params.description}] Monitor exited (${reason}). If you still need to watch, re-arm with a working command.`,
              },
            ],
          })
          .pipe(Effect.ignore)
      })

      const { id: monitorId } = yield* manager.start({
        sessionID: ctx.sessionID,
        command: params.command,
        description: params.description,
        cwd,
        kind: "monitor",
        timeoutMs: params.timeoutMs,
        onEvent: (line) => onEvent(line),
        onExit: (reason) => onExit(reason),
      })

      return {
        title: params.description,
        metadata: { monitor: true, monitorId, description: params.description },
        output: `Monitor armed (${monitorId}) for "${params.description}". Events will arrive as new messages. Do not re-arm it unless you need a different watch.`,
      }
    })

    return {
      description: DESCRIPTION,
      parameters: Parameters,
      execute: (params: Schema.Schema.Type<typeof Parameters>, ctx: Tool.Context) =>
        run(params, ctx).pipe(Effect.orDie),
    }
  }),
)
