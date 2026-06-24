import path from "path"
import * as Tool from "./tool"
import DESCRIPTION from "./bash-background.txt"
import { BackgroundMonitorManager } from "@/background/monitor"
import { Session } from "@/session/session"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { Global } from "@opencode-ai/core/global"
import { Effect, Schema } from "effect"

const id = "bash_background"

export const Parameters = Schema.Struct({
  command: Schema.String.annotate({
    description: "The shell command to run in the background. It keeps running until it exits on its own.",
  }),
  description: Schema.String.annotate({
    description: "A short description of what is being run. Used in the arm and exit messages.",
  }),
})

type BackgroundOps = {
  prompt: (input: {
    sessionID: string
    agent: string
    parts: Array<{ type: "text"; synthetic: boolean; text: string }>
    noReply?: boolean
  }) => Effect.Effect<void>
}

export const BashBackgroundTool = Tool.define(
  id,
  Effect.gen(function* () {
    const manager = yield* BackgroundMonitorManager.Service
    const sessions = yield* Session.Service
    const flags = yield* RuntimeFlags.Service

    const run = Effect.fn("BashBackgroundTool.execute")(function* (
      params: Schema.Schema.Type<typeof Parameters>,
      ctx: Tool.Context,
    ) {
      if (!flags.experimentalBackgroundRun) {
        return yield* Effect.die(new Error("bash_background tool requires OPENCODE_EXPERIMENTAL_BACKGROUND_RUN=true"))
      }

      const ops = (ctx.extra?.promptOps ?? undefined) as BackgroundOps | undefined
      if (!ops) {
        return yield* Effect.die(new Error("bash_background tool requires promptOps in ctx.extra"))
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

      // Capture combined stdout+stderr to a per-call logfile. Because all output
      // is redirected, the manager's stdout stream stays empty (no per-line
      // wake-ups); only onExit fires, giving exactly one notification.
      // Keep this a SINGLE line: the manager runs commands via `eval <json>`, so a
      // literal newline would be re-escaped to a "\n" token and corrupt the command.
      const logPath = path.join(Global.Path.tmp, `bg-${ctx.callID ?? ctx.messageID}.log`)
      const command = `( ${params.command} ) > '${logPath}' 2>&1`

      const onExit = Effect.fn("BashBackgroundTool.onExit")(function* (reason: string) {
        yield* ops
          .prompt({
            sessionID: ctx.sessionID,
            agent: ctx.agent,
            parts: [
              {
                type: "text",
                synthetic: true,
                text: `[Background: ${params.description}] exited (${reason}). Output in ${logPath}`,
              },
            ],
          })
          .pipe(Effect.ignore)
      })

      const { id: backgroundId } = yield* manager.start({
        sessionID: ctx.sessionID,
        command,
        description: params.description,
        cwd,
        kind: "background",
        onEvent: () => Effect.void,
        onExit: (reason) => onExit(reason),
      })

      return {
        title: params.description,
        metadata: { background: true, backgroundId, description: params.description, logPath },
        output:
          `Background run armed (${backgroundId}) for "${params.description}". ` +
          `Output streaming to ${logPath}. You will be notified when it exits; ` +
          `read the logfile to check progress, or stop it with bash_background_stop (id: ${backgroundId}).`,
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
