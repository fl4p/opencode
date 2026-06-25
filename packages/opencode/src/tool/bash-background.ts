import path from "path"
import { readdirSync, statSync, unlinkSync } from "node:fs"
import * as Tool from "./tool"
import DESCRIPTION from "./bash-background.txt"
import { BackgroundJob } from "@/background/job"
import { Session } from "@/session/session"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { Global } from "@opencode-ai/core/global"
import { ChildProcessSpawner } from "effect/unstable/process/ChildProcessSpawner"
import { Effect, Schema } from "effect"
import { makeShellCommand, runShellJob, BackgroundJobsEvent } from "./background-shell"
import { EventV2Bridge } from "@/event-v2-bridge"

const id = "bash_background"
export const TYPE = "bash_background"

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
    const jobs = yield* BackgroundJob.Service
    const sessions = yield* Session.Service
    const flags = yield* RuntimeFlags.Service
    const spawner = yield* ChildProcessSpawner
    const bridge = yield* EventV2Bridge.Service

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
        // Scope "always allow" to THIS command, not "*" — bash_background runs arbitrary
        // shell; granting "*" once would permanently authorize any future command.
        always: [params.command],
        metadata: { description: params.description, command: params.command },
      })

      const session = yield* sessions.get(ctx.sessionID).pipe(Effect.orDie)

      // Best-effort sweep of stale bg-*.log files. The exit note tells the model to read
      // the log AFTER exit, so we can't unlink on exit; instead reap logs older than 24h
      // on each arm so they don't accumulate in the tmp dir forever.
      yield* Effect.sync(() => {
        try {
          const dir = Global.Path.tmp
          const cutoff = Date.now() - 24 * 60 * 60 * 1000
          for (const f of readdirSync(dir)) {
            if (!f.startsWith("bg-") || !f.endsWith(".log")) continue
            const p = path.join(dir, f)
            try {
              if (statSync(p).mtimeMs < cutoff) unlinkSync(p)
            } catch {}
          }
        } catch {}
      }).pipe(Effect.ignore)

      // Capture combined stdout+stderr to a per-call logfile. Keep this a SINGLE
      // line: the shell runs commands via `eval <json>`, so a literal newline
      // would be re-escaped to a "\n" token and corrupt the command.
      const logPath = path.join(Global.Path.tmp, `bg-${ctx.callID ?? ctx.messageID}.log`)
      const command = makeShellCommand(`( ${params.command} ) > '${logPath}' 2>&1`, session.directory)

      // No per-line events (output goes to the logfile); inject one note on exit.
      // Exit note via onExit (forked off the run fiber by runShellJob), NOT an inline
      // Effect.tap: if the model re-arms/cancels on the note, an awaited tap would
      // self-join this job's run fiber (the exit-then-rearm deadlock).
      const job = runShellJob({
        sessionID: ctx.sessionID,
        command,
        // Publish the live job count so the TUI footer can show it (worker->main bridge).
        onCount: (count) => bridge.publish(BackgroundJobsEvent, { sessionID: ctx.sessionID, count }).pipe(Effect.asVoid),
        onExit: (reason) =>
          ops
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
            .pipe(Effect.ignore),
      }).pipe(Effect.provideService(ChildProcessSpawner, spawner))

      const info = yield* jobs.start({
        type: TYPE,
        title: params.description,
        metadata: { background: true, sessionId: ctx.sessionID, description: params.description, logPath },
        run: job,
      })

      return {
        title: params.description,
        metadata: { background: true, backgroundId: info.id, description: params.description, logPath },
        output:
          `Background run armed (${info.id}) for "${params.description}". ` +
          `Output streaming to ${logPath}. You will be notified when it exits; ` +
          `read the logfile to check progress, or stop it with bash_background_stop (id: ${info.id}).`,
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
