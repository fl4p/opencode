import { afterEach, describe, expect } from "bun:test"
import { Database } from "@opencode-ai/core/database/database"
import { Effect, Layer, Stream } from "effect"
import { BackgroundJobsEvent } from "../../src/tool/background-shell"
import { Agent } from "@/agent/agent"
import { BackgroundJob } from "@/background/job"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { EventV2Bridge } from "@/event-v2-bridge"
import { Config } from "@/config/config"
import { Ripgrep } from "@opencode-ai/core/ripgrep"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { Session } from "@/session/session"
import { SessionRunState } from "@/session/run-state"
import { SessionStatus } from "@/session/status"
import { ToolRegistry } from "@/tool/registry"
import { Truncate } from "@/tool/truncate"
import { BashBackgroundTool } from "../../src/tool/bash-background"
import { BackgroundStopTool } from "../../src/tool/background-stop"
import { testEffect } from "../lib/effect"
import { MessageID } from "../../src/session/schema"
import { disposeAllInstances } from "../fixture/fixture"

afterEach(async () => {
  await disposeAllInstances()
})

const layer = Layer.mergeAll(
  Agent.defaultLayer,
  BackgroundJob.defaultLayer,
  EventV2Bridge.defaultLayer,
  Config.defaultLayer,
  CrossSpawnSpawner.defaultLayer,
  Session.defaultLayer,
  SessionRunState.defaultLayer,
  SessionStatus.defaultLayer,
  Truncate.defaultLayer,
  ToolRegistry.defaultLayer,
  Database.defaultLayer,
  RuntimeFlags.layer({ experimentalBackgroundRun: true }),
).pipe(Layer.provide(Ripgrep.defaultLayer))

const it = testEffect(layer)

const ref = {
  providerID: "test" as any,
  modelID: "test-model" as any,
}

const seed = Effect.gen(function* () {
  const session = yield* Session.Service
  const chat = yield* session.create({ title: "BashBackgroundTest" })
  const user = yield* session.updateMessage({
    id: MessageID.ascending(),
    role: "user",
    sessionID: chat.id,
    agent: "build",
    model: ref,
    time: { created: Date.now() },
  })
  const assistant = {
    id: MessageID.ascending(),
    role: "assistant" as const,
    parentID: user.id,
    sessionID: chat.id,
    mode: "build",
    agent: "build",
    cost: 0,
    path: { cwd: chat.directory, root: chat.directory },
    tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
    modelID: ref.modelID,
    providerID: ref.providerID,
    time: { created: Date.now() },
  }
  yield* session.updateMessage(assistant)
  return { chat, assistant }
})

const runTool = Effect.gen(function* () {
  const info = yield* BashBackgroundTool
  const tool = yield* info.init()
  return tool
})

describe("BashBackgroundTool", () => {
  it.instance("arms a background run, returns immediately, notifies once on exit", () =>
    Effect.gen(function* () {
      const { chat, assistant } = yield* seed
      const tool = yield* runTool
      const promptCalls: Array<{ text: string }> = []

      const ops = {
        prompt: (input: any) =>
          Effect.sync(() => {
            promptCalls.push(input.parts[0])
          }),
      }

      const result = yield* tool.execute(
        {
          command: "echo background-hi",
          description: "echo test",
        },
        {
          sessionID: chat.id,
          messageID: assistant.id,
          agent: "build",
          abort: new AbortController().signal,
          extra: { promptOps: ops },
          messages: [],
          metadata: () => Effect.void,
          ask: () => Effect.void,
        },
      )

      expect(result.output).toContain("Background run armed")
      expect(result.metadata.background).toBe(true)
      expect(typeof result.metadata.logPath).toBe("string")

      // Wait for the command to exit and fire the single onExit notification.
      yield* Effect.sleep("500 millis")

      expect(promptCalls.length).toBe(1)
      expect(promptCalls[0].text).toContain("exited")
      expect(promptCalls[0].text).toContain("Output in")

      // Combined output was captured to the logfile.
      const logged = yield* Effect.promise(() => Bun.file(result.metadata.logPath as string).text())
      expect(logged).toContain("background-hi")
    }),
  )

  it.instance("background_stop tree-kills a running background run", () =>
    Effect.gen(function* () {
      const { chat, assistant } = yield* seed
      const tool = yield* runTool
      const stopInfo = yield* BackgroundStopTool
      const stopTool = yield* stopInfo.init()

      const ctx = {
        sessionID: chat.id,
        messageID: assistant.id,
        agent: "build",
        abort: new AbortController().signal,
        extra: { promptOps: { prompt: () => Effect.void } },
        messages: [],
        metadata: () => Effect.void,
        ask: () => Effect.void,
      }

      const armed = yield* tool.execute({ command: "sleep 30", description: "long sleep" }, ctx as any)
      const backgroundId = armed.metadata.backgroundId as string
      yield* Effect.sleep("100 millis")

      // Stop finds the run (proves it was tracked) and tree-kills it.
      const stopped = yield* stopTool.execute({ id: backgroundId }, ctx as any)
      expect(stopped.metadata.stopped).toBe(true)
      expect(stopped.output).toContain("Stopped background run")

      // It is now gone — stopping again is a no-op.
      yield* Effect.sleep("100 millis")
      const again = yield* stopTool.execute({ id: backgroundId }, ctx as any)
      expect(again.metadata.stopped).toBe(false)

      // Stopping an unknown id is a no-op with stopped:false.
      const missing = yield* stopTool.execute({ id: "monitor-999" }, ctx as any)
      expect(missing.metadata.stopped).toBe(false)
    }),
  )

  // The pill's whole point is an accurate AGGREGATE. bash_background (unlike monitor)
  // does not cancel peers, so two concurrent runs must drive the count to 2, and
  // stopping one must drop it to 1 — guarding getMonitorCount inc/dec symmetry across
  // overlapping runShellJob finalizers.
  it.instance("aggregates the running-job count across concurrent runs", () =>
    Effect.gen(function* () {
      const { chat, assistant } = yield* seed
      const tool = yield* runTool
      const stopInfo = yield* BackgroundStopTool
      const stopTool = yield* stopInfo.init()
      const bridge = yield* EventV2Bridge.Service

      const counts: number[] = []
      yield* Stream.runForEach(bridge.subscribe(BackgroundJobsEvent), (e) =>
        Effect.sync(() => counts.push((e.data as { count: number }).count)),
      ).pipe(Effect.forkScoped)
      yield* Effect.sleep("100 millis")

      const ctx = {
        sessionID: chat.id,
        messageID: assistant.id,
        agent: "build",
        abort: new AbortController().signal,
        extra: { promptOps: { prompt: () => Effect.void } },
        messages: [],
        metadata: () => Effect.void,
        ask: () => Effect.void,
      }

      const a = yield* tool.execute({ command: "sleep 30", description: "a" }, ctx as any)
      yield* tool.execute({ command: "sleep 30", description: "b" }, ctx as any)

      yield* Effect.gen(function* () {
        while (counts.at(-1) !== 2) yield* Effect.sleep("50 millis")
      }).pipe(Effect.timeout("5 seconds"))
      expect(counts).toContain(2)

      yield* stopTool.execute({ id: a.metadata.backgroundId as string }, ctx as any)
      yield* Effect.gen(function* () {
        while (counts.at(-1) !== 1) yield* Effect.sleep("50 millis")
      }).pipe(Effect.timeout("5 seconds"))
      expect(counts.at(-1)).toBe(1)
    }),
  )
})
