import { afterEach, describe, expect, test } from "bun:test"
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
import { MonitorTool } from "../../src/tool/monitor"
import { testEffect } from "../lib/effect"
import { MessageID, SessionID } from "../../src/session/schema"
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
  RuntimeFlags.layer({ experimentalMonitor: true }),
).pipe(Layer.provide(Ripgrep.defaultLayer))

const it = testEffect(layer)

const ref = {
  providerID: "test" as any,
  modelID: "test-model" as any,
}

const seed = Effect.gen(function* () {
  const session = yield* Session.Service
  const chat = yield* session.create({ title: "MonitorTest" })
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

const runMonitor = Effect.gen(function* () {
  const info = yield* MonitorTool
  const tool = yield* info.init()
  return tool
})

describe("MonitorTool", () => {
  it.instance("arms a monitor and returns immediately", () =>
    Effect.gen(function* () {
      const { chat, assistant } = yield* seed
      const monitor = yield* runMonitor
      const promptCalls: Array<{ text: string }> = []

      const ops = {
        prompt: (input: any) =>
          Effect.sync(() => {
            promptCalls.push(input.parts[0])
          }),
      }

      const result = yield* monitor.execute(
        {
          command: "echo 'hello'",
          description: "test monitor",
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

      expect(result.output).toContain("Monitor armed")
      expect(result.metadata.monitor).toBe(true)
      expect(result.metadata.description).toBe("test monitor")

      // Wait briefly for the monitor to exit and trigger callbacks
      yield* Effect.sleep("500 millis")

      // Output batch + exit note both arrive (order is not guaranteed: the final
      // batch flush is detached, so assert presence, not position).
      expect(promptCalls.length).toBeGreaterThan(0)
      expect(promptCalls.some((p) => p.text.includes("hello"))).toBe(true)
      expect(promptCalls.some((p) => p.text.includes("Monitor exited"))).toBe(true)
    }),
  )

  // Regression for the re-arm deadlock. The trigger is SELF-CANCEL: the model is
  // woken by monitor A's output and, from inside that wake turn, re-arms the
  // monitor — which cancels A's job. With forkIn(jobScope), A's emit fiber (the
  // fiber currently running this very ops.prompt) is in the scope cancel() closes,
  // so cancel -> Scope.close interrupts+awaits the fiber it is running on. If that
  // self-cancel deadlocks (the original 40-min hang) or silently kills the re-arm,
  // monitor B never arms and "rearmed" never appears. We give the whole flow a hard
  // timeout so a hang FAILS fast instead of wedging the suite.
  it.instance("re-arming from inside a wake turn does not deadlock (self-cancel)", () =>
    Effect.gen(function* () {
      const { chat, assistant } = yield* seed
      const monitor = yield* runMonitor
      const promptCalls: Array<{ text: string }> = []
      let rearmed = false

      const ctx: any = {
        sessionID: chat.id,
        messageID: assistant.id,
        agent: "build",
        abort: new AbortController().signal,
        extra: { promptOps: undefined },
        messages: [],
        metadata: () => Effect.void,
        ask: () => Effect.void,
      }

      // On the first real output wake, re-arm from within the wake effect itself.
      // This effect runs in A's emit fiber (forkIn jobScope), exactly the fiber
      // cancel()'s Scope.close will interrupt — the worst-case self-cancel.
      const ops = {
        prompt: (input: any) =>
          Effect.gen(function* () {
            promptCalls.push(input.parts[0])
            if (!rearmed && input.parts[0].text.includes("monitor_output")) {
              rearmed = true
              yield* monitor.execute({ command: "echo 'rearmed-ok'", description: "self-cancel" }, ctx)
            }
          }),
      }
      ctx.extra.promptOps = ops

      const result = yield* monitor.execute(
        { command: "bash -c 'echo first; sleep 2; echo second'", description: "self-cancel" },
        ctx,
      )
      expect(result.output).toContain("Monitor armed")

      // Wait for: A emits -> wake re-arms B -> B emits "rearmed-ok". If the
      // self-cancel deadlocks or aborts the re-arm, B never emits and this assert
      // is never satisfied; the outer timeout converts the hang into a failure.
      yield* Effect.gen(function* () {
        while (!promptCalls.some((p) => p.text.includes("rearmed-ok"))) {
          yield* Effect.sleep("100 millis")
        }
      }).pipe(
        // On a self-cancel deadlock B never emits rearmed-ok, so this loop never
        // settles; the timeout converts the hang into a TimeoutException -> test fails.
        Effect.timeout("8 seconds"),
      )

      expect(rearmed).toBe(true)
      expect(promptCalls.some((p) => p.text.includes("rearmed-ok"))).toBe(true)
    }),
  )

  // F3: the EXIT-then-rearm variant. The 'Monitor exited' note explicitly invites a
  // re-arm; if the model does so, it cancels the just-exited job from inside the very
  // fiber delivering that note. This is a DIFFERENT path from the per-batch self-cancel
  // (the exit note is emitted by the tool's exit hook, not runShellJob's forked wake),
  // so it needs its own coverage. Same hard timeout: a hang fails fast.
  it.instance("re-arming on the 'Monitor exited' note does not deadlock (exit-then-rearm)", () =>
    Effect.gen(function* () {
      const { chat, assistant } = yield* seed
      const monitor = yield* runMonitor
      const promptCalls: Array<{ text: string }> = []
      let rearmed = false

      const ctx: any = {
        sessionID: chat.id,
        messageID: assistant.id,
        agent: "build",
        abort: new AbortController().signal,
        extra: { promptOps: undefined },
        messages: [],
        metadata: () => Effect.void,
        ask: () => Effect.void,
      }

      const ops = {
        prompt: (input: any) =>
          Effect.gen(function* () {
            promptCalls.push(input.parts[0])
            if (!rearmed && input.parts[0].text.includes("Monitor exited")) {
              rearmed = true
              yield* monitor.execute({ command: "echo 'rearmed-ok'", description: "exit-rearm" }, ctx)
            }
          }),
      }
      ctx.extra.promptOps = ops

      // A command that EXITS on its own -> fires the 'Monitor exited' note.
      const result = yield* monitor.execute(
        { command: "bash -c 'echo first'", description: "exit-rearm" },
        ctx,
      )
      expect(result.output).toContain("Monitor armed")

      yield* Effect.gen(function* () {
        while (!promptCalls.some((p) => p.text.includes("rearmed-ok"))) {
          yield* Effect.sleep("100 millis")
        }
      }).pipe(Effect.timeout("8 seconds"))

      expect(rearmed).toBe(true)
      expect(promptCalls.some((p) => p.text.includes("rearmed-ok"))).toBe(true)
    }),
  )

  // Byte-flood guard: the line-based flood cap counts COMPLETE lines, so a watcher
  // spewing bytes with no newline would grow the carry buffer unbounded and never trip
  // it. The byte cap must catch that — emit a flood warning and stop.
  it.instance("stops a watcher that emits bytes with no newline (byte flood)", () =>
    Effect.gen(function* () {
      const { chat, assistant } = yield* seed
      const monitor = yield* runMonitor
      const promptCalls: Array<{ text: string }> = []

      const ops = {
        prompt: (input: any) =>
          Effect.sync(() => {
            promptCalls.push(input.parts[0])
          }),
      }

      // ~2MB of 'x' with NO newline -> carry exceeds CARRY_MAX_BYTES (1MB).
      const result = yield* monitor.execute(
        { command: "bash -c 'head -c 2000000 /dev/zero | tr \"\\0\" \"x\"'", description: "byte flood" },
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
      expect(result.output).toContain("Monitor armed")

      // Wait for the byte-flood guard to fire its warning.
      yield* Effect.gen(function* () {
        while (!promptCalls.some((p) => p.text.includes("no newline"))) {
          yield* Effect.sleep("100 millis")
        }
      }).pipe(Effect.timeout("8 seconds"))

      expect(promptCalls.some((p) => p.text.includes("flood guard") && p.text.includes("no newline"))).toBe(true)
    }),
  )

  // The footer pill: arming a monitor must publish a session.background-jobs event
  // carrying the live count, so the MAIN-thread TUI footer (which can't read the
  // worker-thread count shim) can render it. This asserts the publish side.
  it.instance("publishes a session.background-jobs count event on arm", () =>
    Effect.gen(function* () {
      const { chat, assistant } = yield* seed
      const monitor = yield* runMonitor
      const bridge = yield* EventV2Bridge.Service

      const counts: number[] = []
      yield* Stream.runForEach(bridge.subscribe(BackgroundJobsEvent), (e) =>
        Effect.sync(() => counts.push((e.data as { count: number }).count)),
      ).pipe(Effect.forkScoped)
      yield* Effect.sleep("100 millis") // let the subscription attach before we publish

      const ops = { prompt: () => Effect.void }
      yield* monitor.execute(
        { command: "bash -c 'while true; do sleep 1; done'", description: "count-test" },
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

      yield* Effect.sleep("500 millis")
      // A live monitor must have published count >= 1 for this session.
      expect(counts.some((c) => c >= 1)).toBe(true)
    }),
  )

  // Guards the stop-finalizer publish: when a monitor exits, count must return to 0 so
  // the footer pill clears (the "pill won't clear" failure mode). A short command that
  // exits immediately drives arm(1) -> exit -> finalizer(0).
  it.instance("publishes count 0 when a monitor exits (clears the pill)", () =>
    Effect.gen(function* () {
      const { chat, assistant } = yield* seed
      const monitor = yield* runMonitor
      const bridge = yield* EventV2Bridge.Service

      const counts: number[] = []
      yield* Stream.runForEach(bridge.subscribe(BackgroundJobsEvent), (e) =>
        Effect.sync(() => counts.push((e.data as { count: number }).count)),
      ).pipe(Effect.forkScoped)
      yield* Effect.sleep("100 millis")

      const ops = { prompt: () => Effect.void }
      yield* monitor.execute(
        { command: "bash -c 'echo hi'", description: "exit-count" },
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

      yield* Effect.gen(function* () {
        while (counts.at(-1) !== 0) yield* Effect.sleep("50 millis")
      }).pipe(Effect.timeout("5 seconds"))

      expect(counts).toContain(1)
      expect(counts.at(-1)).toBe(0)
    }),
  )
})

if (process.env.OPENCODE_LIVE_MONITOR_TEST) {
  describe("MonitorTool live", () => {
    it.instance("watches a live command and emits events", () =>
      Effect.gen(function* () {
        const { chat, assistant } = yield* seed
        const monitor = yield* runMonitor
        const promptCalls: Array<{ text: string }> = []

        const ops = {
          prompt: (input: any) =>
            Effect.sync(() => {
              promptCalls.push(input.parts[0])
            }),
        }

        const result = yield* monitor.execute(
          {
            command: "bash -c 'echo event1; sleep 0.2; echo event2; sleep 0.2'",
            description: "live test",
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

        expect(result.output).toContain("Monitor armed")

        yield* Effect.sleep("1 second")

        const events = promptCalls.filter((p) => p.text.includes("monitor_output"))
        expect(events.length).toBeGreaterThanOrEqual(1)
      }),
    )

    it.instance("watches file ./f for changes and emits events", () =>
      Effect.gen(function* () {
        const { chat, assistant } = yield* seed
        const monitor = yield* runMonitor
        const promptCalls: Array<{ text: string }> = []

        const ops = {
          prompt: (input: any) =>
            Effect.sync(() => {
              promptCalls.push(input.parts[0])
            }),
        }

        // Pre-create the file so monitor finds it immediately
        // Note: must include newline so monitor's line-splitter picks it up
        const watchFile = `${chat.directory}/f`
        yield* Effect.promise(() => Bun.write(watchFile, "change1\n"))

        // Monitor: first print cwd to verify directory, then poll for file
        const result = yield* monitor.execute(
          {
            command: "bash -c 'while true; do if [ -f ./f ]; then cat ./f; rm ./f; fi; sleep 0.1; done'",
            description: "watch file ./f for changes",
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

        expect(result.output).toContain("Monitor armed")
        expect(result.metadata.description).toBe("watch file ./f for changes")

        // Wait for monitor to detect pre-existing file
        yield* Effect.sleep("500 millis")

        // Trigger second change
        yield* Effect.promise(() => Bun.write(watchFile, "change2\n"))
        yield* Effect.sleep("500 millis")

        // Trigger third change
        yield* Effect.promise(() => Bun.write(watchFile, "change3\n"))
        yield* Effect.sleep("500 millis")

        // Collect events
        const events = promptCalls.filter((p) => p.text.includes("monitor_output"))
        expect(events.length).toBeGreaterThanOrEqual(1)

        // Verify event contents
        const texts = events.map((e) => e.text)
        expect(texts.some((t) => t.includes("change1"))).toBe(true)
        expect(texts.some((t) => t.includes("change2"))).toBe(true)
        expect(texts.some((t) => t.includes("change3"))).toBe(true)
      }),
    )

    it.instance("E2E: prompts for watch and then changes file", () =>
      Effect.gen(function* () {
        const { chat, assistant } = yield* seed
        const monitor = yield* runMonitor
        const promptCalls: Array<{ text: string }> = []

        const ops = {
          prompt: (input: any) =>
            Effect.sync(() => {
              promptCalls.push(input.parts[0])
            }),
        }

        const watchFile = `${chat.directory}/watchme.txt`

        // Create initial file
        yield* Effect.promise(() => Bun.write(watchFile, "initial\n"))

        // Simulate user prompt: "watch watchme.txt for changes"
        // Use a polling loop that detects file changes and reads them
        const result = yield* monitor.execute(
          {
            command: "bash -c 'while true; do if [ -f ./watchme.txt ]; then cat ./watchme.txt; rm ./watchme.txt; fi; sleep 0.1; done'",
            description: "watch watchme.txt for changes",
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

        expect(result.output).toContain("Monitor armed")
        expect(result.metadata.description).toBe("watch watchme.txt for changes")

        // Give monitor time to detect initial file
        yield* Effect.sleep("500 millis")

        // Simulate file change: user writes a line
        yield* Effect.promise(() => Bun.write(watchFile, "first line\n"))
        yield* Effect.sleep("500 millis")

        // Simulate another file change
        yield* Effect.promise(() => Bun.write(watchFile, "second line\n"))
        yield* Effect.sleep("500 millis")

        // Collect events
        const events = promptCalls.filter((p) => p.text.includes("monitor_output"))
        expect(events.length).toBeGreaterThanOrEqual(1)

        // Verify event contents
        const texts = events.map((e) => e.text)
        expect(texts.some((t) => t.includes("first line"))).toBe(true)
        expect(texts.some((t) => t.includes("second line"))).toBe(true)
      }),
    )
  })
}
