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
import { BackgroundStopTool } from "../../src/tool/background-stop"
import { BackgroundListTool } from "../../src/tool/background-list"
import { BashBackgroundTool } from "../../src/tool/bash-background"
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

const runBackgroundStop = Effect.gen(function* () {
  const info = yield* BackgroundStopTool
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
      const jobs = yield* BackgroundJob.Service
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
              // B stays running (sleep) so the "exactly one live monitor" assertion
              // below sees B, not a B that already exited.
              yield* monitor.execute({ command: "bash -c 'echo rearmed-ok; sleep 5'", description: "self-cancel" }, ctx)
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

      // Re-arm must REPLACE: monitor A's job is actually cancelled (not orphaned). Wait
      // past A's "sleep 2" so a NOT-cancelled A would have emitted "second" — that line
      // must never appear, and exactly one monitor (B) must remain running. Without this,
      // an orphan-watcher regression (arm B but never cancel A) would pass on no-hang alone.
      const aId = result.metadata.monitorId
      yield* Effect.sleep("2500 millis")
      const aInfo = yield* jobs.get(aId)
      expect(aInfo?.status).toBe("cancelled")
      expect(promptCalls.some((p) => p.text.includes("second"))).toBe(false)
      const liveMonitors = (yield* jobs.list()).filter((j) => j.type === "monitor" && j.status === "running")
      expect(liveMonitors.length).toBe(1)
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

  // Concurrency: monitors are NOT one-per-session. Two DISTINCT descriptions must run
  // SIMULTANEOUSLY (watch a local file AND a remote/SSH log that can't share one tail),
  // so the live count reaches 2. Re-arming the SAME description REPLACES only that watch
  // (dedup), so the count stays at 2 — it must not stack to 3, and must not collapse to 1.
  it.instance("runs distinct-description monitors concurrently (count reaches 2, same-desc re-arm stays 2)", () =>
    Effect.gen(function* () {
      const { chat, assistant } = yield* seed
      const monitor = yield* runMonitor
      const jobs = yield* BackgroundJob.Service
      const bridge = yield* EventV2Bridge.Service

      const counts: number[] = []
      yield* Stream.runForEach(bridge.subscribe(BackgroundJobsEvent), (e) =>
        Effect.sync(() => counts.push((e.data as { count: number }).count)),
      ).pipe(Effect.forkScoped)
      yield* Effect.sleep("100 millis")

      const ops = { prompt: () => Effect.void }
      const ctx = (description: string) => ({
        command: "bash -c 'while true; do sleep 1; done'",
        description,
      })
      const armCtx = {
        sessionID: chat.id,
        messageID: assistant.id,
        agent: "build",
        abort: new AbortController().signal,
        extra: { promptOps: ops },
        messages: [],
        metadata: () => Effect.void,
        ask: () => Effect.void,
      }

      // Two independent sources -> two concurrent monitors.
      yield* monitor.execute(ctx("watch-local"), armCtx)
      yield* monitor.execute(ctx("watch-remote"), armCtx)

      // Count must reach 2 (both live), and exactly 2 monitor jobs are running.
      yield* Effect.gen(function* () {
        while (counts.at(-1) !== 2) yield* Effect.sleep("50 millis")
      }).pipe(Effect.timeout("5 seconds"))
      const live = (yield* jobs.list()).filter(
        (j) => j.type === "monitor" && j.status === "running" && j.metadata?.["sessionId"] === chat.id,
      )
      expect(live.length).toBe(2)
      expect(live.map((j) => j.metadata?.["description"]).sort()).toEqual(["watch-local", "watch-remote"])

      // Re-arm the SAME description: replaces just that watch -> still 2, never 3.
      yield* monitor.execute(ctx("watch-local"), armCtx)
      yield* Effect.sleep("500 millis")
      const after = (yield* jobs.list()).filter(
        (j) => j.type === "monitor" && j.status === "running" && j.metadata?.["sessionId"] === chat.id,
      )
      expect(after.length).toBe(2)
      expect(counts.every((c) => c <= 2)).toBe(true)
    }),
  )

  // background_stop retires a specific watch by description (the leak we hit: a corrected
  // re-arm with a DIFFERENT description leaves the old monitor running; background_stop is
  // the explicit way to retire it). Stop by description must drop only that one; the
  // other monitor keeps running. Also covers stop-by-id.
  it.instance("background_stop retires one monitor by description and by id, leaving others running", () =>
    Effect.gen(function* () {
      const { chat, assistant } = yield* seed
      const monitor = yield* runMonitor
      const backgroundStop = yield* runBackgroundStop
      const jobs = yield* BackgroundJob.Service

      const ops = { prompt: () => Effect.void }
      const armCtx = {
        sessionID: chat.id,
        messageID: assistant.id,
        agent: "build",
        abort: new AbortController().signal,
        extra: { promptOps: ops },
        messages: [],
        metadata: () => Effect.void,
        ask: () => Effect.void,
      }
      const arm = (description: string) =>
        monitor.execute({ command: "bash -c 'while true; do sleep 1; done'", description }, armCtx)

      const a = yield* arm("watch-A")
      yield* arm("watch-B")
      const liveOf = () =>
        Effect.map(jobs.list(), (l) =>
          l.filter((j) => j.type === "monitor" && j.status === "running" && j.metadata?.["sessionId"] === chat.id),
        )
      expect((yield* liveOf()).length).toBe(2)

      // Stop by description -> only watch-B gone.
      const stoppedB = yield* backgroundStop.execute({ description: "watch-B" }, armCtx)
      expect((stoppedB.metadata as { stopped: boolean }).stopped).toBe(true)
      yield* Effect.sleep("300 millis")
      const afterB = yield* liveOf()
      expect(afterB.length).toBe(1)
      expect(afterB[0]!.metadata?.["description"]).toBe("watch-A")

      // Stop the remaining one by id (the id the arm returned) -> none left.
      const aId = (a.metadata as { monitorId: string }).monitorId
      const stoppedA = yield* backgroundStop.execute({ id: aId }, armCtx)
      expect((stoppedA.metadata as { stopped: boolean }).stopped).toBe(true)
      yield* Effect.sleep("300 millis")
      expect((yield* liveOf()).length).toBe(0)

      // Stopping a non-existent id is a no-op, not an error.
      const miss = yield* backgroundStop.execute({ id: "job_doesnotexist" }, armCtx)
      expect((miss.metadata as { stopped: boolean }).stopped).toBe(false)
    }),
  )

  // The exact leak fab hit: "correcting" a watch by RE-WORDING its description (/pv -> ~/pv)
  // does NOT replace the old monitor (dedup is by description) — both run, so a typo'd
  // tail -F (which never exits) lingers. background_stop is the explicit cleanup.
  it.instance("a reworded re-arm does NOT replace the old monitor; background_stop retires the stale one", () =>
    Effect.gen(function* () {
      const { chat, assistant } = yield* seed
      const monitor = yield* runMonitor
      const backgroundStop = yield* runBackgroundStop
      const jobs = yield* BackgroundJob.Service
      const armCtx = {
        sessionID: chat.id,
        messageID: assistant.id,
        agent: "build",
        abort: new AbortController().signal,
        extra: { promptOps: { prompt: () => Effect.void } },
        messages: [],
        metadata: () => Effect.void,
        ask: () => Effect.void,
      }
      const liveOf = () =>
        Effect.map(jobs.list(), (l) =>
          l.filter((j) => j.type === "monitor" && j.status === "running" && j.metadata?.["sessionId"] === chat.id),
        )
      const long = "bash -c 'while true; do sleep 1; done'"

      yield* monitor.execute({ command: long, description: "watch /pv/log" }, armCtx)
      // "Correction" with a DIFFERENT description -> a SECOND monitor, old one stays alive.
      yield* monitor.execute({ command: long, description: "watch ~/pv/log" }, armCtx)
      yield* Effect.sleep("200 millis")
      expect((yield* liveOf()).length).toBe(2)

      // Retire the stale wrong-path watch by its description.
      const stopped = yield* backgroundStop.execute({ description: "watch /pv/log" }, armCtx)
      expect((stopped.metadata as { stopped: boolean }).stopped).toBe(true)
      yield* Effect.sleep("300 millis")
      const after = yield* liveOf()
      expect(after.length).toBe(1)
      expect(after[0]!.metadata?.["description"]).toBe("watch ~/pv/log")
    }),
  )

  // No id and no description must be a safe no-op (NOT an accidental stop-all).
  it.instance("background_stop with neither id nor description is a no-op", () =>
    Effect.gen(function* () {
      const { chat, assistant } = yield* seed
      const monitor = yield* runMonitor
      const backgroundStop = yield* runBackgroundStop
      const jobs = yield* BackgroundJob.Service
      const armCtx = {
        sessionID: chat.id,
        messageID: assistant.id,
        agent: "build",
        abort: new AbortController().signal,
        extra: { promptOps: { prompt: () => Effect.void } },
        messages: [],
        metadata: () => Effect.void,
        ask: () => Effect.void,
      }
      yield* monitor.execute({ command: "bash -c 'while true; do sleep 1; done'", description: "keep-me" }, armCtx)
      const res = yield* backgroundStop.execute({}, armCtx)
      expect((res.metadata as { stopped: boolean; count: number }).stopped).toBe(false)
      expect((res.metadata as { stopped: boolean; count: number }).count).toBe(0)
      expect(res.output).toContain("Provide either")
      yield* Effect.sleep("100 millis")
      const running = (yield* jobs.list()).filter(
        (j) => j.type === "monitor" && j.status === "running" && j.metadata?.["sessionId"] === chat.id,
      )
      expect(running.length).toBe(1)
    }),
  )

  // Description-stop must be SESSION-SCOPED: a same-description monitor in another session
  // is left alone. A regression dropping the sessionId filter would be invisible to every
  // single-session test.
  it.instance("background_stop by description is scoped to the caller's session", () =>
    Effect.gen(function* () {
      const { chat, assistant } = yield* seed
      const sessions = yield* Session.Service
      const chatB = yield* sessions.create({ title: "MonitorTestB" })
      const monitor = yield* runMonitor
      const backgroundStop = yield* runBackgroundStop
      const jobs = yield* BackgroundJob.Service
      const ctxFor = (sid: string) => ({
        sessionID: sid,
        messageID: assistant.id,
        agent: "build",
        abort: new AbortController().signal,
        extra: { promptOps: { prompt: () => Effect.void } },
        messages: [],
        metadata: () => Effect.void,
        ask: () => Effect.void,
      })
      const long = "bash -c 'while true; do sleep 1; done'"

      yield* monitor.execute({ command: long, description: "shared" }, ctxFor(chat.id) as any)
      yield* monitor.execute({ command: long, description: "shared" }, ctxFor(chatB.id) as any)

      // Stop "shared" from session A only.
      yield* backgroundStop.execute({ description: "shared" }, ctxFor(chat.id) as any)
      yield* Effect.sleep("300 millis")
      const running = (yield* jobs.list()).filter((j) => j.type === "monitor" && j.status === "running")
      expect(running.length).toBe(1)
      expect(running[0]!.metadata?.["sessionId"]).toBe(chatB.id)
    }),
  )

  // END-TO-END: the per-line monitor watch loop. Arm `monitor` on the REAL
  // channel.py `stream`, which prints one line per peer message and NEVER exits.
  // `monitor` wakes the agent inline on each stdout line, so TWO peer messages
  // are delivered through ONE persistent monitor with NO re-arm between them —
  // the advantage over bash_background's wake-on-exit (which re-arms per message).
  it.instance("delivers peer messages inline via channel.py stream (continuous, no re-arm)", () =>
    Effect.gen(function* () {
      const { chat, assistant } = yield* seed
      const monitor = yield* runMonitor
      const backgroundStop = yield* runBackgroundStop
      const jobs = yield* BackgroundJob.Service
      const CHANNEL_PY = "/Users/fab/dev/agent-channel/opencode/channel/scripts/channel.py"
      const channel = "monstreamtest_" + Date.now()
      const promptCalls: Array<{ text: string }> = []
      const ops = { prompt: (input: any) => Effect.sync(() => promptCalls.push(input.parts[0])) }
      const ctx = {
        sessionID: chat.id,
        messageID: assistant.id,
        agent: "build",
        abort: new AbortController().signal,
        extra: { promptOps: ops },
        messages: [],
        metadata: () => Effect.void,
        ask: () => Effect.void,
      }

      // Start the watcher's cursor at the current (empty) end, like a real join.
      yield* Effect.promise(() => Bun.spawn(["python3", CHANNEL_PY, "setup", channel, "watcher"]).exited)

      const armed = yield* monitor.execute(
        { command: `python3 ${CHANNEL_PY} stream ${channel} watcher`, description: "watch channel" },
        ctx as any,
      )
      expect(armed.output).toContain("Monitor armed")
      const monId = (armed.metadata as { monitorId: string }).monitorId

      // First peer message -> stream prints one line -> monitor wakes inline.
      yield* Effect.promise(() => Bun.spawn(["python3", CHANNEL_PY, "send", channel, "peer", "ping-one"]).exited)
      yield* Effect.gen(function* () {
        while (!promptCalls.some((p) => p.text.includes("ping-one"))) yield* Effect.sleep("50 millis")
      }).pipe(Effect.timeout("6 seconds"))

      // The SAME monitor is still running — stream never exited, so no re-arm.
      const midLive = (yield* jobs.list()).filter(
        (j) => j.type === "monitor" && j.status === "running" && j.metadata?.["sessionId"] === chat.id,
      )
      expect(midLive.length).toBe(1)
      expect(midLive[0]!.id).toBe(monId)

      // Second peer message -> a SECOND inline wake through that same monitor.
      yield* Effect.promise(() => Bun.spawn(["python3", CHANNEL_PY, "send", channel, "peer", "ping-two"]).exited)
      yield* Effect.gen(function* () {
        while (!promptCalls.some((p) => p.text.includes("ping-two"))) yield* Effect.sleep("50 millis")
      }).pipe(Effect.timeout("6 seconds"))

      // Both arrived as monitor output lines, with no intervening re-arm.
      expect(promptCalls.some((p) => p.text.includes("monitor_output") && p.text.includes("ping-one"))).toBe(true)
      expect(promptCalls.some((p) => p.text.includes("ping-two"))).toBe(true)

      // Tear it down explicitly (stream would also self-exit on a peer 'leave').
      yield* backgroundStop.execute({ id: monId }, ctx as any)
      yield* Effect.sleep("200 millis")
      const after = (yield* jobs.list()).filter(
        (j) => j.type === "monitor" && j.status === "running" && j.metadata?.["sessionId"] === chat.id,
      )
      expect(after.length).toBe(0)
      yield* Effect.promise(() => Bun.spawn(["rm", "-f", `/tmp/claude-channels/${channel}.ndjson`]).exited)
    }),
  )
})

// background_list spans BOTH tool types, so it needs both feature flags. A dedicated layer
// here proves the answer to "will list show monitors AND background bash?" — yes, both.
const bothLayer = Layer.mergeAll(
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
  RuntimeFlags.layer({ experimentalMonitor: true, experimentalBackgroundRun: true }),
).pipe(Layer.provide(Ripgrep.defaultLayer))
const itBoth = testEffect(bothLayer)

describe("background_list", () => {
  itBoth.instance("lists both a monitor and a background run in one session, with correct kinds", () =>
    Effect.gen(function* () {
      const { chat, assistant } = yield* seed
      const monitor = yield* Effect.flatMap(MonitorTool, (i) => i.init())
      const bashbg = yield* Effect.flatMap(BashBackgroundTool, (i) => i.init())
      const list = yield* Effect.flatMap(BackgroundListTool, (i) => i.init())
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

      yield* monitor.execute({ command: "bash -c 'while true; do sleep 1; done'", description: "watch f" }, ctx as any)
      yield* bashbg.execute({ command: "sleep 30", description: "build" }, ctx as any)
      yield* Effect.sleep("150 millis")

      const res = yield* list.execute({}, ctx as any)
      const meta = res.metadata as { count: number; jobs: Array<{ kind: string; description: string }> }
      expect(meta.count).toBe(2)
      expect(meta.jobs.map((j) => j.kind).sort()).toEqual(["background_run", "monitor"])
      expect(res.output).toContain("[monitor]")
      expect(res.output).toContain("[background run]")
    }),
  )

  itBoth.instance("returns an empty list when nothing is running", () =>
    Effect.gen(function* () {
      const { chat, assistant } = yield* seed
      const list = yield* Effect.flatMap(BackgroundListTool, (i) => i.init())
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
      const res = yield* list.execute({}, ctx as any)
      expect((res.metadata as { count: number }).count).toBe(0)
      expect(res.output).toContain("No active")
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
