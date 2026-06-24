import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { BackgroundMonitorManager as CoreBackgroundMonitorManager } from "@opencode-ai/core/background-monitor"
import { InstanceState } from "@/effect/instance-state"
import { Effect, Layer } from "effect"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { ChildProcessSpawner } from "effect/unstable/process/ChildProcessSpawner"

export {
  Service,
  type Info,
  type Interface,
  type StartInput,
  type Status,
} from "@opencode-ai/core/background-monitor"

export const layer = Layer.effect(
  CoreBackgroundMonitorManager.Service,
  Effect.gen(function* () {
    const spawner = yield* ChildProcessSpawner
    const state = yield* InstanceState.make(() =>
      CoreBackgroundMonitorManager.make.pipe(Effect.provideService(ChildProcessSpawner, spawner)),
    )
    return CoreBackgroundMonitorManager.Service.of({
      list: () => InstanceState.useEffect(state, (m) => m.list()),
      get: (id) => InstanceState.useEffect(state, (m) => m.get(id)),
      start: (input) =>
        InstanceState.useEffect(state, (m) =>
          m.start(input).pipe(Effect.provideService(ChildProcessSpawner, spawner)),
        ),
      stop: (id) => InstanceState.useEffect(state, (m) => m.stop(id)),
      stopAllForSession: (sessionID) => InstanceState.useEffect(state, (m) => m.stopAllForSession(sessionID)),
      stopAllForSessionByKind: (sessionID, kind) =>
        InstanceState.useEffect(state, (m) => m.stopAllForSessionByKind(sessionID, kind)),
      stopAll: () => InstanceState.useEffect(state, (m) => m.stopAll()),
      countForSession: (sessionID) => InstanceState.useEffect(state, (m) => m.countForSession(sessionID)),
      listForSession: (sessionID) => InstanceState.useEffect(state, (m) => m.listForSession(sessionID)),
      whenIdleForSession: (sessionID, signal) =>
        InstanceState.useEffect(state, (m) => m.whenIdleForSession(sessionID, signal)),
    })
  }),
)

export const defaultLayer = layer

export const node = LayerNode.make(layer, [CrossSpawnSpawner.node])

export * as BackgroundMonitorManager from "./monitor"
