import SwiftUI
import UIKit

@main
struct MeshThingWorkerApp: App {
    @State private var status = "Starting foreground worker…"
    @State private var started = false
    private let leader = "7c8c4f91e3b1798c4c64a3615573700a9785a01d3036cb913bff827cb6ea6182"

    var body: some Scene {
        WindowGroup {
            ScrollView {
                VStack(alignment: .leading, spacing: 16) {
                    Text("MeshThing iPad worker").font(.title)
                    Text("Qwen3.8-27B · layers 28–31 · Metal + Iroh")
                    Text("Keep this app open and the iPad powered. This diagnostic does not promise background availability.")
                    Text(status).font(.system(.body, design: .monospaced)).textSelection(.enabled)
                }.padding()
            }.task {
                guard !started else { return }
                started = true
                await startWorker()
            }
        }
    }

    @MainActor private func startWorker() async {
        guard let model = Bundle.main.path(forResource: "Qwen3.8-27B-middle-28-32", ofType: "gguf"),
              let promptURL = Bundle.main.url(forResource: "qwen38-prompt", withExtension: "txt"),
              let prompt = try? String(contentsOf: promptURL, encoding: .utf8) else {
            status = "Missing model shard or prompt"
            return
        }
        UIApplication.shared.isIdleTimerDisabled = true
        let documents = FileManager.default.urls(for: .documentDirectory, in: .userDomainMask)[0]
        let key = documents.appendingPathComponent("iroh.key").path
        let identity = documents.appendingPathComponent("node-id.txt").path
        let approved = leader
        let downstream = "0555d4c7eb356daa7004b1f07ac94186a752b4f84d9e5282ab0dd75ebabde0df"
        DispatchQueue.global(qos: .userInitiated).async {
            let code = key.withCString { keyPath in
                approved.withCString { peer in
                    approved.withCString { leaderPeer in
                        downstream.withCString { nextPeer in
                            identity.withCString { statusPath in meshthing_iroh_run(keyPath, peer, leaderPeer, nextPeer, statusPath, 50171) }
                        }
                    }
                }
            }
            print("IROH_WORKER_EXIT: \(code)")
            DispatchQueue.main.async { status = "Iroh stopped (\(code)); inspect device log" }
        }
        DispatchQueue.global(qos: .userInitiated).async {
            while true {
                let code = model.withCString { modelPath in
                    prompt.withCString { text in meshthing_native_worker(modelPath, text, 28, 32, 512) }
                }
                print("NATIVE_WORKER_SESSION_EXIT: \(code)")
                if code != 0 {
                    DispatchQueue.main.async { status = "Session ended (\(code)); waiting for a fresh request" }
                    Thread.sleep(forTimeInterval: 1)
                }
            }
        }
        for _ in 0..<120 {
            if let id = try? String(contentsOfFile: identity, encoding: .utf8), !id.isEmpty {
                status = "Iroh identity:\n\(id)\nOnly the test Mac is approved. Waiting for a session."
                return
            }
            try? await Task.sleep(nanoseconds: 1_000_000_000)
        }
        status = "Identity not ready; inspect device log"
    }
}
