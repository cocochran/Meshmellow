import Foundation
import SwiftUI

private final class EnrollmentBridge: @unchecked Sendable {
    private let queue = DispatchQueue(label: "org.meshmellow.enrollment")
    func command(_ value: [String: Any]) async throws -> Data {
        let input = String(decoding: try JSONSerialization.data(withJSONObject: value), as: UTF8.self)
        return try await withCheckedThrowingContinuation { continuation in
            queue.async {
                let result = input.withCString { mesh_enrollment_command($0) }
                guard let result else { continuation.resume(throwing: AppFailure.message("Enrollment unavailable.")); return }
                let data = Data(String(cString: result).utf8)
                mesh_enrollment_free(result)
                continuation.resume(returning: data)
            }
        }
    }
}

@MainActor final class EnrollmentStore: ObservableObject {
    @Published var state: EnrollmentState?
    @Published var error = ""
    @Published var busy = false
    @Published var discoveryError = ""
    private let bridge = EnrollmentBridge()
    private var active = false


    private func call(_ command: [String: Any], update: Bool = true) async throws {
        struct Response: Decodable { let ok: Bool; let error: String?; let state: EnrollmentState? }
        let response = try JSONDecoder().decode(Response.self, from: await bridge.command(command))
        guard response.ok else { throw AppFailure.message(response.error ?? "Enrollment failed.") }
        if update, active, let state = response.state {
            self.state = state
        }
    }

    func start() async {
        active = true
        do {
            let directory = try FileManager.default.url(for: .applicationSupportDirectory, in: .userDomainMask, appropriateFor: nil, create: true).appendingPathComponent("Enrollment", isDirectory: true)
            try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true, attributes: [.protectionKey: FileProtectionType.completeUntilFirstUserAuthentication])
            var protected = directory
            var resource = URLResourceValues(); resource.isExcludedFromBackup = true
            try protected.setResourceValues(resource)
            try await call(["op": "open", "directory": directory.path])
            guard active else { return }
            error = ""
        } catch { self.error = error.localizedDescription }
    }

    func stop() async {
        active = false
        _ = try? await bridge.command(["op": "close"])
    }

    func refresh() async {
        guard active, !busy, state != nil else { return }
        do {
            try await call(["op": "refresh"])
        } catch { self.error = error.localizedDescription }
    }

    func perform(_ command: [String: Any]) async {
        guard active, !busy else { return }
        busy = true
        defer { busy = false }
        do {
            try await call(command)
            error = ""
        } catch { self.error = error.localizedDescription }
    }
}
