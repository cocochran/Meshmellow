import SwiftUI
import UniformTypeIdentifiers

final class NoRedirects: NSObject, URLSessionTaskDelegate, @unchecked Sendable {
    func urlSession(_ session: URLSession, task: URLSessionTask, willPerformHTTPRedirection response: HTTPURLResponse, newRequest request: URLRequest, completionHandler: @escaping (URLRequest?) -> Void) { completionHandler(nil) }
}

@MainActor final class MeshStore: ObservableObject {
    @Published var snapshot: MeshSnapshot?
    @Published var endpoint: String
    @Published var error = ""
    @Published var refreshing = false
    @Published var modelName: String
    @Published var modelBytes: Int64
    @Published var modelNote = "Runtime validation pending. No model is loaded by this app."
    @Published var deviceCheckedAt = Date()
    private let defaults: UserDefaults
    private let session: URLSession
    var connected: Bool { error.isEmpty && snapshot?.connected == true }

    init(defaults: UserDefaults = .standard) {
        self.defaults = defaults
        endpoint = defaults.string(forKey: "dashboard") ?? ""
        modelName = defaults.string(forKey: "modelName") ?? ""
        modelBytes = defaults.object(forKey: "modelBytes") as? Int64 ?? 0
        let config = URLSessionConfiguration.ephemeral
        config.timeoutIntervalForRequest = 6
        config.timeoutIntervalForResource = 10
        config.httpShouldSetCookies = false
        session = URLSession(configuration: config, delegate: NoRedirects(), delegateQueue: nil)
    }
    func saveEndpoint(_ value: String) throws {
        let trimmed = value.trimmingCharacters(in: .whitespacesAndNewlines)
        if !trimmed.isEmpty { _ = try MeshValidation.dashboardURL(trimmed) }
        endpoint = trimmed
        defaults.set(trimmed, forKey: "dashboard")
        snapshot = nil
        error = ""
    }
    func refresh() async {
        guard !endpoint.isEmpty, !refreshing else { return }
        let requestedEndpoint = endpoint
        refreshing = true
        defer { refreshing = false }
        do {
            let url = try MeshValidation.dashboardURL(requestedEndpoint)
            let (bytes, response) = try await session.bytes(from: url)
            guard (response as? HTTPURLResponse)?.statusCode == 200 else { throw AppFailure.message("The dashboard did not return mesh state.") }
            var data = Data()
            for try await byte in bytes {
                try Task.checkCancellation()
                guard data.count < 1_048_576 else { throw AppFailure.message("Dashboard response is too large.") }
                data.append(byte)
            }
            let next = try JSONDecoder().decode(MeshSnapshot.self, from: data)
            guard !Task.isCancelled, requestedEndpoint == endpoint else { return }
            snapshot = next
            error = ""
        } catch {
            if !Task.isCancelled, requestedEndpoint == endpoint { self.error = "Cannot read the dashboard. Check the address and connection." }
        }
    }
    func selectModel(_ url: URL) throws {
        guard modelName.isEmpty else { throw AppFailure.message("Remove the current file selection first. One model at a time.") }
        let access = url.startAccessingSecurityScopedResource()
        defer { if access { url.stopAccessingSecurityScopedResource() } }
        let file = try FileHandle(forReadingFrom: url)
        defer { try? file.close() }
        let version = try MeshValidation.ggufVersion(file.read(upToCount: 24) ?? Data())
        let values = try url.resourceValues(forKeys: [.fileSizeKey, .isRegularFileKey])
        guard values.isRegularFile == true else { throw AppFailure.message("Choose a regular GGUF file.") }
        let bookmark = try url.bookmarkData(options: .minimalBookmark, includingResourceValuesForKeys: nil, relativeTo: nil)
        defaults.set(bookmark, forKey: "modelBookmark")
        defaults.set(url.lastPathComponent, forKey: "modelName")
        defaults.set(Int64(values.fileSize ?? 0), forKey: "modelBytes")
        modelName = url.lastPathComponent
        modelBytes = Int64(values.fileSize ?? 0)
        modelNote = "GGUF v\(version) header recognized. Full integrity and runtime support are not verified."
    }
    func verifySelectedFile() {
        do {
            guard let bookmark = defaults.data(forKey: "modelBookmark") else { throw AppFailure.message("Select the file again.") }
            var stale = false
            let url = try URL(resolvingBookmarkData: bookmark, options: [], relativeTo: nil, bookmarkDataIsStale: &stale)
            let access = url.startAccessingSecurityScopedResource()
            defer { if access { url.stopAccessingSecurityScopedResource() } }
            let file = try FileHandle(forReadingFrom: url)
            defer { try? file.close() }
            let version = try MeshValidation.ggufVersion(file.read(upToCount: 24) ?? Data())
            if stale { defaults.set(try url.bookmarkData(options: .minimalBookmark, includingResourceValuesForKeys: nil, relativeTo: nil), forKey: "modelBookmark") }
            modelNote = "File accessible · GGUF v\(version). Runtime validation pending."
        } catch { modelNote = "File unavailable or invalid. Remove the selection and choose it again." }
    }
    func removeModel() {
        for key in ["modelBookmark", "modelName", "modelBytes"] { defaults.removeObject(forKey: key) }
        modelName = ""
        modelBytes = 0
        modelNote = "Runtime validation pending. No model is loaded by this app."
    }
}
