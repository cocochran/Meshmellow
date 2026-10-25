import SwiftUI

final class NoRedirects: NSObject, URLSessionTaskDelegate, @unchecked Sendable {
    func urlSession(_ session: URLSession, task: URLSessionTask, willPerformHTTPRedirection response: HTTPURLResponse, newRequest request: URLRequest, completionHandler: @escaping (URLRequest?) -> Void) { completionHandler(nil) }
}

@MainActor final class MeshStore: ObservableObject {
    @Published var snapshot: MeshSnapshot?
    @Published var endpoint: String
    @Published var error = ""
    @Published var refreshing = false
    @Published var deviceCheckedAt = Date()
    private let defaults: UserDefaults
    private let session: URLSession
    var connected: Bool { error.isEmpty && snapshot?.connected == true }

    init(defaults: UserDefaults = .standard) {
        self.defaults = defaults
        endpoint = defaults.string(forKey: "dashboard") ?? ""
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
}
