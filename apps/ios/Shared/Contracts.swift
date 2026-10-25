import Foundation

struct MeshSnapshot: Decodable {
    let connected: Bool
    let source: String
    let checkedAt: String
    let message: String?
    let active: Bool
    let event: String
    let model: String
    let devices: [MeshDevice]
    let layers: Int?
    let decodeRate: Double?
    let outputSteps: Int?
}
struct MeshDevice: Decodable, Identifiable {
    let id: String
    let name: String
    let hardware: String
    let begin: Int?
    let end: Int?
    let status: String
    let executions: Int
    let computeMs: Double?
    let bufferBytes: Double?
    var range: String {
        guard let begin, let end, begin >= 0, end > begin else { return "Unreported" }
        return "\(begin)–\(end - 1)"
    }
}
enum MeshValidation {
    static func dashboardURL(_ value: String) throws -> URL {
        guard let parts = URLComponents(string: value.trimmingCharacters(in: .whitespacesAndNewlines)),
              let host = parts.host, !host.isEmpty,
              parts.user == nil, parts.password == nil, parts.query == nil, parts.fragment == nil,
              parts.path.isEmpty || parts.path == "/",
              parts.scheme == "https" || (parts.scheme == "http" && ["localhost", "127.0.0.1", "::1", "[::1]"].contains(host)),
              let url = parts.url else { throw AppFailure.message("Use an HTTPS dashboard address, or localhost for the simulator.") }
        return url.appendingPathComponent("api/mesh")
    }
    static func ggufVersion(_ data: Data) throws -> UInt32 {
        guard data.count >= 24, Array(data.prefix(4)) == [0x47, 0x47, 0x55, 0x46] else { throw AppFailure.message("This file does not have a complete GGUF header.") }
        let bytes = Array(data)
        let version = UInt32(bytes[4]) | UInt32(bytes[5]) << 8 | UInt32(bytes[6]) << 16 | UInt32(bytes[7]) << 24
        guard version == 2 || version == 3 else { throw AppFailure.message("Only GGUF versions 2 and 3 are recognized.") }
        return version
    }
}
enum AppFailure: LocalizedError {
    case message(String)
    var errorDescription: String? { switch self { case .message(let value): value } }
}
