import Foundation

enum BonjourTXT {
    // NetService.dictionary(fromTXTRecord:) can bridge valueless TXT entries as
    // NSNull despite its [String: Data] signature. Parse the bounded wire format.
    static func decode(_ data: Data) -> [String: Data]? {
        guard data.count <= 4096 else { return nil }
        var result: [String: Data] = [:]
        let bytes = [UInt8](data)
        var offset = 0
        while offset < bytes.count {
            let length = Int(bytes[offset]); offset += 1
            guard offset + length <= bytes.count else { return nil }
            let record = Array(bytes[offset..<(offset + length)])
            offset += length
            if record.isEmpty { continue }
            let separator = record.firstIndex(of: 61) ?? record.count
            guard let key = String(bytes: record[..<separator], encoding: .ascii)?.lowercased(), !key.isEmpty, result[key] == nil else { return nil }
            result[key] = separator < record.count ? Data(record[(separator + 1)...]) : Data()
        }
        return result
    }
}

struct EnrollmentState: Decodable {
    struct API: Decodable { let base_url: String; let scope: String; let inference_ready: Bool; let message: String }
    struct Mesh: Decodable { let id: String; let name: String }
    struct Member: Decodable, Identifiable { let id: String; let name: String; let online: Bool?; let api: API? }
    struct Invitation: Decodable {
        let id: String; let expires: Double; let ticket: String
    }
    struct Joining: Decodable { let name: String; let expires: Double }
    struct Pending: Decodable, Identifiable {
        let id: String; let applicant: String; let name: String; let issuer: String; let expires: Double
    }
    let id: String
    let port: Int
    let mesh: Mesh?
    let members: [Member]
    let pending: [Pending]
    let invite: Invitation?
    let joining: Joining?
    let notice: String
}
