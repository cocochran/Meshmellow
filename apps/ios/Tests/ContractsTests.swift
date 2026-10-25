import XCTest
@testable import MeshContracts

final class ContractsTests: XCTestCase {
    func testEndpoints() throws {
        XCTAssertEqual(try MeshValidation.dashboardURL("http://127.0.0.1:3210").path, "/api/mesh")
        XCTAssertNoThrow(try MeshValidation.dashboardURL("https://mesh.example"))
        for address in ["http://mesh.example", "https://user:pass@mesh.example", "https://mesh.example/api", "https://mesh.example?token=secret"] { XCTAssertThrowsError(try MeshValidation.dashboardURL(address)) }
    }
    func testHeaderRecognitionDoesNotAcceptTruncatedOrUnknownFiles() throws {
        let header = Data([0x47, 0x47, 0x55, 0x46, 3, 0, 0, 0] + Array(repeating: UInt8(0), count: 16))
        XCTAssertEqual(try MeshValidation.ggufVersion(header), 3)
        XCTAssertThrowsError(try MeshValidation.ggufVersion(Data(header.prefix(8))))
        var bad = header; bad[4] = 99
        XCTAssertThrowsError(try MeshValidation.ggufVersion(bad))
    }
    func testUnknownMeasurementsRemainUnknown() throws {
        let data = Data(#"{"id":"a","name":"Phone","hardware":"CPU","begin":0,"end":4,"status":"returned","executions":1,"computeMs":null,"bufferBytes":null}"#.utf8)
        let device = try JSONDecoder().decode(MeshDevice.self, from: data)
        XCTAssertEqual(device.range, "0–3")
        XCTAssertNil(device.bufferBytes)
    }
}
