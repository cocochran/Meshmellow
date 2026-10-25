import XCTest
@testable import MeshContracts

final class EnrollmentTests: XCTestCase {
    func testFullInvitationSnapshotWithoutLegacyFields() throws {
        let json = #"{"id":"device","port":0,"mesh":{"id":"mesh","name":"Our mesh"},"members":[],"pending":[],"invite":{"id":"invite","expires":123,"ticket":"layercake://join/test"},"joining":null,"notice":""}"#
        let state = try JSONDecoder().decode(EnrollmentState.self, from: Data(json.utf8))
        XCTAssertEqual(state.invite?.ticket, "layercake://join/test")
    }
    func testBonjourAcceptsValuelessAndEmptyInvitationRecords() {
        let txt = Data([4] + Array("id=a".utf8) + [6] + Array("invite".utf8))
        XCTAssertEqual(BonjourTXT.decode(txt)?["id"], Data("a".utf8))
        XCTAssertEqual(BonjourTXT.decode(txt)?["invite"], Data())
        XCTAssertEqual(BonjourTXT.decode(Data([7] + Array("invite=".utf8)))?["invite"], Data())
    }
    func testBonjourRejectsTruncatedAndDuplicateRecords() {
        XCTAssertNil(BonjourTXT.decode(Data([20, 65])))
        XCTAssertNil(BonjourTXT.decode(Data([4] + Array("id=a".utf8) + [4] + Array("ID=b".utf8))))
        XCTAssertNil(BonjourTXT.decode(Data(repeating: 0, count: 4097)))
    }
}
