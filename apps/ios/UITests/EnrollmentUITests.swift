import XCTest

final class EnrollmentUITests: XCTestCase {
    func testCreateInviteApproveAndPersist() {
        continueAfterFailure = false
        let app = XCUIApplication()
        addUIInterruptionMonitor(withDescription: "Local network") { alert in
            if alert.buttons["Allow"].exists { alert.buttons["Allow"].tap(); return true }
            if alert.buttons["OK"].exists { alert.buttons["OK"].tap(); return true }
            return false
        }
        app.launch()
        XCTAssertTrue(app.buttons["Create a mesh"].waitForExistence(timeout: 15))
        app.buttons["Create a mesh"].tap()
        app.tap() // Delivers any system permission interruption to the monitor.
        XCTAssertTrue(app.buttons["Create mesh"].waitForExistence(timeout: 10))
        let create = app.buttons["Create mesh"]
        let enabled = NSPredicate(format: "enabled == true")
        expectation(for: enabled, evaluatedWith: create)
        waitForExpectations(timeout: 15)
        create.tap()
        XCTAssertTrue(app.descendants(matching: .any)["invitation-link"].waitForExistence(timeout: 50))
        let code = app.descendants(matching: .any)["invitation-link"].label
        XCTAssertTrue(code.hasPrefix("meshmellow://join/"))
        let invitation = XCTAttachment(screenshot: app.screenshot()); invitation.name = "Create mesh and signed invitation"; invitation.lifetime = .keepAlways; add(invitation)
        print("ENROLLMENT_INVITATION \(code)")
        app.buttons["Copy invitation"].tap()
        app.buttons["Done"].tap()
        // The companion CLI integration driver joins through a self-contained invitation + the core Iroh endpoint.
        XCTAssertTrue(app.buttons["Trust device"].waitForExistence(timeout: 120))
        app.buttons["Trust device"].tap()
        XCTAssertTrue(app.staticTexts["Validation peer"].waitForExistence(timeout: 15))
        let trusted = XCTAttachment(screenshot: app.screenshot()); trusted.name = "Trusted peer after real network approval"; trusted.lifetime = .keepAlways; add(trusted)
        app.terminate(); app.launch()
        XCTAssertTrue(app.staticTexts["Validation peer"].waitForExistence(timeout: 15))
        XCTAssertTrue(app.buttons["Invite a device"].exists)
    }
}
