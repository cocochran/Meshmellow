# Meshmellow for iPhone and iPad

## Build the enrollment core first

```sh
rustup target add aarch64-apple-ios aarch64-apple-ios-sim
bash ../enrollment/build-mobile.sh ios
bash ../enrollment/build-mobile.sh ios-sim
```

The CMake project links the matching arm64 static library. For simulator builds,
pass an explicit simulator destination and `ARCHS=arm64 ONLY_ACTIVE_ARCH=YES`.
See [mesh creation and pairing](../enrollment/README.md) for the flow and limits.

## Native app

Native SwiftUI application targeting iOS 17+. The iPad layout uses a workspace
sidebar; compact windows and iPhones use tabs. Membership and approvals live on
Mesh, with runtime observations on Model. Light and dark palettes share semantic
colors, and invitations have copy and share actions.

Requires Xcode, CMake 3.28+ and
the iOS SDK. No model or private key is bundled. Full signed invitations work without a lookup service. No Meshmellow server needs to be hosted.
See [core membership](../../docs/mesh-membership.md).

From this directory, generate the Xcode project and build for Simulator:

```sh
cmake -S . -B build/simulator -G Xcode \
  -DCMAKE_SYSTEM_NAME=iOS -DCMAKE_OSX_SYSROOT=iphonesimulator \
  -DCMAKE_OSX_DEPLOYMENT_TARGET=17.0
xcodebuild -project build/simulator/Meshmellow.xcodeproj -scheme Meshmellow \
  -sdk iphonesimulator -configuration Debug CODE_SIGNING_ALLOWED=NO build
swift test
```

The app is `build/simulator/Debug-iphonesimulator/Meshmellow.app`. Open the generated
project in Xcode to choose an iPhone or iPad simulator. To run from the terminal
after booting a simulator:

```sh
xcrun simctl install booted build/simulator/Debug-iphonesimulator/Meshmellow.app
xcrun simctl launch booted org.meshmellow.mobile
```

For live development, start the web UI on the Mac and save
`http://127.0.0.1:3210` in the app's Settings. The simulator can access the Mac's
loopback server. It reads `/api/mesh`, not the raw `/state` or `/jobs` endpoints.
An empty address disconnects. HTTPS origins are also accepted; this does not
configure or expose a remote server. Do not expose the unauthenticated web
dashboard publicly to make it reachable from a phone.

## Build and install on the connected iPad

These commands run from the **repository root**, not `apps/ios` or the parent
workspace. They were used successfully on this Mac with Xcode 27.1 and PearPad.
Unlock the iPad, trust this Mac and enable Developer Mode. Xcode must be signed
into the Apple account for team `BYXSG896MK`. Keep the existing app installed to
preserve its device identity and mesh membership.

```sh
cd /Users/stephenson.439/llama-rpc-demo/app-planning
export DEVELOPER_DIR=/Applications/Xcode.app/Contents/Developer

# 1. Confirm that the physical iPad is connected.
xcrun devicectl list devices

# 2. Build the Rust static library for a physical iOS device.
rustup target add aarch64-apple-ios
CARGO_TARGET_DIR="$PWD/apps/enrollment/target" bash apps/enrollment/build-mobile.sh ios

# 3. Generate a signed device project.
cmake -S apps/ios -B apps/ios/build/ipad-signed -G Xcode \
  -DCMAKE_SYSTEM_NAME=iOS \
  -DCMAKE_OSX_SYSROOT=iphoneos \
  -DCMAKE_OSX_DEPLOYMENT_TARGET=17.0 \
  -DMESH_APP_TEAM=BYXSG896MK

# 4. Build/sign the SwiftUI app, linking the Rust library.
xcodebuild -project apps/ios/build/ipad-signed/Meshmellow.xcodeproj \
  -scheme Meshmellow -configuration Debug \
  -destination 'generic/platform=iOS' \
  -allowProvisioningUpdates build

# 5. Update the existing app on PearPad.
xcrun devicectl device install app \
  --device 00008132-001438EC0E39001C \
  apps/ios/build/ipad-signed/Debug-iphoneos/Meshmellow.app \
  --timeout 60

# 6. Unlock the iPad, then launch the installed bundle.
xcrun devicectl device process launch \
  --device 00008132-001438EC0E39001C \
  --terminate-existing org.meshmellow.mobile \
  --timeout 30
```

If using a different checkout, change only the first `cd`. If using another
Apple account or device, select its signing team and UDID instead.

Common failures:

- **Missing `libmeshmellow_enrollment.a`:** run step 2 first. Expected path is
  `apps/enrollment/target/aarch64-apple-ios/debug/libmeshmellow_enrollment.a`.
  An inherited `CARGO_TARGET_DIR` can redirect Cargo output; use the default
  location or set `-DMESH_ENROLLMENT_LIB=/absolute/path/to/the/library.a` in CMake.
- **Wrong platform/architecture:** `ios` / `aarch64-apple-ios` / `iphoneos`
  must agree. The device bundle is under `Debug-iphoneos`; simulator builds
  cannot be installed on this physical iPad.
- **Signing/profile error:** open Xcode → Settings → Accounts and verify the
  account/team, then open `apps/ios/build/ipad-signed/Meshmellow.xcodeproj` and
  inspect Signing & Capabilities. Keep automatic signing and the existing
  `org.meshmellow.mobile` bundle ID. Do not use `CODE_SIGNING_ALLOWED=NO` for a
  device build.
- **Install succeeds, launch fails because locked:** unlock PearPad and repeat
  step 6. Rebuilding or deleting the app is unnecessary.
- **Device unavailable:** use USB, unlock/trust the Mac and check Xcode’s
  Devices and Simulators window before retrying `devicectl`.
- **CMake generator/cache mismatch:** use a new build directory, such as
  `apps/ios/build/ipad-signed-fresh`, consistently in steps 3–5. Preserve the
  existing project and its signing settings instead of deleting unrelated files.

For a launch diagnostic, add `--console` to step 6. Add
`--json-output /tmp/meshmellow-ipad-launch.json` for a structured error report.
The installed app is branded Meshmellow; the build target is still `Meshmellow`
and its bundle identifier is `org.meshmellow.mobile`.

See [mobile capabilities and core integration](../README.md) for the precise
scope. The app now runs an Iroh enrollment peer while in the foreground.
Native inference and automatic model benchmarking remain pending.

## Native enrollment integration test

Use a dedicated fresh arm64 iPad simulator; this test creates persistent test
membership. With XcodeGen installed, generate the test project into `build/uitests`:

```sh
mkdir -p build/uitests
xcodegen generate --spec UITests/project.yml --project build/uitests
xcodebuild -project build/uitests/EnrollmentValidation.xcodeproj \
  -scheme EnrollmentValidation -destination 'id=SIMULATOR_UUID' \
  -derivedDataPath build/uitests-derived build-for-testing
```

Start `python3 ../enrollment/tests/ios-peer.py SIMULATOR_UUID` in another terminal
(after building the enrollment CLI), then run the same xcodebuild command with
`-parallel-testing-enabled NO test-without-building`. XCTest creates the mesh,
shows the invitation, approves the discovered peer and checks relaunch persistence.
The driver uses a full signed invitation and real authenticated Iroh.

Cake QR invitations are generated on-device with Core Image. Join a mesh → Scan
cake QR requests camera permission on demand, uses Apple VisionKit’s full-screen DataScanner with guidance, highlighting and pinch-to-zoom,
and fills the invitation for review. Camera images are not uploaded. Denied access
shows Settings and paste alternatives; leaving the scanner or backgrounding pauses
capture. NSCameraUsageDescription is included in the installed bundle.

Trusted device details show reported OpenAI API URLs, copy controls and inference
readiness. Loopback URLs belong to the named device. Mobile membership currently
has no HTTP inference listener and reports that explicitly.

## Background limits

iOS/iPadOS membership currently follows the visible app lifecycle. It is not an
always-on peer after the OS suspends the application. Do not use silent audio,
location or unrelated background modes to keep a mesh endpoint alive.

On iOS/iPadOS 26+, Apple's BGContinuedProcessingTask can continue a **finite,
user-started job** after the app enters the background. It requires progress,
completion and expiration/cancellation handling; it is not an indefinite peer
listener. Once managed native inference exists, a bounded generation/profiling
job can use that mechanism and release its resources when canceled or expired.
No task is registered here while there is no such executor to run.

[Apple: performing long-running tasks](https://developer.apple.com/documentation/BackgroundTasks/performing-long-running-tasks-on-ios-and-ipados)
