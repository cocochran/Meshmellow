# Layercake for iPhone and iPad

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
colors, and invitations display eight numbered emoji tiles.

Requires Xcode, CMake 3.28+ and
the iOS SDK. No model or private key is bundled. Set `LAYERCAKE_RENDEZVOUS_ID`
when building the Rust library to select the shared public invitation lookup ID.
See [lookup deployment](../enrollment/README.md#invitation-lookup-deployment).

From this directory, generate the Xcode project and build for Simulator:

```sh
cmake -S . -B build/simulator -G Xcode \
  -DCMAKE_SYSTEM_NAME=iOS -DCMAKE_OSX_SYSROOT=iphonesimulator \
  -DCMAKE_OSX_DEPLOYMENT_TARGET=17.0
xcodebuild -project build/simulator/MeshThing.xcodeproj -scheme MeshThing \
  -sdk iphonesimulator -configuration Debug CODE_SIGNING_ALLOWED=NO build
swift test
```

The app is `build/simulator/Debug-iphonesimulator/MeshThing.app`. Open the generated
project in Xcode to choose an iPhone or iPad simulator. To run from the terminal
after booting a simulator:

```sh
xcrun simctl install booted build/simulator/Debug-iphonesimulator/MeshThing.app
xcrun simctl launch booted org.meshthing.mobile
```

For live development, start the web UI on the Mac and save
`http://127.0.0.1:3210` in the app's Settings. The simulator can access the Mac's
loopback server. It reads `/api/mesh`, not the raw `/state` or `/jobs` endpoints.
An empty address disconnects. HTTPS origins are also accepted; this does not
configure or expose a remote server. Do not expose the unauthenticated web
dashboard publicly to make it reachable from a phone.

For a physical iPhone/iPad build, generate a separate project:

```sh
cmake -S . -B build/device -G Xcode \
  -DCMAKE_SYSTEM_NAME=iOS -DCMAKE_OSX_SYSROOT=iphoneos \
  -DCMAKE_OSX_DEPLOYMENT_TARGET=17.0 -DMESH_APP_TEAM=YOUR_TEAM_ID
open build/device/MeshThing.xcodeproj
```

Select your signing team and an appropriate bundle identifier, then build/install
through Xcode. A simulator `.app` or unsigned device `.app` cannot be installed as
a signed iPhone release. TestFlight/App Store distribution is separate work.

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
The driver uses the configured invitation lookup plus real authenticated Iroh.
