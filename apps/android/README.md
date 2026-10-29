# Meshmellow for Android

Native Kotlin / Jetpack Compose application, Android 8/API 26 or later. Build
with JDK 17+ compatible with Gradle 9.4.1 (JDK 21 recommended), Android SDK platform
36.1 and build tools 36.1.0. The Gradle wrapper pins and checksums its distribution.

Build the arm64 enrollment library with your Android NDK first:

```sh
rustup target add aarch64-linux-android
ANDROID_NDK_HOME=/path/to/ndk bash ../enrollment/build-mobile.sh android
```

Set `ANDROID_HOME` to your SDK directory, then:

```sh
./gradlew assembleDebug testDebugUnitTest lintDebug
```

The installable development artifact is
`app/build/outputs/apk/debug/app-debug.apk`. It uses the Android debug signing key;
production signing and Play distribution are separate work.

For a dedicated emulator, substitute its exact ID below:

```sh
adb -s emulator-5580 install -r app/build/outputs/apk/debug/app-debug.apk
adb -s emulator-5580 reverse tcp:3210 tcp:3210
adb -s emulator-5580 shell am start -n org.meshmellow.mobile/.MainActivity
```

With the web UI running on the host, save `http://127.0.0.1:3210` in the app's
Settings. The explicit development port forward keeps the webserver on loopback.
An empty address disconnects. HTTPS dashboard origins are supported; the app does
not deploy, expose or authenticate a remote dashboard. Do not expose the existing
unauthenticated web dashboard publicly just to connect a phone.

Use an explicit device ID for installation and port forwarding. Do not replace
or interfere with active experimental workers while validating the app.

See [mobile capabilities and core integration](../README.md). The application
runs the core Iroh endpoint with signed full invitations. By default it runs while
the app is visible; Keep mesh active opts into a foreground service.
It creates meshes, shares signed invitations and manages trusted membership.
Native inference and automatic benchmarking are not yet connected. No broad storage permission, diagnostic peer identity or model weights are included.

Full invitations work over Iroh without a lookup service. No Meshmellow server needs to be hosted. See [core membership](../../docs/mesh-membership.md). Device validation uses physical phones; do not start an Android emulator.

Cake QR invitations are generated locally with ZXing. Join a mesh → Scan cake QR
requests CAMERA permission on demand and opens a full-screen CameraX preview with bundled, offline ML Kit QR recognition.
Tap to focus, pinch to zoom, automatic zoom suggestions and a flashlight help
read dense invitations. ZXing is used only to generate the QR image.
Denied access offers Camera settings and paste. Scanning fills the invitation;
Request to join and trusted approval are still required. Camera hardware is
optional, so devices without a camera can paste invitations.

Trusted device cards show reported OpenAI API URLs, copy controls and inference
readiness. Loopback URLs belong to the named device. Mobile membership currently
has no HTTP inference listener and reports that explicitly.

## Keep the mesh active in the background

On the Mesh tab (also Settings), enable **Keep mesh active** after joining or
creating a mesh. Android runs the existing Iroh core in a connected-device
foreground service with a persistent notification and Stop background usage
action. The screen may turn off; a partial wake lock keeps the CPU available for
peer heartbeats. This is connectivity, not a native model-execution service.
Dashboard polling and camera scanning still stop when their screens are hidden.

The application owns one EnrollmentController. Visible activities and the service
share that owner; activity teardown cannot close a service-owned core. A flow
serializes endpoint close/open across quick activity transitions. Turning off
background mode leaves membership saved; Leave mesh also stops the service and
releases its wake lock. Mode is session-only: Android task-manager Stop, force-stop,
process death and reboot do not silently restart it. Swiping away the activity
while the mode is active leaves the foreground service running.

On Android 13+, notification permission is requested when enabling the mode.
Denial still allows the foreground service; Android Active apps and the in-app
switch can stop it. No camera service, boot receiver, alarm restart, or battery
exemption request is added.

For long screen-off sessions, use **Battery settings** to allow unrestricted
battery usage for Meshmellow. Doze can suspend networking and ignore wake locks
without exemption, and vendor restrictions/resource pressure can still stop an
app. The app never changes battery settings on the user's behalf. Background
connectivity uses additional power; charging is preferable.

Relevant platform references:
[connected-device service](https://developer.android.com/develop/background-work/services/fgs/service-types#connected-device),
[Doze restrictions](https://developer.android.com/training/monitoring-device-state/doze-standby).

Physical-phone lifecycle validation uses an isolated Application/core identity:

```sh
env ANDROID_HOME="$ANDROID_HOME" apps/android/gradlew -p apps/android \
  -PbackgroundTest=true assembleDebug assembleDebugAndroidTest lintDebug testDebugUnitTest
# Install debug APK and androidTest APK on the physical device first.
python3 tests/mobile/android_mesh.py ADB_SERIAL --background
```

The instrumentation launches/finishes a real Activity, waits longer than two
heartbeat expiry windows with the screen off and only the service owning the core, verifies the
connection from both peers, invokes the notification Stop action, resumes saved
membership, checks Leave cleanup, and checks for leaked wake locks. It uses test-only Activity lock-screen flags, restores the screen-off state, and
does not alter the user's mesh, keyguard or battery policy. This is a bounded background test, not
an overnight screen-off/Doze reliability claim.
