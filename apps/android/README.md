# Layercake for Android

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
adb -s emulator-5580 shell am start -n org.meshthing.mobile/.MainActivity
```

With the web UI running on the host, save `http://127.0.0.1:3210` in the app's
Settings. The explicit development port forward keeps the webserver on loopback.
An empty address disconnects. HTTPS dashboard origins are supported; the app does
not deploy, expose or authenticate a remote dashboard. Do not expose the existing
unauthenticated web dashboard publicly just to connect a phone.

Use an explicit device ID for installation and port forwarding. Do not replace
or interfere with active experimental workers while validating the app.

See [mobile capabilities and core integration](../README.md). The application
runs the core Iroh endpoint with Internet invitation lookup while in the foreground.
It creates meshes, shares signed invitations and manages trusted membership.
Native inference and automatic benchmarking are not yet connected. No broad storage permission, background
service, wake lock, diagnostic peer identity or model weights are included.

Full invitations work over Iroh without a lookup service. No Layercake server needs to be hosted. See [core membership](../../docs/mesh-membership.md). Device validation uses physical phones; do not start an Android emulator.

Cake QR invitations are generated locally with ZXing. Join a mesh → Scan cake QR
requests CAMERA permission on demand and opens the lifecycle-managed scanner.
Denied access offers Camera settings and paste. Scanning fills the invitation;
Request to join and trusted approval are still required. Camera hardware is
optional, so devices without a camera can paste invitations.
