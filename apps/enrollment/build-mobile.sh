#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
case "${1:-}" in
  ios)
    IPHONEOS_DEPLOYMENT_TARGET=17.0 cargo build --locked --target aarch64-apple-ios
    ;;
  ios-sim)
    IPHONEOS_DEPLOYMENT_TARGET=17.0 cargo build --locked --target aarch64-apple-ios-sim
    ;;
  android)
    : "${ANDROID_NDK_HOME:?Set ANDROID_NDK_HOME to your Android NDK directory}"
    case "$(uname -s)" in
      Darwin) ndk_host=darwin-x86_64 ;;
      Linux) ndk_host=linux-x86_64 ;;
      *) echo 'Use a macOS or Linux build host.' >&2; exit 1 ;;
    esac
    ndk_bin="$ANDROID_NDK_HOME/toolchains/llvm/prebuilt/$ndk_host/bin"
    export CARGO_TARGET_AARCH64_LINUX_ANDROID_LINKER="$ndk_bin/aarch64-linux-android26-clang"
    export CC_aarch64_linux_android="$CARGO_TARGET_AARCH64_LINUX_ANDROID_LINKER"
    export AR_aarch64_linux_android="$ndk_bin/llvm-ar"
    cargo build --locked --target aarch64-linux-android
    mkdir -p ../android/app/src/main/jniLibs/arm64-v8a
    cp target/aarch64-linux-android/debug/libmeshthing_enrollment.so ../android/app/src/main/jniLibs/arm64-v8a/
    # Keep full symbols in target/ for debugging, not in the distributable APK.
    "$ndk_bin/llvm-strip" --strip-unneeded ../android/app/src/main/jniLibs/arm64-v8a/libmeshthing_enrollment.so
    ;;
  *) echo 'Usage: build-mobile.sh ios|ios-sim|android' >&2; exit 1 ;;
esac
