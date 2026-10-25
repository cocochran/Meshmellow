#!/usr/bin/env bash
set -euo pipefail
: "${ANDROID_NDK_HOME:?Set ANDROID_NDK_HOME to the Android NDK directory}"
: "${MESH_LLAMA_ROOT:?Set MESH_LLAMA_ROOT to the patched llama.cpp source}"
project_root="$(cd "$(dirname "$0")/../.." && pwd)"
worker_build="${MESH_ANDROID_WORKER_BUILD:-$project_root/apps/android/native-worker-build}"
cmake -S "$project_root/experiments/mobile-probe/android" -B "$worker_build" \
  -DCMAKE_TOOLCHAIN_FILE="$ANDROID_NDK_HOME/build/cmake/android.toolchain.cmake" \
  -DANDROID_ABI=arm64-v8a -DANDROID_PLATFORM=android-26 \
  -DANDROID_STL=c++_static -DCMAKE_BUILD_TYPE=Release \
  -DMESH_LLAMA_ROOT="$MESH_LLAMA_ROOT"
cmake --build "$worker_build" --target mesh-shard-runner --parallel 2
mkdir -p "$project_root/apps/android/app/src/main/jniLibs/arm64-v8a"
# APK-extracted native libraries are executable on Android; app-data files are not.
cp "$worker_build/mesh-shard-runner" "$project_root/apps/android/app/src/main/jniLibs/arm64-v8a/libmesh_worker.so"
