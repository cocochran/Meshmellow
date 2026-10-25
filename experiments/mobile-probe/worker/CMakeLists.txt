cmake_minimum_required(VERSION 3.28)
project(MeshThingWorker LANGUAGES CXX Swift)
set(MESH_LLAMA_ROOT "" CACHE PATH "Patched runtime clone")
set(MESH_SHARD "" CACHE FILEPATH "Qwen middle shard")
set(MESH_PROMPT "" CACHE FILEPATH "Rendered Qwen prompt")
set(MESH_IROH_LIB "" CACHE FILEPATH "iOS Rust static library")
set(MESH_APP_TEAM "" CACHE STRING "Signing team")
set(PROBE "${CMAKE_CURRENT_SOURCE_DIR}/..")
set(LIBROOT "${MESH_LLAMA_ROOT}/build-ios-probe")
add_executable(MeshThingWorker MACOSX_BUNDLE "${PROBE}/WorkerApp.swift" "${PROBE}/WorkerNative.cpp"
  "${PROBE}/android/runner_impl.cpp" "${PROBE}/android/socket_main.cpp" "${MESH_SHARD}" "${MESH_PROMPT}")
set_source_files_properties("${PROBE}/android/socket_main.cpp" PROPERTIES COMPILE_DEFINITIONS "main=mesh_socket_main")
set_source_files_properties("${MESH_SHARD}" "${MESH_PROMPT}" PROPERTIES MACOSX_PACKAGE_LOCATION Resources)
target_include_directories(MeshThingWorker PRIVATE "${MESH_LLAMA_ROOT}/include" "${MESH_LLAMA_ROOT}/src" "${MESH_LLAMA_ROOT}/ggml/include")
target_compile_features(MeshThingWorker PRIVATE cxx_std_17)
target_compile_options(MeshThingWorker PRIVATE "$<$<COMPILE_LANGUAGE:Swift>:-parse-as-library>")
target_link_libraries(MeshThingWorker PRIVATE "${MESH_IROH_LIB}"
  "${LIBROOT}/src/Release-iphoneos/libllama.a"
  "${LIBROOT}/ggml/src/Release-iphoneos/libggml.a"
  "${LIBROOT}/ggml/src/Release-iphoneos/libggml-cpu.a"
  "${LIBROOT}/ggml/src/ggml-metal/Release-iphoneos/libggml-metal.a"
  "${LIBROOT}/ggml/src/ggml-blas/Release-iphoneos/libggml-blas.a"
  "${LIBROOT}/ggml/src/Release-iphoneos/libggml-base.a"
  "-framework Foundation" "-framework UIKit" "-framework Metal" "-framework MetalKit"
  "-framework Accelerate" "-framework Security" "-framework SystemConfiguration" "-framework Network"
  "c++" "resolv" "iconv")
set_target_properties(MeshThingWorker PROPERTIES
  MACOSX_BUNDLE_GUI_IDENTIFIER "systems.stephenson.meshthing-worker-probe"
  MACOSX_BUNDLE_BUNDLE_NAME "MeshThingWorker"
  MACOSX_BUNDLE_BUNDLE_VERSION "1"
  MACOSX_BUNDLE_SHORT_VERSION_STRING "0.1"
  XCODE_ATTRIBUTE_DEVELOPMENT_TEAM "${MESH_APP_TEAM}"
  XCODE_ATTRIBUTE_CODE_SIGN_STYLE "Automatic"
  XCODE_ATTRIBUTE_CODE_SIGN_IDENTITY "Apple Development"
  XCODE_ATTRIBUTE_PRODUCT_BUNDLE_IDENTIFIER "systems.stephenson.meshthing-worker-probe"
  XCODE_ATTRIBUTE_SWIFT_VERSION "5.0"
  XCODE_ATTRIBUTE_TARGETED_DEVICE_FAMILY "1,2"
  XCODE_ATTRIBUTE_GENERATE_INFOPLIST_FILE "YES"
  XCODE_ATTRIBUTE_INFOPLIST_KEY_UILaunchScreen_Generation "YES"
  XCODE_ATTRIBUTE_INFOPLIST_KEY_NSLocalNetworkUsageDescription "Connect to your approved MeshThing peer for model inference."
  XCODE_ATTRIBUTE_SWIFT_OBJC_BRIDGING_HEADER "${PROBE}/WorkerBridge.h")
