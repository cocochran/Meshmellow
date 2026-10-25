cmake_minimum_required(VERSION 3.28)
project(MeshThing LANGUAGES Swift)
add_executable(MeshThing MACOSX_BUNDLE
  Sources/MeshThingApp.swift Sources/MeshStore.swift Sources/Views.swift Sources/Palette.swift Sources/Design.swift
  Sources/EnrollmentStore.swift Sources/EnrollmentViews.swift Sources/InvitationQR.swift Shared/Contracts.swift Shared/EnrollmentState.swift
  Resources/Assets.xcassets)
set_source_files_properties(Resources/Assets.xcassets PROPERTIES
  MACOSX_PACKAGE_LOCATION Resources XCODE_EXPLICIT_FILE_TYPE folder.assetcatalog)
target_compile_options(MeshThing PRIVATE -parse-as-library)
if(NOT DEFINED MESH_ENROLLMENT_LIB)
  if(CMAKE_OSX_SYSROOT MATCHES "simulator")
    set(MESH_RUST_TARGET "aarch64-apple-ios-sim")
  else()
    set(MESH_RUST_TARGET "aarch64-apple-ios")
  endif()
  set(MESH_ENROLLMENT_LIB "${CMAKE_CURRENT_SOURCE_DIR}/../enrollment/target/${MESH_RUST_TARGET}/debug/libmeshthing_enrollment.a")
endif()
target_link_libraries(MeshThing PRIVATE "${MESH_ENROLLMENT_LIB}" "-framework Security" "-framework SystemConfiguration" "-framework Network" "-liconv" "-lresolv")
set_target_properties(MeshThing PROPERTIES
  XCODE_ATTRIBUTE_ASSETCATALOG_COMPILER_APPICON_NAME "AppIcon"
  XCODE_ATTRIBUTE_SWIFT_OBJC_BRIDGING_HEADER "${CMAKE_CURRENT_SOURCE_DIR}/Sources/EnrollmentBridge.h"
  MACOSX_BUNDLE_INFO_PLIST "${CMAKE_CURRENT_SOURCE_DIR}/Info.plist"
  MACOSX_BUNDLE_GUI_IDENTIFIER "org.meshthing.mobile"
  MACOSX_BUNDLE_BUNDLE_NAME "Layercake"
  MACOSX_BUNDLE_BUNDLE_VERSION "1"
  MACOSX_BUNDLE_SHORT_VERSION_STRING "0.1.0"
  XCODE_ATTRIBUTE_SWIFT_VERSION "5.0"
  XCODE_ATTRIBUTE_IPHONEOS_DEPLOYMENT_TARGET "17.0"
  XCODE_ATTRIBUTE_TARGETED_DEVICE_FAMILY "1,2"
  XCODE_ATTRIBUTE_CODE_SIGN_STYLE "Automatic"
  XCODE_ATTRIBUTE_PRODUCT_BUNDLE_IDENTIFIER "org.meshthing.mobile"
  XCODE_ATTRIBUTE_GENERATE_INFOPLIST_FILE "NO")
if(DEFINED MESH_APP_TEAM)
  set_target_properties(MeshThing PROPERTIES XCODE_ATTRIBUTE_DEVELOPMENT_TEAM "${MESH_APP_TEAM}")
endif()
