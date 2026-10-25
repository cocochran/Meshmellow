// swift-tools-version: 5.9
import PackageDescription
let package = Package(name: "MeshContracts", platforms: [.macOS(.v13), .iOS(.v17)],
  products: [.library(name: "MeshContracts", targets: ["MeshContracts"])],
  targets: [.target(name: "MeshContracts", path: "Shared"), .testTarget(name: "MeshContractsTests", dependencies: ["MeshContracts"], path: "Tests")])
