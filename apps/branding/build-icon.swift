// Package the supplied logo on an opaque launcher canvas; preserve pixel edges.
// From the repo root: swiftc apps/branding/build-icon.swift -o /tmp/meshmellow-icon
// Then run /tmp/meshmellow-icon from the same directory.
import Foundation
import CoreGraphics
import ImageIO
import UniformTypeIdentifiers
let source = URL(fileURLWithPath: "apps/branding/meshmellow.png")
let output = URL(fileURLWithPath: "apps/ios/Resources/Assets.xcassets/AppIcon.appiconset/app-icon.png")
guard let input = CGImageSourceCreateWithURL(source as CFURL, nil),
      let logo = CGImageSourceCreateImageAtIndex(input, 0, nil),
      let context = CGContext(data: nil, width: 1024, height: 1024, bitsPerComponent: 8, bytesPerRow: 0, space: CGColorSpaceCreateDeviceRGB(), bitmapInfo: CGImageAlphaInfo.noneSkipLast.rawValue) else {
    fatalError("Cannot load the logo or create the icon canvas")
}
context.setFillColor(CGColor(red: 245.0/255, green: 244.0/255, blue: 240.0/255, alpha: 1))
context.fill(CGRect(x: 0, y: 0, width: 1024, height: 1024))
context.interpolationQuality = .none
context.draw(logo, in: CGRect(x: 96, y: 210, width: 832, height: 832))
guard let rendered = context.makeImage(),
      let destination = CGImageDestinationCreateWithURL(output as CFURL, UTType.png.identifier as CFString, 1, nil) else {
    fatalError("Cannot create launcher icon")
}
CGImageDestinationAddImage(destination, rendered, nil)
guard CGImageDestinationFinalize(destination) else { fatalError("Cannot save launcher icon") }
print("Created \(output.path)")
