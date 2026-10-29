import SwiftUI
import CoreImage.CIFilterBuiltins
import AVFoundation
import VisionKit

struct CakeInvitationQR: View {
    let ticket: String
    private var qr: UIImage? {
        let filter = CIFilter.qrCodeGenerator()
        filter.message = Data(ticket.utf8)
        filter.correctionLevel = "M"
        guard let output = filter.outputImage,
              let image = CIContext().createCGImage(output.transformed(by: CGAffineTransform(scaleX: 6, y: 6)), from: output.extent.applying(CGAffineTransform(scaleX: 6, y: 6))) else { return nil }
        return UIImage(cgImage: image)
    }
    var body: some View {
        VStack(spacing: 0) {
            HStack {
                berries; Spacer()
                Image("BrandCake").resizable().interpolation(.none).scaledToFit().frame(width: 48, height: 48)
                Spacer(); berries
            }.padding(.horizontal, 24).padding(.top, 12).background(.white)
            if let qr {
                Image(uiImage: qr).interpolation(.none).resizable().scaledToFit()
                    .padding(18).background(.white)
                    .accessibilityLabel("Invitation QR code. Scan with Meshmellow on another device.")
                    .accessibilityIdentifier("invitation-qr")
            } else { Text("Use Copy invitation below.").foregroundStyle(.black).padding().background(.white) }
            Rectangle().fill(Color(red: 0.74, green: 0.50, blue: 0.34)).frame(height: 18)
            Rectangle().fill(Color(red: 0.96, green: 0.84, blue: 0.72)).frame(height: 8)
            Rectangle().fill(Color(red: 0.70, green: 0.29, blue: 0.33)).frame(height: 9)
            Rectangle().fill(Color(red: 0.74, green: 0.50, blue: 0.34)).frame(height: 15)
        }.overlay(Rectangle().stroke(Color(red: 0.84, green: 0.74, blue: 0.63), lineWidth: 4))
            .frame(maxWidth: 340).shadow(color: .black.opacity(0.07), radius: 0, x: 6, y: 7)
            .frame(maxWidth: .infinity)
        Text("A slice of your mesh.").font(MeshType.display(24)).frame(maxWidth: .infinity)
    }
    private var berries: some View {
        HStack(spacing: 15) { ForEach(0..<2) { _ in Rectangle().fill(Color(red: 0.72, green: 0.24, blue: 0.29)).frame(width: 12, height: 12).shadow(color: .pink.opacity(0.5), radius: 0, x: 4, y: -4) } }
    }
}

struct InvitationCamera: View {
    @Environment(\.dismiss) private var dismiss
    @State private var allowed = false
    @State private var error = ""
    let onScan: (String) -> Void
    var body: some View {
        NavigationStack {
            ZStack(alignment: .bottom) {
                Color.black.ignoresSafeArea()
                if allowed {
                    NativeInvitationScanner(onScan: { onScan($0); dismiss() }, onError: { error = $0 })
                        .ignoresSafeArea(edges: .bottom)
                }
                VStack(spacing: 12) {
                    Text(error.isEmpty ? "Point at the invitation QR" : error).font(.headline)
                    Text("Tap to focus · Pinch to zoom").font(.subheadline)
                    if !error.isEmpty {
                        Button("Camera settings") { if let url = URL(string: UIApplication.openSettingsURLString) { UIApplication.shared.open(url) } }
                        Button("Paste an invitation instead") { dismiss() }
                    }
                }.padding(24).frame(maxWidth: .infinity).background(.ultraThinMaterial)
            }.navigationTitle("Scan invitation").navigationBarTitleDisplayMode(.inline)
                .toolbar { ToolbarItem(placement: .cancellationAction) { Button("Close") { dismiss() } } }
        }.task {
            guard DataScannerViewController.isSupported else { error = "This device cannot use the camera scanner. Paste an invitation instead."; return }
            let granted = await AVCaptureDevice.requestAccess(for: .video)
            if granted { allowed = true } else { error = "Allow camera access in Settings, or paste an invitation." }
        }
    }
}

private struct NativeInvitationScanner: UIViewControllerRepresentable {
    let onScan: (String) -> Void
    let onError: (String) -> Void
    func makeCoordinator() -> Coordinator { Coordinator(onScan: onScan, onError: onError) }
    func makeUIViewController(context: Context) -> ScannerHost {
        let host = ScannerHost()
        host.scanner.delegate = context.coordinator
        host.onError = onError
        return host
    }
    func updateUIViewController(_ controller: ScannerHost, context: Context) {}
    static func dismantleUIViewController(_ controller: ScannerHost, coordinator: Coordinator) { controller.scanner.stopScanning() }
    final class Coordinator: NSObject, DataScannerViewControllerDelegate {
        var delivered = false
        let onScan: (String) -> Void
        let onError: (String) -> Void
        init(onScan: @escaping (String) -> Void, onError: @escaping (String) -> Void) { self.onScan = onScan; self.onError = onError }
        private func read(_ items: [RecognizedItem], scanner: DataScannerViewController) {
            guard !delivered else { return }
            for item in items {
                if case .barcode(let barcode) = item, let text = barcode.payloadStringValue {
                    guard text.utf8.count <= 4096 && text.hasPrefix("meshmellow://join/") else { onError("That QR is not a Meshmellow invitation."); continue }
                    delivered = true; scanner.stopScanning(); onScan(text); return
                }
            }
        }
        func dataScanner(_ dataScanner: DataScannerViewController, didAdd addedItems: [RecognizedItem], allItems: [RecognizedItem]) { read(addedItems, scanner: dataScanner) }
        func dataScanner(_ dataScanner: DataScannerViewController, didUpdate updatedItems: [RecognizedItem], allItems: [RecognizedItem]) { read(updatedItems, scanner: dataScanner) }
        func dataScanner(_ dataScanner: DataScannerViewController, didTapOn item: RecognizedItem) { read([item], scanner: dataScanner) }
        func dataScanner(_ dataScanner: DataScannerViewController, becameUnavailableWithError error: DataScannerViewController.ScanningUnavailable) { onError("Camera unavailable. Close and reopen the scanner, or paste your invitation.") }
    }
}

private final class ScannerHost: UIViewController {
    let scanner = DataScannerViewController(recognizedDataTypes: [.barcode(symbologies: [.qr])], qualityLevel: .accurate, recognizesMultipleItems: false, isHighFrameRateTrackingEnabled: true, isPinchToZoomEnabled: true, isGuidanceEnabled: true, isHighlightingEnabled: true)
    var onError: (String) -> Void = { _ in }
    private var visible = false
    override func viewDidLoad() {
        super.viewDidLoad()
        addChild(scanner); view.addSubview(scanner.view); scanner.didMove(toParent: self)
        NotificationCenter.default.addObserver(self, selector: #selector(pause), name: UIApplication.willResignActiveNotification, object: nil)
        NotificationCenter.default.addObserver(self, selector: #selector(resume), name: UIApplication.didBecomeActiveNotification, object: nil)
    }
    override func viewDidLayoutSubviews() { super.viewDidLayoutSubviews(); scanner.view.frame = view.bounds }
    override func viewDidAppear(_ animated: Bool) { super.viewDidAppear(animated); visible = true; resume() }
    override func viewWillDisappear(_ animated: Bool) { visible = false; pause(); super.viewWillDisappear(animated) }
    @objc private func pause() { scanner.stopScanning() }
    @objc private func resume() {
        guard visible else { return }
        do { try scanner.startScanning() } catch { onError("Cannot start the camera. Check camera access in Settings or paste an invitation.") }
    }
    deinit { NotificationCenter.default.removeObserver(self) }
}
