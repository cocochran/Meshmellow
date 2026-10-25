import SwiftUI
import CoreImage.CIFilterBuiltins
import AVFoundation

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
                    .accessibilityLabel("Invitation QR code. Scan with Layercake on another device.")
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
    @State private var error = ""
    let onScan: (String) -> Void
    var body: some View {
        NavigationStack {
            VStack(spacing: 20) {
                Text("Find your slice.").font(MeshType.display(32))
                Text("Point your camera at the cake QR on a trusted device.").foregroundStyle(MeshPalette.muted)
                CameraPreview(onScan: { text in onScan(text); dismiss() }, onError: { error = $0 })
                    .frame(maxHeight: 430).clipShape(RoundedRectangle(cornerRadius: 20))
                if !error.isEmpty {
                    Text(error).foregroundStyle(MeshPalette.warning)
                    Button("Camera settings") { if let url = URL(string: UIApplication.openSettingsURLString) { UIApplication.shared.open(url) } }
                }
                Text("The scan fills your invitation. You still choose Request to join.").font(.footnote).foregroundStyle(MeshPalette.muted)
            }.padding(24).background(MeshPalette.canvas)
                .toolbar { ToolbarItem(placement: .cancellationAction) { Button("Close camera") { dismiss() } } }
        }
    }
}

private struct CameraPreview: UIViewControllerRepresentable {
    let onScan: (String) -> Void
    let onError: (String) -> Void
    func makeUIViewController(context: Context) -> InvitationCameraController { InvitationCameraController(onScan: onScan, onError: onError) }
    func updateUIViewController(_ controller: InvitationCameraController, context: Context) {}
}

private final class InvitationCameraController: UIViewController, AVCaptureMetadataOutputObjectsDelegate {
    private let session = AVCaptureSession()
    private let queue = DispatchQueue(label: "layercake.invitation-camera")
    private var preview: AVCaptureVideoPreviewLayer!
    private var configured = false
    private var visible = false
    private var delivered = false
    private let onScan: (String) -> Void
    private let onError: (String) -> Void
    init(onScan: @escaping (String) -> Void, onError: @escaping (String) -> Void) {
        self.onScan = onScan; self.onError = onError
        super.init(nibName: nil, bundle: nil)
    }
    required init?(coder: NSCoder) { fatalError("Not used") }
    override func viewDidLoad() {
        super.viewDidLoad()
        view.backgroundColor = .black
        preview = AVCaptureVideoPreviewLayer(session: session); preview.videoGravity = .resizeAspectFill
        view.layer.addSublayer(preview)
        NotificationCenter.default.addObserver(self, selector: #selector(suspend), name: UIApplication.willResignActiveNotification, object: nil)
        NotificationCenter.default.addObserver(self, selector: #selector(resume), name: UIApplication.didBecomeActiveNotification, object: nil)
    }
    override func viewDidLayoutSubviews() { super.viewDidLayoutSubviews(); preview.frame = view.bounds }
    override func viewDidAppear(_ animated: Bool) { super.viewDidAppear(animated); visible = true; resume() }
    override func viewWillDisappear(_ animated: Bool) { visible = false; suspend(); super.viewWillDisappear(animated) }
    deinit { NotificationCenter.default.removeObserver(self) }
    @objc private func suspend() { queue.async { self.session.stopRunning() } }
    @objc private func resume() {
        guard visible && !delivered else { return }
        switch AVCaptureDevice.authorizationStatus(for: .video) {
        case .authorized: start()
        case .notDetermined:
            AVCaptureDevice.requestAccess(for: .video) { [weak self] granted in DispatchQueue.main.async {
                guard let self, self.visible else { return }
                if granted { self.start() } else { self.onError("Allow camera access in Settings, or close this and paste an invitation.") }
            } }
        default: onError("Camera access is off. Enable it in Settings, or paste an invitation instead.")
        }
    }
    private func start() {
        queue.async {
            do {
                if !self.configured {
                    self.session.beginConfiguration()
                    defer { self.session.commitConfiguration() }
                    guard let camera = AVCaptureDevice.default(.builtInWideAngleCamera, for: .video, position: .back) ?? AVCaptureDevice.default(for: .video) else { throw AppFailure.message("No camera is available. Paste an invitation instead.") }
                    let input = try AVCaptureDeviceInput(device: camera)
                    let output = AVCaptureMetadataOutput()
                    guard self.session.canAddInput(input), self.session.canAddOutput(output) else { throw AppFailure.message("Cannot open the camera. Paste an invitation instead.") }
                    self.session.addInput(input); self.session.addOutput(output)
                    output.setMetadataObjectsDelegate(self, queue: .main)
                    output.metadataObjectTypes = [.qr]
                    self.configured = true
                }
                self.session.startRunning()
            } catch { DispatchQueue.main.async { self.onError(error.localizedDescription) } }
        }
    }
    func metadataOutput(_ output: AVCaptureMetadataOutput, didOutput objects: [AVMetadataObject], from connection: AVCaptureConnection) {
        guard visible && !delivered, let text = (objects.first as? AVMetadataMachineReadableCodeObject)?.stringValue else { return }
        guard text.utf8.count <= 4096 && text.hasPrefix("layercake://join/") else { onError("That is not a Layercake invitation. Try the cake QR."); return }
        delivered = true; suspend(); onScan(text)
    }
}
