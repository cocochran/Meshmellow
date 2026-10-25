import SwiftUI

struct MembershipSummary: View {
    @EnvironmentObject private var enrollment: EnrollmentStore
    var body: some View {
        if let joining = enrollment.state?.joining { waiting(joining) }
        ForEach(enrollment.state?.pending ?? []) { request in approval(request) }
        if enrollment.state?.mesh != nil { members }
        EnrollmentNotices()
    }
    private var members: some View {
        Surface {
            VStack(alignment: .leading, spacing: 22) {
                HStack {
                    Text("Your devices").font(MeshType.display(25))
                    Spacer()
                    Text("\(enrollment.state?.members.count ?? 0)").font(.caption.monospacedDigit()).foregroundStyle(MeshPalette.muted)
                        .padding(.horizontal, 10).padding(.vertical, 5).background(MeshPalette.canvas, in: Capsule())
                }
                ForEach(enrollment.state?.members ?? []) { member in
                    memberRow(member)
                    if member.id != enrollment.state?.members.last?.id { Divider().overlay(MeshPalette.line) }
                }
                StatusNote(symbol: "checkmark.shield", text: "Trusted members can invite and approve other devices.")
            }
        }
    }
    private func memberRow(_ member: EnrollmentState.Member) -> some View {
        let isSelf = member.id == enrollment.state?.id
        return DisclosureGroup {
            VStack(alignment: .leading, spacing: 8) {
                Eyebrow(text: "Device identity")
                Text(member.id).font(.caption.monospaced()).textSelection(.enabled)
                Text("Trust is saved. Connected means the core is exchanging heartbeats; compute readiness is separate.").font(.caption).foregroundStyle(MeshPalette.muted)
            }.padding(.top, 12)
        } label: {
            HStack(spacing: 14) {
                Image(systemName: isSelf ? "ipad" : "network").font(.system(size: 21, weight: .light))
                    .frame(width: 48, height: 48).foregroundStyle(MeshPalette.accent)
                    .background(MeshPalette.hero, in: RoundedRectangle(cornerRadius: 15))
                VStack(alignment: .leading, spacing: 5) {
                    Text(member.name).font(.subheadline.weight(.semibold)).foregroundStyle(MeshPalette.ink)
                    Text(isSelf ? "This device · Trusted" : (member.online == true ? "Trusted · Connected" : "Trusted · Disconnected")).font(.caption).foregroundStyle(MeshPalette.muted)
                }
            }
        }.tint(MeshPalette.muted)
    }
    private func waiting(_ joining: EnrollmentState.Joining) -> some View {
        Surface {
            VStack(alignment: .leading, spacing: 20) {
                Label("REQUEST SENT", systemImage: "hourglass").font(MeshType.label).foregroundStyle(MeshPalette.accent)
                Text("A little trust goes\na long way.").font(MeshType.display(30))
                Text("Waiting for a member of \(joining.name) to approve this device. Keep both apps open.").font(.subheadline).foregroundStyle(MeshPalette.muted)
                Text("Expires in \(Date(timeIntervalSince1970: joining.expires), style: .relative)").font(.caption).foregroundStyle(MeshPalette.muted)
                Button("Cancel request", role: .destructive) { Task { await enrollment.perform(["op": "cancel_join"]) } }.disabled(enrollment.busy)
            }
        }
    }
    private func approval(_ request: EnrollmentState.Pending) -> some View {
        VStack(alignment: .leading, spacing: 18) {
            Label("KNOCK, KNOCK", systemImage: "person.badge.plus").font(MeshType.label).foregroundStyle(MeshPalette.accent)
            Text("\(request.name) wants to join.").font(MeshType.display(27))
            Text("Trust this device to join your mesh and approve other members.").font(.subheadline).foregroundStyle(MeshPalette.muted)
            Text("DEVICE \(request.applicant.prefix(12))").font(MeshType.label).foregroundStyle(MeshPalette.muted)
            ViewThatFits(in: .horizontal) {
                HStack(spacing: 12) { trustButton(request); declineButton(request) }
                VStack(alignment: .leading, spacing: 12) { trustButton(request); declineButton(request) }
            }.disabled(enrollment.busy)
        }.padding(24).frame(maxWidth: .infinity, alignment: .leading)
            .background(MeshPalette.hero, in: RoundedRectangle(cornerRadius: 22))
            .overlay(RoundedRectangle(cornerRadius: 22).strokeBorder(MeshPalette.accent.opacity(0.2), lineWidth: 1))
    }
    private func trustButton(_ request: EnrollmentState.Pending) -> some View {
        Button { decide(request, approve: true) } label: { Label("Trust device", systemImage: "checkmark") }.buttonStyle(MeshButtonStyle())
    }
    private func declineButton(_ request: EnrollmentState.Pending) -> some View {
        Button("Decline", role: .destructive) { decide(request, approve: false) }.buttonStyle(MeshButtonStyle(primary: false))
    }
    private func decide(_ request: EnrollmentState.Pending, approve: Bool) {
        Task { await enrollment.perform(["op": "decide", "issuer": request.issuer, "request": request.id, "approve": approve]) }
    }
}

private struct EnrollmentNotices: View {
    @EnvironmentObject private var enrollment: EnrollmentStore
    var body: some View {
        if let notice = enrollment.state?.notice, !notice.isEmpty { StatusNote(symbol: "info.circle", text: notice) }
        if !enrollment.error.isEmpty { notice(enrollment.error) }
        if !enrollment.discoveryError.isEmpty { notice(enrollment.discoveryError) }
    }
    private func notice(_ message: String) -> some View {
        Label(message, systemImage: "exclamationmark.circle").font(.subheadline)
            .foregroundStyle(MeshPalette.warning).padding(16).frame(maxWidth: .infinity, alignment: .leading)
            .background(MeshPalette.warning.opacity(0.08), in: RoundedRectangle(cornerRadius: 14))
    }
}

struct PairingView: View {
    @EnvironmentObject private var enrollment: EnrollmentStore
    @Environment(\.dismiss) private var dismiss
    @AppStorage("deviceName") private var deviceName = "My device"
    @State var mode = "create"
    @State private var name = "My mesh"
    @State private var code = ""
    @State private var copied = false
    var body: some View {
        NavigationStack {
            ScrollView {
                VStack(alignment: .leading, spacing: 26) {
                    if let mesh = enrollment.state?.mesh { invitation(mesh) }
                    else if enrollment.state?.joining == nil { setup }
                    if enrollment.state?.joining != nil || enrollment.state?.pending.isEmpty == false {
                        MembershipSummary()
                    } else { EnrollmentNotices() }
                    Divider().overlay(MeshPalette.line)
                    StatusNote(symbol: "globe", text: "Pair across networks through Iroh. Keep both apps open until a trusted member approves.")
                }.padding(28).frame(maxWidth: 590, alignment: .leading).frame(maxWidth: .infinity)
            }.background(MeshPalette.canvas).foregroundStyle(MeshPalette.ink)
                .navigationBarTitleDisplayMode(.inline)
                .toolbar {
                    ToolbarItem(placement: .confirmationAction) { Button("Done") { dismiss() }.font(.subheadline.weight(.semibold)) }
                }
                .overlay { if enrollment.busy { ProgressView().padding(20).background(.regularMaterial, in: RoundedRectangle(cornerRadius: 16)) } }
        }.presentationDetents([.large]).presentationDragIndicator(.visible).presentationCornerRadius(28)
            .task {
                if enrollment.state?.mesh != nil && enrollment.state?.invite == nil && enrollment.state?.pending.isEmpty == true {
                    await enrollment.perform(["op": "invite"])
                }
            }
    }
    private var setup: some View {
        VStack(alignment: .leading, spacing: 24) {
            Picker("Set up", selection: $mode) { Text("Create a mesh").tag("create"); Text("Join a mesh").tag("join") }.pickerStyle(.segmented)
            VStack(alignment: .leading, spacing: 12) {
                Text(mode == "create" ? "Start something\nshared." : "You're invited.").font(MeshType.display(36))
                Text(mode == "create" ? "This device is your first trusted member. Give your mesh a name, then invite the others." : "Paste the invitation shared by a trusted device. Any trusted member can approve your request.")
                    .font(.subheadline).foregroundStyle(MeshPalette.muted).lineSpacing(4)
            }
            MeshField(title: "This device’s name", value: $deviceName).onChange(of: deviceName) { _, value in if value.count > 48 { deviceName = String(value.prefix(48)) } }
            if mode == "create" {
                MeshField(title: "Mesh name", value: $name).onChange(of: name) { _, value in if value.count > 48 { name = String(value.prefix(48)) } }
                Button {
                    Task {
                        await enrollment.perform(["op": "create", "name": name, "device_name": deviceName])
                        if enrollment.state?.mesh != nil { await enrollment.perform(["op": "invite"]) }
                    }
                } label: { HStack { Text("Create mesh"); Spacer(); Image(systemName: "arrow.right") } }.buttonStyle(MeshButtonStyle())
                    .disabled(enrollment.busy || enrollment.state == nil || name.trimmingCharacters(in: .whitespaces).isEmpty || deviceName.trimmingCharacters(in: .whitespaces).isEmpty)
            } else {
                VStack(alignment: .leading, spacing: 10) {
                    Text("Invitation").font(.subheadline.weight(.medium))
                    TextField("Paste your invitation", text: $code, axis: .vertical)
                        .font(.system(size: 14, design: .monospaced)).lineLimit(3...5).padding(18)
                        .background(MeshPalette.surface, in: RoundedRectangle(cornerRadius: 14))
                        .overlay(RoundedRectangle(cornerRadius: 14).strokeBorder(MeshPalette.line, lineWidth: 1))
                        .autocorrectionDisabled().textInputAutocapitalization(.never)
                        .onChange(of: code) { _, value in if value.count > 4096 { code = String(value.prefix(4096)) } }.accessibilityIdentifier("join-invitation")
                }
                Button {
                    Task { await enrollment.perform(["op": "join", "invitation": code, "device_name": deviceName]) }
                } label: { HStack { Text("Request to join"); Spacer(); Image(systemName: "arrow.right") } }.buttonStyle(MeshButtonStyle())
                    .disabled(enrollment.busy || code.isEmpty || enrollment.state == nil || deviceName.trimmingCharacters(in: .whitespaces).isEmpty)
                StatusNote(symbol: "checkmark.shield", text: "A trusted member will approve your request before you join.")
            }
        }
    }
    private func invitation(_ mesh: EnrollmentState.Mesh) -> some View {
        VStack(alignment: .leading, spacing: 24) {
            VStack(alignment: .leading, spacing: 12) {
                CakeSprinkles()
                Eyebrow(text: mesh.name)
                Text("A familiar way in.").font(MeshType.display(35))
                Text("Share this invitation, then paste it into Join a mesh on the other device.")
                    .font(.subheadline).foregroundStyle(MeshPalette.muted).lineSpacing(4)
            }
            if let invite = enrollment.state?.invite {
                VStack(alignment: .leading, spacing: 14) {
                    Label("Private invitation", systemImage: "link").font(.headline)
                    Text(invite.ticket).font(.caption.monospaced()).lineLimit(3)
                        .foregroundStyle(MeshPalette.muted)
                        .accessibilityIdentifier("invitation-link")
                    Text("One invitation. One new device.").font(.subheadline)
                }.padding(22).frame(maxWidth: .infinity, alignment: .leading)
                    .background(MeshPalette.surface, in: RoundedRectangle(cornerRadius: 18))
                    .overlay(RoundedRectangle(cornerRadius: 18).strokeBorder(MeshPalette.line, lineWidth: 1))
                HStack {
                    Label { Text("Expires in \(Date(timeIntervalSince1970: invite.expires), style: .relative)") } icon: { Image(systemName: "clock") }
                    Spacer()
                    Text("ONE DEVICE").font(MeshType.label)
                }.font(.caption).foregroundStyle(MeshPalette.muted)
                ViewThatFits(in: .horizontal) {
                    HStack(spacing: 12) { share(invite.ticket); copy(invite.ticket) }
                    VStack(alignment: .leading, spacing: 12) { share(invite.ticket); copy(invite.ticket) }
                }
                Button("Create a new invitation") { Task { await enrollment.perform(["op": "invite"]); copied = false } }
                    .font(.subheadline).foregroundStyle(MeshPalette.muted)
                    .disabled(enrollment.busy || enrollment.state?.pending.isEmpty == false)
            } else {
                Button { Task { await enrollment.perform(["op": "invite"]) } } label: { Label("Create invitation", systemImage: "plus") }
                    .buttonStyle(MeshButtonStyle()).disabled(enrollment.busy || enrollment.state?.pending.isEmpty == false)
            }
            StatusNote(symbol: "checkmark.shield", text: "You stay in control. A trusted member must approve each new device.")
        }
    }
    private func share(_ code: String) -> some View {
        ShareLink(item: code) { Label("Share invitation", systemImage: "square.and.arrow.up") }.buttonStyle(MeshButtonStyle())
    }
    private func copy(_ code: String) -> some View {
        Button { UIPasteboard.general.string = code; copied = true } label: { Label(copied ? "Copied" : "Copy invitation", systemImage: copied ? "checkmark" : "doc.on.doc") }.buttonStyle(MeshButtonStyle(primary: false))
    }
}
