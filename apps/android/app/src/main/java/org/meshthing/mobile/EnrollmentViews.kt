package org.meshthing.mobile

import android.content.ClipData
import android.content.ClipboardManager
import android.content.Context
import android.content.Intent
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.background
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.ui.Alignment
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.verticalScroll
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.runtime.saveable.rememberSaveable
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import kotlinx.coroutines.launch
import java.text.DateFormat
import java.util.Date

@Composable fun LeaveMeshButton(enrollment: EnrollmentController) {
    var confirmingLeave by remember { mutableStateOf(false) }
    val scope = rememberCoroutineScope()
    OutlinedButton(onClick = { confirmingLeave = true }, enabled = !enrollment.busy) {
        Text("Leave mesh", color = MaterialTheme.colorScheme.error)
    }
    if (confirmingLeave) AlertDialog(
        onDismissRequest = { confirmingLeave = false },
        title = { Text("Leave this mesh?") },
        text = { Text("This device will disconnect and return to Create / Join. Other devices keep their mesh and trust history.") },
        confirmButton = { TextButton(onClick = { confirmingLeave = false; scope.launch { enrollment.perform("leave") } }) { Text("Leave mesh") } },
        dismissButton = { TextButton(onClick = { confirmingLeave = false }) { Text("Stay in mesh") } }
    )
}

@Composable fun MembershipSummary(enrollment: EnrollmentController) {
    val state = enrollment.state
    val scope = rememberCoroutineScope()
    state?.meshName?.let { name ->
        Text(name, style = MaterialTheme.typography.titleLarge)
        Text("Trusted devices", fontWeight = FontWeight.SemiBold)
        state.members.forEach { member ->
            Text("${member.name}${if (member.id == state.id) " (this device)" else ""} · Trusted · ${if (member.online) "Connected" else "Disconnected"}")
        }
        Text("Devices are enrolled. Model benchmarking and compute setup are still pending.", style = MaterialTheme.typography.bodySmall)
        LeaveMeshButton(enrollment)
    }
    state?.joiningName?.let { name ->
        Text("Waiting to join $name", style = MaterialTheme.typography.titleMedium)
        Text("Ask any trusted member to approve this device. Keep both apps open.")
        TextButton(onClick = { scope.launch { enrollment.perform("cancel_join") } }, enabled = !enrollment.busy) { Text("Cancel request") }
    }
    state?.pending?.forEach { request ->
        Card(colors = CardDefaults.cardColors(containerColor = MaterialTheme.colorScheme.secondaryContainer)) {
            Column(Modifier.fillMaxWidth().padding(16.dp), verticalArrangement = Arrangement.spacedBy(10.dp)) {
                Text("${request.name} wants to join", fontWeight = FontWeight.SemiBold)
                Text("Device ID: ${request.applicant.take(12)}", style = MaterialTheme.typography.labelSmall)
                Text("Trust lets this device invite and approve other devices.", style = MaterialTheme.typography.bodySmall)
                Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                    Button(onClick = { scope.launch { enrollment.perform("decide", "issuer" to request.issuer, "request" to request.id, "approve" to true) } }, enabled = !enrollment.busy) { Text("Trust device") }
                    TextButton(onClick = { scope.launch { enrollment.perform("decide", "issuer" to request.issuer, "request" to request.id, "approve" to false) } }, enabled = !enrollment.busy) { Text("Decline") }
                }
            }
        }
    }
    if (!state?.notice.isNullOrEmpty()) Text(state!!.notice)
    if (enrollment.error.isNotEmpty()) Text(enrollment.error, color = MaterialTheme.colorScheme.error)
    if (enrollment.discoveryError.isNotEmpty()) Text(enrollment.discoveryError, color = MaterialTheme.colorScheme.error)
}

@OptIn(ExperimentalMaterial3Api::class)
@Composable fun PairingDialog(store: MeshStore, startWithJoin: Boolean, close: () -> Unit) {
    val enrollment = store.enrollment
    val state = enrollment.state
    var join by rememberSaveable { mutableStateOf(startWithJoin) }
    var meshName by rememberSaveable { mutableStateOf("My mesh") }
    var deviceName by rememberSaveable { mutableStateOf(store.deviceName) }
    var code by rememberSaveable { mutableStateOf("") }
    val scope = rememberCoroutineScope()
    val context = LocalContext.current
    ModalBottomSheet(onDismissRequest = close, sheetState = rememberModalBottomSheetState(skipPartiallyExpanded = true), containerColor = MaterialTheme.colorScheme.background) {
        Column(Modifier.fillMaxWidth().verticalScroll(rememberScrollState()).padding(horizontal = 24.dp).padding(bottom = 32.dp), verticalArrangement = Arrangement.spacedBy(20.dp)) {
            CakeSprinkles()
            Text(if (state?.meshName == null) "Your mesh starts here" else "A familiar way in.", style = MaterialTheme.typography.headlineLarge)

            if (state?.meshName != null) {
                Text(state.meshName, style = MaterialTheme.typography.titleLarge)
                Text("On your other device, choose Join a mesh, then Scan cake QR.")
                if (state.ticket.isNotEmpty()) {
                    CakeInvitationQR(state.ticket)
                    Column(Modifier.fillMaxWidth().background(MaterialTheme.colorScheme.surface, RoundedCornerShape(18.dp)).padding(22.dp), verticalArrangement = Arrangement.spacedBy(14.dp)) {
                        Text("Private invitation", style = MaterialTheme.typography.titleMedium)
                        Text(state.ticket, fontFamily = FontFamily.Monospace, style = MaterialTheme.typography.bodySmall, maxLines = 3, color = MaterialTheme.colorScheme.onSurfaceVariant)
                        Text("One invitation. One new device.", style = MaterialTheme.typography.bodyMedium)
                    }
                    Text("Expires at ${DateFormat.getTimeInstance(DateFormat.SHORT).format(Date(state.expires * 1000))}", style = MaterialTheme.typography.labelSmall)
                    Button(onClick = {
                        val send = Intent(Intent.ACTION_SEND).apply {
                            type = "text/plain"
                            putExtra(Intent.EXTRA_TEXT, state.ticket)
                        }
                        context.startActivity(Intent.createChooser(send, "Share invitation"))
                    }) { Text("Share invitation") }
                    OutlinedButton(onClick = { (context.getSystemService(Context.CLIPBOARD_SERVICE) as ClipboardManager).setPrimaryClip(ClipData.newPlainText("Layercake invitation", state.ticket)) }) { Text("Copy invitation") }
                }
                Button(onClick = { scope.launch { enrollment.perform("invite") } }, enabled = !enrollment.busy && state.pending.isEmpty()) { Text(if (state.ticket.isEmpty()) "Create invitation" else "Replace invitation") }
                Text("Invitations expire after ten minutes. A trusted member must approve each new device.", style = MaterialTheme.typography.bodySmall)
            } else if (state?.joiningName == null) {
                Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                    FilterChip(selected = !join, onClick = { join = false }, label = { Text("Create") })
                    FilterChip(selected = join, onClick = { join = true }, label = { Text("Join") })
                }
                OutlinedTextField(value = deviceName, onValueChange = { if (it.length <= 48) deviceName = it }, label = { Text("This device’s name") }, singleLine = true)
                if (join) {
                    ScanInvitationButton { code = it }
                    OutlinedTextField(value = code, onValueChange = { if (it.length <= 4096) code = it }, label = { Text("Paste invitation") })
                    Text("Paste the full invitation from a trusted device. Any trusted member can approve your request.")
                    Button(onClick = { scope.launch { enrollment.perform("join", "invitation" to code, "device_name" to deviceName) } }, enabled = !enrollment.busy && state != null && code.isNotBlank() && deviceName.isNotBlank()) { Text("Request to join") }
                } else {
                    OutlinedTextField(value = meshName, onValueChange = { if (it.length <= 48) meshName = it }, label = { Text("Mesh name") }, singleLine = true)
                    Text("This device becomes your first trusted member. Then invite your other devices with an invitation.")
                    Button(onClick = { scope.launch {
                        enrollment.perform("create", "name" to meshName, "device_name" to deviceName)
                        if (enrollment.state?.meshName != null) enrollment.perform("invite")
                    } }, enabled = !enrollment.busy && state != null && meshName.isNotBlank() && deviceName.isNotBlank()) { Text("Create mesh") }
                }
            }
            if (enrollment.busy) LinearProgressIndicator(Modifier.fillMaxWidth())
            MembershipSummary(enrollment)
            Text("Pair across networks through Iroh. Keep both apps open until a trusted member approves. Membership is saved on this device.", style = MaterialTheme.typography.bodySmall)
            TextButton(onClick = close, modifier = Modifier.align(Alignment.End)) { Text("Done") }
        }
    }
}
