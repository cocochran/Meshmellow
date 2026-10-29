package org.meshmellow.mobile

import android.Manifest
import android.content.pm.PackageManager
import androidx.camera.core.CameraSelector
import androidx.camera.mlkit.vision.MlKitAnalyzer
import androidx.camera.view.CameraController
import androidx.camera.view.LifecycleCameraController
import androidx.camera.view.PreviewView
import androidx.compose.foundation.background
import androidx.compose.foundation.border
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.unit.dp
import androidx.compose.ui.viewinterop.AndroidView
import androidx.compose.ui.window.Dialog
import androidx.compose.ui.window.DialogProperties
import androidx.core.content.ContextCompat
import androidx.lifecycle.compose.LocalLifecycleOwner
import com.google.mlkit.vision.barcode.BarcodeScanning
import com.google.mlkit.vision.barcode.BarcodeScannerOptions
import com.google.mlkit.vision.barcode.ZoomSuggestionOptions
import com.google.mlkit.vision.barcode.common.Barcode

/** CameraX owns rotation, autofocus and preview gestures; ML Kit is bundled offline. */
@Composable fun InvitationScanner(onScan: (String) -> Unit, onClose: () -> Unit) {
    val context = LocalContext.current
    val owner = LocalLifecycleOwner.current
    val onResult by rememberUpdatedState(onScan)
    val controller = remember { LifecycleCameraController(context) }
    var message by remember { mutableStateOf("Point at the invitation QR. We’ll read it automatically.") }
    var torch by remember { mutableStateOf(false) }
    var available by remember { mutableStateOf(false) }
    DisposableEffect(controller, owner) {
        var active = true
        var delivered = false
        val main = ContextCompat.getMainExecutor(context)
        val scanner = BarcodeScanning.getClient(BarcodeScannerOptions.Builder()
            .setBarcodeFormats(Barcode.FORMAT_QR_CODE)
            .setZoomSuggestionOptions(ZoomSuggestionOptions.Builder { ratio ->
                if (!active) false else {
                    val limits = controller.zoomState.value
                    controller.setZoomRatio(ratio.coerceIn(limits?.minZoomRatio ?: 1f, limits?.maxZoomRatio ?: 4f))
                    true
                }
            }.setMaxSupportedZoomRatio(4f).build()).build())
        try {
            check(ContextCompat.checkSelfPermission(context, Manifest.permission.CAMERA) == PackageManager.PERMISSION_GRANTED) { "Allow camera access, or close this and paste your invitation." }
            controller.cameraSelector = CameraSelector.DEFAULT_BACK_CAMERA
            controller.setEnabledUseCases(CameraController.IMAGE_ANALYSIS)
            controller.isTapToFocusEnabled = true
            controller.isPinchToZoomEnabled = true
            controller.setImageAnalysisAnalyzer(main, MlKitAnalyzer(listOf(scanner), CameraController.COORDINATE_SYSTEM_VIEW_REFERENCED, main) { result ->
                if (active && !delivered) {
                    val value = result.getValue(scanner)?.firstOrNull { it.rawValue != null }?.rawValue
                    if (value != null) {
                        if (InvitationQR.accepts(value)) { delivered = true; onResult(value) }
                        else message = "That QR isn’t a Meshmellow invitation. Try the code on a trusted device."
                    }
                }
            })
            controller.bindToLifecycle(owner)
            controller.initializationFuture.addListener({
                if (active) {
                    try { controller.initializationFuture.get(); available = true }
                    catch (_: Exception) { message = "Camera unavailable. Close this and paste your invitation." }
                }
            }, main)
        } catch (failure: Exception) {
            message = failure.message ?: "Camera unavailable. Close this and paste your invitation."
        }
        onDispose {
            active = false
            controller.clearImageAnalysisAnalyzer()
            controller.enableTorch(false)
            controller.unbind()
            scanner.close()
        }
    }
    Dialog(onDismissRequest = onClose, properties = DialogProperties(usePlatformDefaultWidth = false, decorFitsSystemWindows = false)) {
        Column(Modifier.fillMaxSize().background(Color(0xff171311)).systemBarsPadding()) {
            Row(Modifier.fillMaxWidth().padding(horizontal = 16.dp, vertical = 8.dp), verticalAlignment = Alignment.CenterVertically) {
                Text("Scan invitation", Modifier.weight(1f), color = Color.White, style = MaterialTheme.typography.titleLarge)
                TextButton(onClick = onClose) { Text("Close", color = Color.White) }
            }
            Box(Modifier.fillMaxWidth().weight(1f)) {
                AndroidView(factory = { PreviewView(it).apply {
                    implementationMode = PreviewView.ImplementationMode.COMPATIBLE
                    scaleType = PreviewView.ScaleType.FILL_CENTER
                    this.controller = controller
                } }, modifier = Modifier.fillMaxSize())
                Box(Modifier.align(Alignment.Center).fillMaxWidth(0.82f).aspectRatio(1f).border(2.dp, Color(0xfff7eee3).copy(alpha = 0.7f), RoundedCornerShape(24.dp)))
            }
            Column(Modifier.fillMaxWidth().padding(24.dp), horizontalAlignment = Alignment.CenterHorizontally, verticalArrangement = Arrangement.spacedBy(12.dp)) {
                Text(message, color = Color.White, style = MaterialTheme.typography.bodyLarge)
                Text("Tap to focus · Pinch to zoom", color = Color(0xffd5c5b7), style = MaterialTheme.typography.bodyMedium)
                OutlinedButton(onClick = { torch = !torch; controller.enableTorch(torch) }, enabled = available && controller.cameraInfo?.hasFlashUnit() == true) {
                    Text(if (torch) "Turn light off" else "Turn light on", color = Color.White)
                }
            }
        }
    }
}
