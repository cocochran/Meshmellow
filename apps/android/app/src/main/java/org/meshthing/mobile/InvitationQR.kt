package org.meshthing.mobile

import android.Manifest
import android.content.Intent
import android.content.pm.PackageManager
import android.graphics.Bitmap
import android.net.Uri
import android.provider.Settings
import androidx.activity.compose.rememberLauncherForActivityResult
import androidx.activity.result.contract.ActivityResultContracts
import androidx.compose.foundation.Image
import androidx.compose.foundation.background
import androidx.compose.foundation.border
import androidx.compose.foundation.layout.*
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.runtime.saveable.rememberSaveable
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.graphics.FilterQuality
import androidx.compose.ui.graphics.asImageBitmap
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.res.painterResource
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.unit.dp
import androidx.core.content.ContextCompat
import com.google.zxing.BarcodeFormat
import com.google.zxing.EncodeHintType
import com.google.zxing.qrcode.QRCodeWriter
import com.google.zxing.qrcode.decoder.ErrorCorrectionLevel

object InvitationQR {
    fun accepts(text: String) = text.toByteArray(Charsets.UTF_8).size <= 4096 && text.startsWith("layercake://join/")
    fun matrix(ticket: String) = QRCodeWriter().encode(ticket, BarcodeFormat.QR_CODE, 720, 720,
        mapOf(EncodeHintType.ERROR_CORRECTION to ErrorCorrectionLevel.M, EncodeHintType.MARGIN to 4))
}

@Composable fun CakeInvitationQR(ticket: String) {
    val bitmap = remember(ticket) {
        val matrix = InvitationQR.matrix(ticket)
        val pixels = IntArray(matrix.width * matrix.height) { i -> if (matrix[i % matrix.width, i / matrix.width]) 0xff35221d.toInt() else android.graphics.Color.WHITE }
        Bitmap.createBitmap(pixels, matrix.width, matrix.height, Bitmap.Config.ARGB_8888).asImageBitmap()
    }
    Column(Modifier.fillMaxWidth(), horizontalAlignment = Alignment.CenterHorizontally) {
        Column(Modifier.widthIn(max = 340.dp).fillMaxWidth().border(4.dp, Color(0xffd6bda1)).background(Color.White)) {
            Row(Modifier.fillMaxWidth().padding(horizontal = 22.dp, vertical = 12.dp), horizontalArrangement = Arrangement.SpaceBetween, verticalAlignment = Alignment.CenterVertically) {
                repeat(2) { Box(Modifier.size(12.dp).background(Color(0xffb83f49))) }
                Image(painterResource(R.drawable.layercake), null, Modifier.size(48.dp))
                repeat(2) { Box(Modifier.size(12.dp).background(Color(0xffb83f49))) }
            }
            Image(bitmap, "Invitation QR code. Scan with Layercake on another device.", Modifier.fillMaxWidth().aspectRatio(1f).padding(horizontal = 8.dp), filterQuality = FilterQuality.None)
            Box(Modifier.fillMaxWidth().height(18.dp).background(Color(0xffbd8057)))
            Box(Modifier.fillMaxWidth().height(8.dp).background(Color(0xfff5d6b8)))
            Box(Modifier.fillMaxWidth().height(9.dp).background(Color(0xffb34a54)))
            Box(Modifier.fillMaxWidth().height(15.dp).background(Color(0xffbd8057)))
        }
        Text("A slice of your mesh.", modifier = Modifier.padding(top = 14.dp), fontFamily = FontFamily.Serif, style = MaterialTheme.typography.headlineSmall)
    }
}

@Composable fun ScanInvitationButton(onScan: (String) -> Unit) {
    val context = LocalContext.current
    var message by rememberSaveable { mutableStateOf("") }
    var denied by rememberSaveable { mutableStateOf(false) }
    var scanning by rememberSaveable { mutableStateOf(false) }
    fun launchScan() { message = ""; scanning = true }
    if (scanning) InvitationScanner(
        onScan = { text -> scanning = false; message = "Invitation scanned. Request to join when you’re ready."; onScan(text) },
        onClose = { scanning = false }
    )
    val permission = rememberLauncherForActivityResult(ActivityResultContracts.RequestPermission()) { granted ->
        denied = !granted
        if (granted) launchScan() else message = "Camera access is off. Enable it in Settings, or paste an invitation below."
    }
    OutlinedButton(onClick = {
        if (ContextCompat.checkSelfPermission(context, Manifest.permission.CAMERA) == PackageManager.PERMISSION_GRANTED) launchScan()
        else permission.launch(Manifest.permission.CAMERA)
    }) { Text("Scan cake QR") }
    if (message.isNotEmpty()) Text(message, style = MaterialTheme.typography.bodySmall)
    if (denied) TextButton(onClick = { context.startActivity(Intent(Settings.ACTION_APPLICATION_DETAILS_SETTINGS, Uri.parse("package:${context.packageName}"))) }) { Text("Camera settings") }
}
