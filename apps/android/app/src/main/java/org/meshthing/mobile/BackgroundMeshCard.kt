package org.meshthing.mobile

import android.Manifest
import android.content.Intent
import android.os.Build
import android.provider.Settings
import androidx.activity.compose.rememberLauncherForActivityResult
import androidx.activity.result.contract.ActivityResultContracts
import androidx.compose.foundation.layout.*
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.unit.dp
import androidx.core.content.ContextCompat

@Composable fun BackgroundMeshCard(runtime: MeshRuntime) {
    val context = LocalContext.current
    var notice by remember { mutableStateOf("") }
    fun start() {
        try { ContextCompat.startForegroundService(context, Intent(context, MeshBackgroundService::class.java)) }
        catch (failure: Exception) { runtime.backgroundFailed(failure.message ?: "Open Layercake to enable background usage.") }
    }
    val notifications = rememberLauncherForActivityResult(ActivityResultContracts.RequestPermission()) { granted ->
        if (!granted) notice = "Notifications are off. You can stop background usage here or in Android’s Active apps."
        start()
    }
    Card(Modifier.fillMaxWidth()) {
        Column(Modifier.padding(18.dp), verticalArrangement = Arrangement.spacedBy(10.dp)) {
            Row(verticalAlignment = Alignment.CenterVertically) {
                Text("Keep mesh active", Modifier.weight(1f), style = MaterialTheme.typography.titleMedium)
                Switch(checked = runtime.backgroundActive, enabled = runtime.backgroundActive || runtime.enrollment.state?.meshName != null,
                    onCheckedChange = { enabled ->
                        if (!enabled) context.stopService(Intent(context, MeshBackgroundService::class.java))
                        else if (Build.VERSION.SDK_INT >= 33) notifications.launch(Manifest.permission.POST_NOTIFICATIONS)
                        else start()
                    })
            }
            Text(if (runtime.backgroundActive) "Active with the screen off. Stop here or from the notification." else "Stay connected while using other apps or with the screen off.")
            Text("Uses more battery; best while charging. This keeps mesh connections active, not model inference.", style = MaterialTheme.typography.bodySmall)
            if (runtime.enrollment.state?.meshName == null) Text("Create or join a mesh first.", style = MaterialTheme.typography.bodySmall)
            if (runtime.backgroundActive) {
                Text("For long screen-off sessions, allow unrestricted battery usage for Layercake. Android can still stop the app under resource pressure.", style = MaterialTheme.typography.bodySmall)
                OutlinedButton(onClick = {
                    try { context.startActivity(Intent(Settings.ACTION_IGNORE_BATTERY_OPTIMIZATION_SETTINGS)) }
                    catch (_: Exception) { notice = "Open Android Settings → Apps → Layercake → Battery." }
                }) { Text("Battery settings") }
            }
            if (notice.isNotEmpty()) Text(notice, style = MaterialTheme.typography.bodySmall)
            if (runtime.backgroundError.isNotEmpty()) Text(runtime.backgroundError, color = MaterialTheme.colorScheme.error)
        }
    }
}
