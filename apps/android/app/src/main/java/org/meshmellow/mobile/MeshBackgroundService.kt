package org.meshmellow.mobile

import android.app.*
import android.content.Intent
import android.content.pm.ServiceInfo
import android.os.Build
import android.os.IBinder
import android.os.PowerManager
import androidx.core.app.NotificationCompat
import androidx.core.app.ServiceCompat
import kotlinx.coroutines.*

/** User-started peer connectivity, never a camera or an automatically restarted worker. */
class MeshBackgroundService : Service() {
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Main.immediate)
    private val runtime get() = (application as MeshmellowApplication).meshRuntime
    private var awake: PowerManager.WakeLock? = null
    private var monitor: Job? = null

    override fun onBind(intent: Intent?): IBinder? = null
    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        if (intent?.action == STOP) { stopSelf(); return START_NOT_STICKY }
        try {
            val notifications = getSystemService(NotificationManager::class.java)
            notifications.createNotificationChannel(NotificationChannel(CHANNEL, "Active mesh", NotificationManager.IMPORTANCE_LOW).apply {
                description = "Keeps your peer connected when Meshmellow is in the background."
            })
            val open = PendingIntent.getActivity(this, 0, Intent(this, MainActivity::class.java), PendingIntent.FLAG_IMMUTABLE or PendingIntent.FLAG_UPDATE_CURRENT)
            val stop = PendingIntent.getService(this, 1, Intent(this, MeshBackgroundService::class.java).setAction(STOP), PendingIntent.FLAG_IMMUTABLE or PendingIntent.FLAG_UPDATE_CURRENT)
            val notification = NotificationCompat.Builder(this, CHANNEL)
                .setSmallIcon(R.drawable.ic_mesh).setContentTitle("Meshmellow mesh is active")
                .setContentText("Keeping your peer connected. Tap to open Meshmellow.")
                .setContentIntent(open).setOngoing(true).setOnlyAlertOnce(true)
                .setCategory(NotificationCompat.CATEGORY_SERVICE)
                .addAction(0, "Stop background usage", stop).build()
            ServiceCompat.startForeground(this, 41, notification,
                if (Build.VERSION.SDK_INT >= 29) ServiceInfo.FOREGROUND_SERVICE_TYPE_CONNECTED_DEVICE else 0)
            if (runtime.enrollment.state?.meshName == null) { stopSelf(); return START_NOT_STICKY }
            runtime.background(true)
            if (monitor == null) {
                awake = getSystemService(PowerManager::class.java).newWakeLock(PowerManager.PARTIAL_WAKE_LOCK, "Meshmellow:MeshConnection").apply { setReferenceCounted(false) }
                monitor = scope.launch {
                    var nextRenewal = 0L
                    while (isActive) {
                        if (runtime.enrollment.state?.meshName == null) { stopSelf(); break }
                        val now = android.os.SystemClock.elapsedRealtime()
                        if (now >= nextRenewal) {
                            // A timeout bounds a leaked lock; explicit Stop/Leave releases it immediately.
                            awake?.acquire(10 * 60 * 1000L)
                            nextRenewal = now + 5 * 60 * 1000L
                        }
                        delay(1000)
                    }
                }
            }
        } catch (failure: Exception) {
            runtime.backgroundFailed("Could not keep the mesh active: ${failure.message ?: "Android stopped the service"}")
            stopSelf()
        }
        // Respect user/system stops. Reopening the app never silently re-enables this mode.
        return START_NOT_STICKY
    }
    override fun onDestroy() {
        scope.cancel()
        awake?.let { if (it.isHeld) it.release() }
        awake = null
        runtime.background(false)
        stopForeground(STOP_FOREGROUND_REMOVE)
        super.onDestroy()
    }
    override fun onTimeout(startId: Int, fgsType: Int) { stopSelf() }
    companion object {
        const val STOP = "org.meshmellow.mobile.STOP_BACKGROUND"
        private const val CHANNEL = "mesh-background"
    }
}
