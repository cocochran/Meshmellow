package org.meshmellow.mobile

import android.app.*
import android.content.Context
import android.content.Intent
import android.os.Bundle
import kotlinx.coroutines.*
import java.io.File

class IsolatedBackgroundApplication : MeshmellowApplication() {
    val directory by lazy { File(cacheDir, "background-test-${java.util.UUID.randomUUID()}") }
    override val meshRuntime by lazy { MeshRuntime(this, directory) }
}

/** Real Android service/lifecycle validation against a separate Iroh CLI peer. */
class BackgroundInstrumentation : Instrumentation() {
    private var expectedPeer = ""
    override fun newApplication(cl: ClassLoader, className: String, context: Context): Application =
        Instrumentation.newApplication(IsolatedBackgroundApplication::class.java, context)
    override fun callActivityOnCreate(activity: Activity, state: Bundle?) {
        super.callActivityOnCreate(activity, state)
        // Install flags before onStart so keyguard cannot stop the test's pairing UI.
        if (android.os.Build.VERSION.SDK_INT >= 27) {
            activity.setShowWhenLocked(true)
            activity.setTurnScreenOn(true)
        }
        activity.window.addFlags(android.view.WindowManager.LayoutParams.FLAG_KEEP_SCREEN_ON)
    }
    override fun onCreate(arguments: Bundle?) {
        super.onCreate(arguments)
        expectedPeer = arguments?.getString("expectedPeer").orEmpty()
        start()
    }
    override fun onException(obj: Any?, error: Throwable): Boolean {
        finish(Activity.RESULT_CANCELED, Bundle().apply { putString("error", error.stackTraceToString()) })
        return true
    }
    override fun onStart() {
        Thread {
            val app = targetContext.applicationContext as IsolatedBackgroundApplication
            val runtime = app.meshRuntime
            val result = Bundle()
            var code = Activity.RESULT_CANCELED
            var activity: Activity? = null
            val power = targetContext.getSystemService(android.os.PowerManager::class.java)
            val wasInteractive = power.isInteractive
            fun onMain(action: () -> Unit) = runOnMainSync(action)
            fun powerKey(key: String) {
                uiAutomation.executeShellCommand("input keyevent $key").use { fd ->
                    java.io.FileInputStream(fd.fileDescriptor).readBytes()
                }
            }
            fun launchVisibleTestActivity(): Activity {
                powerKey("KEYCODE_WAKEUP")
                val next = startActivitySync(Intent(targetContext, MainActivity::class.java).addFlags(Intent.FLAG_ACTIVITY_NEW_TASK))
                return next
            }
            fun perform(op: String, vararg fields: Pair<String, Any>) = runBlocking {
                withContext(Dispatchers.Main) { runtime.enrollment.perform(op, *fields) }
            }
            fun eventually(message: String, seconds: Long = 30, condition: () -> Boolean) {
                val end = System.nanoTime() + seconds * 1_000_000_000L
                while (System.nanoTime() < end) {
                    var done = false; onMain { done = condition() }
                    if (done) return
                    Thread.sleep(200)
                }
                error("$message (running=${runtime.running}, background=${runtime.backgroundActive}, core=${runtime.enrollment.error})")
            }
            try {
                check(expectedPeer.matches(Regex("[0-9a-f]{64}")))
                activity = launchVisibleTestActivity()
                eventually("Core did not open") { runtime.running && runtime.enrollment.state != null }
                perform("create", "name" to "Background validation", "device_name" to "Background phone")
                perform("invite")
                val ticket = runtime.enrollment.state!!.ticket
                sendStatus(1, Bundle().apply { putString("ticket", ticket) })
                eventually("Peer did not request trust", 60) { runtime.enrollment.state!!.pending.any { it.applicant == expectedPeer } }
                val pending = runtime.enrollment.state!!.pending.single { it.applicant == expectedPeer }
                perform("decide", "issuer" to pending.issuer, "request" to pending.id, "approve" to true)
                eventually("Peer never connected") { runtime.enrollment.state!!.members.any { it.id == expectedPeer && it.online } }
                onMain { targetContext.startForegroundService(Intent(targetContext, MeshBackgroundService::class.java)) }
                eventually("Foreground service did not start") { runtime.backgroundActive }
                // Finish the actual Activity: only the foreground service may now own the core.
                onMain { activity!!.finish() }
                powerKey("KEYCODE_SLEEP")
                eventually("Screen did not turn off") { !power.isInteractive }
                Thread.sleep(20_000) // More than two seven-second heartbeat expiry windows.
                onMain {
                    check(runtime.running && runtime.backgroundActive) { "Runtime closed with its Activity" }
                    check(runtime.enrollment.state!!.members.any { it.id == expectedPeer && it.online }) { "Peer expired in background" }
                }
                sendStatus(2, Bundle().apply { putString("background", "connected with screen off after Activity finish") })
                // Exercise the notification's Stop action, not just an in-app toggle.
                onMain { targetContext.startService(Intent(targetContext, MeshBackgroundService::class.java).setAction(MeshBackgroundService.STOP)) }
                eventually("Stop did not close the background core") { !runtime.backgroundActive && !runtime.running }
                // Rapid Activity replacement must not let an old cleanup close the new core.
                activity = launchVisibleTestActivity()
                eventually("Saved mesh did not resume") { runtime.running && runtime.enrollment.state?.meshName != null }
                check(!runtime.backgroundActive) { "Background mode silently restarted" }
                onMain { targetContext.startForegroundService(Intent(targetContext, MeshBackgroundService::class.java)) }
                eventually("Service did not restart explicitly") { runtime.backgroundActive }
                perform("leave")
                eventually("Leave did not stop the service") { !runtime.backgroundActive }
                onMain { activity!!.finish() }
                eventually("Core stayed open without owners") { !runtime.running }
                val locks = uiAutomation.executeShellCommand("dumpsys power").use { fd -> java.io.FileInputStream(fd.fileDescriptor).bufferedReader().readText() }
                // dumpsys also retains release history; only inspect currently held locks.
                check(locks.contains("Wake Locks:")) { "Could not inspect active wake locks" }
                val held = locks.substringAfter("Wake Locks:").substringBefore("\n\n")
                check(!held.contains("Meshmellow:MeshConnection")) { "Wake lock leaked after Stop/Leave" }
                code = Activity.RESULT_OK
                result.putString("result", "PASS: real foreground service, Iroh heartbeats with Activity closed and screen off, notification Stop, resume, Leave cleanup, no wake-lock leak")
            } catch (failure: Throwable) { result.putString("error", failure.toString()) }
            finally {
                onMain { targetContext.stopService(Intent(targetContext, MeshBackgroundService::class.java)); activity?.finish() }
                Thread.sleep(1000)
                app.directory.deleteRecursively()
                if (!wasInteractive) powerKey("KEYCODE_SLEEP")
            }
            finish(code, result)
        }.apply { uncaughtExceptionHandler = Thread.UncaughtExceptionHandler { _, error ->
            finish(Activity.RESULT_CANCELED, Bundle().apply { putString("error", error.stackTraceToString()) })
        } }.start()
    }
}
