package org.meshthing.mobile

import android.app.Activity
import android.app.Instrumentation
import android.os.Bundle
import org.json.JSONObject
import java.io.File

/** Real-device core integration. Uses an isolated temporary identity, never app membership. */
class MeshInstrumentation : Instrumentation() {
    private var expectedPeer = ""
    override fun onCreate(arguments: Bundle?) {
        super.onCreate(arguments)
        expectedPeer = arguments?.getString("expectedPeer").orEmpty()
        start()
    }
    override fun onStart() {
        Thread {
            val state = File(targetContext.cacheDir, "mesh-validation-${java.util.UUID.randomUUID()}")
            fun command(op: String, vararg fields: Pair<String, Any>): JSONObject {
                val input = JSONObject().put("op", op)
                fields.forEach { input.put(it.first, it.second) }
                val result = JSONObject(EnrollmentNative.command(input.toString()))
                check(result.getBoolean("ok")) { result.optString("error") }
                return result.getJSONObject("state")
            }
            var resultCode = Activity.RESULT_CANCELED
            val result = Bundle()
            try {
                check(expectedPeer.matches(Regex("[0-9a-f]{64}"))) { "Expected the companion core's public endpoint ID" }
                EnrollmentNative.initialize(targetContext)
                val initial = command("open", "directory" to state.absolutePath)
                val identity = initial.getString("id")
                command("create", "name" to "Hardware validation", "device_name" to "Pixel validation")
                val invitation = command("invite").getJSONObject("invite")
                sendStatus(1, Bundle().apply { putString("ticket", invitation.getString("ticket")) })
                val deadline = System.nanoTime() + 120_000_000_000L
                var approved = false
                var connected = false
                while (System.nanoTime() < deadline) {
                    val snapshot = command("refresh")
                    val requests = snapshot.getJSONArray("pending")
                    for (i in 0 until requests.length()) {
                        val request = requests.getJSONObject(i)
                        check(request.getString("applicant") == expectedPeer) { "Unexpected applicant" }
                        command("decide", "issuer" to request.getString("issuer"), "request" to request.getString("id"), "approve" to true)
                        approved = true
                    }
                    if (approved && snapshot.getJSONArray("connected_peers").toString().contains(expectedPeer)) { connected = true; break }
                    Thread.sleep(250)
                }
                check(approved && connected) { "Trusted core control connection was not established" }
                command("close")
                val restored = command("open", "directory" to state.absolutePath)
                check(restored.getString("id") == identity && restored.getJSONArray("members").length() == 2) { "Membership did not survive core restart" }
                resultCode = Activity.RESULT_OK
                result.putString("result", "PASS: physical Android create, signed ticket, Iroh join, explicit approval, core connection, persisted identity")
            } catch (failure: Throwable) {
                result.putString("error", failure.toString())
            } finally {
                runCatching { command("close") }
                state.deleteRecursively()
            }
            finish(resultCode, result)
        }.start()
    }
}
