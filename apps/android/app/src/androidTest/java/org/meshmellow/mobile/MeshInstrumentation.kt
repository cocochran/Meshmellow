package org.meshmellow.mobile

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
                // Decode the real signed invitation with the bundled production ML Kit engine.
                val ticket = invitation.getString("ticket")
                val matrix = InvitationQR.matrix(ticket)
                val pixels = IntArray(matrix.width * matrix.height) { i -> if (matrix[i % matrix.width, i / matrix.width]) 0xff35221d.toInt() else android.graphics.Color.WHITE }
                val bitmap = android.graphics.Bitmap.createBitmap(pixels, matrix.width, matrix.height, android.graphics.Bitmap.Config.ARGB_8888)
                val scanner = com.google.mlkit.vision.barcode.BarcodeScanning.getClient(
                    com.google.mlkit.vision.barcode.BarcodeScannerOptions.Builder().setBarcodeFormats(com.google.mlkit.vision.barcode.common.Barcode.FORMAT_QR_CODE).build())
                try {
                    for (size in listOf(720, 360)) {
                        val scaled = android.graphics.Bitmap.createScaledBitmap(bitmap, size, size, false)
                        val codes = com.google.android.gms.tasks.Tasks.await(scanner.process(com.google.mlkit.vision.common.InputImage.fromBitmap(scaled, 0)), 15, java.util.concurrent.TimeUnit.SECONDS)
                        check(codes.any { it.rawValue == ticket }) { "ML Kit could not decode the signed invitation at $size pixels" }
                    }
                } finally { scanner.close() }
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
                check(command("leave").isNull("mesh")) { "Leave did not clear membership" }
                command("close")
                val left = command("open", "directory" to state.absolutePath)
                check(left.isNull("mesh") && left.getJSONArray("members").length() == 0 && left.getString("id") == identity) { "Leave did not survive restart" }
                resultCode = Activity.RESULT_OK
                result.putString("result", "PASS: ML Kit invitation decoding at 720/360px, physical Android create, signed ticket, Iroh join, explicit approval, core connection, persisted identity, leave and restart")
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
