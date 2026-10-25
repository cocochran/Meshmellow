package org.meshthing.mobile

import android.app.Application
import android.content.Context
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.setValue
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import org.json.JSONObject

object EnrollmentNative {
    init { System.loadLibrary("meshthing_enrollment") }
    @JvmStatic external fun initialize(context: Context)
    @JvmStatic external fun command(input: String): String
}

data class NodeAPI(val baseURL: String, val ready: Boolean, val message: String)
data class TrustedMember(val id: String, val name: String, val online: Boolean = false, val api: NodeAPI? = null)
data class JoinRequest(val id: String, val issuer: String, val applicant: String, val name: String)
data class EnrollmentState(
    val id: String, val port: Int, val meshName: String?, val joiningName: String?,
    val inviteID: String, val expires: Long,
    val members: List<TrustedMember>, val pending: List<JoinRequest>, val notice: String,
    val ticket: String = ""
) {
    companion object {
        fun parse(json: JSONObject): EnrollmentState {
            val members = json.getJSONArray("members")
            val pending = json.getJSONArray("pending")
            val invite = json.optJSONObject("invite")
            return EnrollmentState(json.getString("id"), json.getInt("port"), json.optJSONObject("mesh")?.getString("name"), json.optJSONObject("joining")?.getString("name"),
                invite?.getString("id") ?: "", invite?.getLong("expires") ?: 0,
                (0 until members.length()).map { members.getJSONObject(it).let { m -> TrustedMember(m.getString("id"), m.getString("name"), m.optBoolean("online"), m.optJSONObject("api")?.let { a -> NodeAPI(a.getString("base_url"), a.getBoolean("inference_ready"), a.getString("message")) }) } },
                (0 until pending.length()).map { pending.getJSONObject(it).let { p -> JoinRequest(p.getString("id"), p.getString("issuer"), p.getString("applicant"), p.getString("name")) } }, json.getString("notice"), invite?.optString("ticket") ?: "")
        }
    }
}

class EnrollmentController(private val app: Application) {
    var state by mutableStateOf<EnrollmentState?>(null); private set
    var error by mutableStateOf(""); private set
    var discoveryError by mutableStateOf(""); private set
    var busy by mutableStateOf(false); private set
    private var active = false
    private val mutex = Mutex()

    private suspend fun call(command: JSONObject, update: Boolean = true) {
        val response = withContext(Dispatchers.IO) { mutex.withLock { JSONObject(EnrollmentNative.command(command.toString())) } }
        check(response.getBoolean("ok")) { response.optString("error", "Enrollment failed.") }
        if (update && active && response.optJSONObject("state")?.has("id") == true) {
            state = EnrollmentState.parse(response.getJSONObject("state"))
        }
    }
    suspend fun start() {
        active = true
        try {
            EnrollmentNative.initialize(app)
            call(JSONObject().put("op", "open").put("directory", java.io.File(app.noBackupFilesDir, "enrollment").absolutePath))
            error = ""
        } catch (cancelled: CancellationException) { throw cancelled }
        catch (failure: Exception) { error = failure.message ?: "Cannot start enrollment." }
    }
    suspend fun stop() {
        active = false
        call(JSONObject().put("op", "close"), false)
    }
    suspend fun refresh() {
        if (!active || busy || state == null) return
        try {
            call(JSONObject().put("op", "refresh"))
        } catch (cancelled: CancellationException) { throw cancelled }
        catch (failure: Exception) { error = failure.message ?: "Cannot contact the inviting device." }
    }
    suspend fun perform(op: String, vararg values: Pair<String, Any>) {
        if (!active || busy) return
        busy = true
        try {
            val command = JSONObject().put("op", op)
            values.forEach { command.put(it.first, it.second) }
            call(command)
            error = ""
        } catch (cancelled: CancellationException) { throw cancelled }
        catch (failure: Exception) { error = failure.message ?: "Enrollment failed." }
        finally { busy = false }
    }
}
