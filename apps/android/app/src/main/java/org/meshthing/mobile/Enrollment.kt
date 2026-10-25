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
/** A device's own report of its native model: verifying, benchmarking, standby, loading, ready or failed. */
data class ComputeStatus(val phase: String, val message: String, val begin: Int?, val end: Int?) {
    private val range get() = if (begin != null && end != null) "$begin–${end - 1}" else null
    val ready get() = phase == "ready"
    val working get() = phase in setOf("verifying", "benchmarking", "loading")
    val label get() = when (phase) {
        "verifying" -> "Checking model file…"
        "benchmarking" -> "Benchmarking…"
        "standby" -> "Model ready · no layers assigned"
        "loading" -> range?.let { "Loading layers $it…" } ?: "Loading layers…"
        "ready" -> range?.let { "Layers $it loaded" } ?: "Loaded"
        "failed" -> "Model failed to load"
        else -> phase
    }
    companion object {
        fun parse(json: JSONObject?): ComputeStatus? = json?.let {
            ComputeStatus(it.getString("phase"), it.optString("message"),
                if (it.isNull("begin")) null else it.optInt("begin"), if (it.isNull("end")) null else it.optInt("end"))
        }
    }
}
data class TrustedMember(val id: String, val name: String, val online: Boolean = false, val api: NodeAPI? = null, val compute: ComputeStatus? = null)
data class JoinRequest(val id: String, val issuer: String, val applicant: String, val name: String)
data class EnrollmentState(
    val id: String, val port: Int, val meshName: String?, val joiningName: String?,
    val inviteID: String, val expires: Long,
    val members: List<TrustedMember>, val pending: List<JoinRequest>, val notice: String,
    val ticket: String = "",
    val compute: ComputeStatus? = null
) {
    companion object {
        fun parse(json: JSONObject): EnrollmentState {
            val compute = ComputeStatus.parse(json.optJSONObject("inference")?.optJSONObject("compute"))
            val members = json.getJSONArray("members")
            val pending = json.getJSONArray("pending")
            val invite = json.optJSONObject("invite")
            return EnrollmentState(json.getString("id"), json.getInt("port"), json.optJSONObject("mesh")?.getString("name"), json.optJSONObject("joining")?.getString("name"),
                invite?.getString("id") ?: "", invite?.getLong("expires") ?: 0,
                (0 until members.length()).map { members.getJSONObject(it).let { m -> TrustedMember(m.getString("id"), m.getString("name"), m.optBoolean("online"), m.optJSONObject("api")?.let { a -> NodeAPI(a.getString("base_url"), a.getBoolean("inference_ready"), a.getString("message")) }, ComputeStatus.parse(m.optJSONObject("compute"))) } },
                (0 until pending.length()).map { pending.getJSONObject(it).let { p -> JoinRequest(p.getString("id"), p.getString("issuer"), p.getString("applicant"), p.getString("name")) } }, json.getString("notice"), invite?.optString("ticket") ?: "", compute)
        }
    }
}

class EnrollmentController(private val app: Application, private val directory: java.io.File = java.io.File(app.noBackupFilesDir, "enrollment")) {
    var state by mutableStateOf<EnrollmentState?>(null); private set
    var error by mutableStateOf(""); private set
    var discoveryError by mutableStateOf(""); private set
    var busy by mutableStateOf(false); private set
    private var active = false
    val isRunning get() = active
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
            // The model cache supplies the pin and memory policy; the installed
            // executable always comes from this APK, never a peer-supplied path.
            val computeFile = java.io.File(directory, "compute.json")
            if (computeFile.isFile) {
                val compute = JSONObject(computeFile.readText())
                compute.put("runner", java.io.File(app.applicationInfo.nativeLibraryDir, "libmesh_worker.so").absolutePath)
                computeFile.writeText(compute.toString())
            }
            call(JSONObject().put("op", "open").put("directory", directory.absolutePath))
            error = ""
        } catch (cancelled: CancellationException) { throw cancelled }
        catch (failure: Exception) { active = false; state = null; error = failure.message ?: "Cannot start enrollment." }
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
