package org.meshthing.mobile

import android.app.Application
import android.content.Intent
import android.net.Uri
import android.provider.OpenableColumns
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.setValue
import androidx.lifecycle.AndroidViewModel
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.withContext
import java.io.ByteArrayOutputStream
import java.net.HttpURLConnection

class MeshStore(app: Application) : AndroidViewModel(app) {
    val enrollment = EnrollmentController(app)
    private val preferences = app.getSharedPreferences("meshthing", 0)
    var endpoint by mutableStateOf(preferences.getString("dashboard", "")!!); private set
    var deviceName by mutableStateOf(preferences.getString("deviceName", "My device")!!); private set
    var budgetMiB by mutableStateOf(preferences.getInt("budget", 1024)); private set
    var powerOnly by mutableStateOf(preferences.getBoolean("powerOnly", true)); private set
    var snapshot by mutableStateOf<MeshSnapshot?>(null); private set
    var refreshing by mutableStateOf(false); private set
    var error by mutableStateOf(""); private set
    var modelName by mutableStateOf(preferences.getString("modelName", "")!!); private set
    var modelBytes by mutableStateOf(preferences.getLong("modelBytes", 0)); private set
    var modelNote by mutableStateOf("Runtime validation pending. No model is loaded by this app."); private set
    var selectingModel by mutableStateOf(false); private set
    val connected get() = error.isEmpty() && snapshot?.connected == true

    fun saveEndpoint(value: String) {
        val trimmed = value.trim()
        if (trimmed.isNotEmpty()) Validation.dashboardURL(trimmed)
        preferences.edit().putString("dashboard", trimmed).apply()
        endpoint = trimmed; snapshot = null; error = ""
    }
    fun saveDevice(name: String, budget: Int, power: Boolean) {
        require(name.trim().isNotEmpty() && name.length <= 48 && budget in listOf(512, 1024, 2048, 4096)) { "Use a name of 1–48 characters and a supported memory preference." }
        preferences.edit().putString("deviceName", name.trim()).putInt("budget", budget).putBoolean("powerOnly", power).apply()
        deviceName = name.trim(); budgetMiB = budget; powerOnly = power
    }
    suspend fun refresh() {
        if (endpoint.isEmpty() || refreshing) return
        val requestedEndpoint = endpoint
        refreshing = true
        try {
            val next = withContext(Dispatchers.IO) {
                val connection = Validation.dashboardURL(requestedEndpoint).toURL().openConnection() as HttpURLConnection
                connection.connectTimeout = 4000; connection.readTimeout = 4000; connection.instanceFollowRedirects = false
                try {
                    require(connection.responseCode == 200) { "Dashboard unavailable" }
                    val body = ByteArrayOutputStream()
                    connection.inputStream.use { input ->
                        val buffer = ByteArray(8192)
                        while (true) {
                            currentCoroutineContext().ensureActive()
                            val count = input.read(buffer)
                            if (count == -1) break
                            require(body.size() + count <= 1_048_576) { "Response too large" }
                            body.write(buffer, 0, count)
                        }
                    }
                    MeshSnapshot.parse(body.toString(Charsets.UTF_8.name()))
                } finally { connection.disconnect() }
            }
            if (requestedEndpoint == endpoint) { snapshot = next; error = "" }
        } catch (cancelled: CancellationException) { throw cancelled }
        catch (_: Exception) { if (requestedEndpoint == endpoint) error = "Cannot read the dashboard. Check the address and connection." }
        finally { refreshing = false }
    }
    suspend fun selectModel(uri: Uri) {
        if (selectingModel) return
        selectingModel = true
        try {
            require(modelName.isEmpty()) { "Remove the current selection first. One model at a time." }
            val resolver = getApplication<Application>().contentResolver
            val info = withContext(Dispatchers.IO) {
                val version = resolver.openInputStream(uri)?.use { Validation.ggufVersion(Validation.readHeader(it)) } ?: error("File cannot be read")
                var name = "Selected GGUF"
                var size = 0L
                resolver.query(uri, arrayOf(OpenableColumns.DISPLAY_NAME, OpenableColumns.SIZE), null, null, null)?.use { cursor ->
                    if (cursor.moveToFirst()) {
                        name = cursor.getString(0) ?: name
                        size = if (cursor.isNull(1)) 0L else cursor.getLong(1)
                    }
                }
                Triple(name, size, version)
            }
            resolver.takePersistableUriPermission(uri, Intent.FLAG_GRANT_READ_URI_PERMISSION)
            preferences.edit().putString("modelUri", uri.toString()).putString("modelName", info.first).putLong("modelBytes", info.second).apply()
            modelName = info.first; modelBytes = info.second
            modelNote = "GGUF v${info.third} header recognized. Full integrity and runtime support are not verified."
        } finally { selectingModel = false }
    }
    suspend fun checkModel() {
        try {
            val uri = Uri.parse(preferences.getString("modelUri", ""))
            val version = withContext(Dispatchers.IO) {
                getApplication<Application>().contentResolver.openInputStream(uri)?.use { Validation.ggufVersion(Validation.readHeader(it)) } ?: error("File cannot be read")
            }
            modelNote = "File accessible · GGUF v$version. Runtime validation pending."
        } catch (cancelled: CancellationException) { throw cancelled }
        catch (_: Exception) { modelNote = "File unavailable or invalid. Remove the selection and choose it again." }
    }
    fun removeModel() {
        val uri = preferences.getString("modelUri", null)
        if (uri != null) runCatching { getApplication<Application>().contentResolver.releasePersistableUriPermission(Uri.parse(uri), Intent.FLAG_GRANT_READ_URI_PERMISSION) }
        preferences.edit().remove("modelUri").remove("modelName").remove("modelBytes").apply()
        modelName = ""; modelBytes = 0
    }
}
