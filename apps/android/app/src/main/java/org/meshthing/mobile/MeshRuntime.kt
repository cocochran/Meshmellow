package org.meshthing.mobile

import android.app.Application
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.setValue
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.collectLatest

open class LayercakeApplication : Application() {
    open val meshRuntime by lazy { MeshRuntime(this) }
}

/** One native core owner for both visible activities and the optional foreground service. */
class MeshRuntime(app: Application, directory: java.io.File = java.io.File(app.noBackupFilesDir, "enrollment")) {
    val enrollment = EnrollmentController(app, directory)
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Main.immediate)
    private val wanted = MutableStateFlow(false)
    private val visibleOwners = mutableSetOf<Any>()
    var backgroundActive by mutableStateOf(false); private set
    var backgroundError by mutableStateOf(""); private set
    var running by mutableStateOf(false); private set

    init {
        scope.launch {
            // collectLatest awaits the old owner's cleanup before opening a new endpoint.
            wanted.collectLatest { active ->
                if (active) {
                    try {
                        enrollment.start()
                        running = enrollment.isRunning
                        while (isActive) { enrollment.refresh(); delay(3000) }
                    } finally {
                        running = false
                        withContext(NonCancellable) { runCatching { enrollment.stop() }.onFailure { backgroundFailed("Could not close the mesh: ${it.message}") } }
                    }
                }
            }
        }
    }
    fun visible(owner: Any, visible: Boolean) {
        if (visible) visibleOwners.add(owner) else visibleOwners.remove(owner)
        reconcile()
    }
    fun background(active: Boolean) {
        backgroundActive = active
        if (active) backgroundError = ""
        reconcile()
    }
    fun backgroundFailed(message: String) { backgroundError = message }
    private fun reconcile() { wanted.value = visibleOwners.isNotEmpty() || backgroundActive }
}
