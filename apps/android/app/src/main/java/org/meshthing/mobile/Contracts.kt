package org.meshthing.mobile

import java.net.URI
import java.io.InputStream
import org.json.JSONObject

data class MeshDevice(val id: String, val name: String, val hardware: String, val begin: Int?, val end: Int?, val status: String, val executions: Int, val computeMs: Double?, val bufferBytes: Double?) {
    val range: String get() = if (begin != null && end != null && begin >= 0 && end > begin) "$begin–${end - 1}" else "Unreported"
}
data class MeshSnapshot(val connected: Boolean, val active: Boolean, val model: String, val devices: List<MeshDevice>, val rate: Double?, val message: String?) {
    companion object {
        fun parse(text: String): MeshSnapshot {
            val root = JSONObject(text)
            val stages = root.getJSONArray("devices")
            require(stages.length() <= 64) { "Too many reported stages" }
            return MeshSnapshot(root.getBoolean("connected"), root.getBoolean("active"), root.getString("model"),
                (0 until stages.length()).map { index ->
                    val stage = stages.getJSONObject(index)
                    MeshDevice(stage.getString("id"), stage.getString("name"), stage.getString("hardware"), stage.number("begin")?.toInt(), stage.number("end")?.toInt(), stage.getString("status"), stage.getInt("executions"), stage.number("computeMs"), stage.number("bufferBytes"))
                }, root.number("decodeRate"), if (root.isNull("message")) null else root.getString("message"))
        }
    }
}
private fun JSONObject.number(key: String): Double? = if (isNull(key)) null else getDouble(key).takeIf { it.isFinite() && it >= 0 }

object Validation {
    fun readHeader(input: InputStream): ByteArray {
        val header = ByteArray(24)
        var offset = 0
        while (offset < header.size) {
            val count = input.read(header, offset, header.size - offset)
            if (count < 0) break
            if (count == 0) { val byte = input.read(); if (byte < 0) break; header[offset++] = byte.toByte() }
            else offset += count
        }
        return header.copyOf(offset)
    }
    fun dashboardURL(value: String): URI {
        val uri = URI(value.trim())
        require(!uri.host.isNullOrEmpty() && uri.userInfo == null && uri.rawQuery == null && uri.rawFragment == null && uri.path.orEmpty() in listOf("", "/")) { "Use a dashboard origin without a path or credentials." }
        require(uri.scheme == "https" || (uri.scheme == "http" && uri.host in listOf("localhost", "127.0.0.1"))) { "Use HTTPS, or localhost with a development port forward." }
        return uri.resolve("/api/mesh")
    }
    fun ggufVersion(header: ByteArray): Int {
        require(header.size >= 24 && header.take(4) == listOf<Byte>(0x47, 0x47, 0x55, 0x46)) { "This file does not have a complete GGUF header." }
        val version = (header[4].toInt() and 255) or ((header[5].toInt() and 255) shl 8) or ((header[6].toInt() and 255) shl 16) or ((header[7].toInt() and 255) shl 24)
        require(version in 2..3) { "Only GGUF versions 2 and 3 are recognized." }
        return version
    }
}
