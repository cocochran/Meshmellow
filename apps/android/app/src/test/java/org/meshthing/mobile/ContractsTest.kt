package org.meshthing.mobile
import org.junit.Assert.*
import org.junit.Test

class ContractsTest {
    @Test fun validatesDashboardOrigins() {
        assertEquals("/api/mesh", Validation.dashboardURL("http://127.0.0.1:3210").path)
        assertEquals("https", Validation.dashboardURL("https://mesh.example").scheme)
        listOf("http://mesh.example", "https://user:password@mesh.example", "https://mesh.example/path", "https://mesh.example?token=value").forEach { value ->
            assertThrows(IllegalArgumentException::class.java) { Validation.dashboardURL(value) }
        }
    }
    @Test fun rejectsBadHeaders() {
        val header = byteArrayOf(0x47, 0x47, 0x55, 0x46, 3, 0, 0, 0) + ByteArray(16)
        assertEquals(3, Validation.ggufVersion(header))
        assertThrows(IllegalArgumentException::class.java) { Validation.ggufVersion(header.copyOf(8)) }
        header[4] = 99
        assertThrows(IllegalArgumentException::class.java) { Validation.ggufVersion(header) }
    }
    @Test fun readsHeadersFromPartialStreamsWithoutReadingModelWeights() {
        val content = byteArrayOf(0x47, 0x47, 0x55, 0x46, 3, 0, 0, 0) + ByteArray(32)
        val stream = object : java.io.ByteArrayInputStream(content) {
            override fun read(buffer: ByteArray, offset: Int, length: Int): Int = super.read(buffer, offset, minOf(length, 1))
        }
        assertEquals(3, Validation.ggufVersion(Validation.readHeader(stream)))
        assertEquals(content.size - 24, stream.available())
    }
    @Test fun unknownBuffersRemainUnknown() {
        val snapshot = MeshSnapshot.parse("""{"connected":true,"active":false,"model":"Synthetic","devices":[{"id":"phone","name":"Phone","hardware":"CPU","begin":0,"end":4,"status":"returned","executions":1,"bufferBytes":null,"computeMs":null}],"decodeRate":null,"message":null}""")
        assertNull(snapshot.devices.first().bufferBytes)
        assertEquals("0–3", snapshot.devices.first().range)
        assertFalse(snapshot.active)
    }
}
