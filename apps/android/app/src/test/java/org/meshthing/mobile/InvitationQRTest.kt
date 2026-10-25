package org.meshthing.mobile

import com.google.zxing.BinaryBitmap
import com.google.zxing.RGBLuminanceSource
import com.google.zxing.common.HybridBinarizer
import com.google.zxing.qrcode.QRCodeReader
import org.junit.Assert.*
import org.junit.Test

class InvitationQRTest {
    @Test fun denseFullInvitationRoundTripsWithCocoaModulesAndQuietZone() {
        val ticket = "layercake://join/" + "AbC123_-".repeat(100)
        val matrix = InvitationQR.matrix(ticket)
        val pixels = IntArray(matrix.width * matrix.height) { i -> if (matrix[i % matrix.width, i / matrix.width]) 0xff35221d.toInt() else 0xffffffff.toInt() }
        val image = BinaryBitmap(HybridBinarizer(RGBLuminanceSource(matrix.width, matrix.height, pixels)))
        assertEquals(ticket, QRCodeReader().decode(image).text)
        assertTrue(InvitationQR.accepts(ticket))
        assertFalse(InvitationQR.accepts("https://example.com"))
        assertFalse(InvitationQR.accepts("layercake://join/" + "x".repeat(4096)))
    }
}
