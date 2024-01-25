import java.util.Base64
import kotlin.ByteArray

private fun hexToBytes(hexString: String): ByteArray {
    return hexString.chunked(2)
        .map { it.toInt(16).toByte() }
        .toByteArray()
}

private fun bytesToHex(bytes: ByteArray): String {
    return bytes.joinToString("") { "%02x".format(it) }
}

private fun xor(a: ByteArray, b: ByteArray): ByteArray {
    val c = ByteArray(maxOf(a.size, b.size))
    for (i in c.indices) {
         c[i] = (a[i].toInt() xor b[i].toInt()).toByte()
    }
    return c
}

fun main() {
    println(
        bytesToHex(xor(
            hexToBytes("1c0111001f010100061a024b53535009181c"),
            hexToBytes("686974207468652062756c6c277320657965"),
        ))
        ==
        "746865206b696420646f6e277420706c6179"
    )
}
