import kotlin.ByteArray

// Hamming distance between two bytearrays
private fun hammingDistance(a: ByteArray, b: ByteArray): Int {
    var d = 0
    for (i in a.indices) {
        var x = a[i].toInt() xor b[i].toInt()
        while (x > 0) {
            d += x and 1
            x = x shr 1
        }
    }
    return d
}

fun main() {
    // Assert that the hamming distance between two strings is as expected
    if (hammingDistance("this is a test".toByteArray(), "wokka wokka!!!".toByteArray()) != 37) {
        throw AssertionError("hammingDistance failed")
    }
}
