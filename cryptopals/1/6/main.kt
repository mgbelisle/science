import java.io.FileReader
import java.util.Base64
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

    // Read 6.txt and base64 decode each line
    val ciphertext =
            FileReader("1/6/6.txt").useLines {
                it.flatMap { Base64.getDecoder().decode(it).asIterable() }.toList().toByteArray()
            }

    val keySizeToNormalizedHammingDistance = mapOf(*(2..40).map { keySize ->
        keySize to hammingDistance(ciphertext.sliceArray(0 until keySize),
                ciphertext.sliceArray(keySize until keySize * 2)) / keySize.toDouble()
    }.toTypedArray())
}
