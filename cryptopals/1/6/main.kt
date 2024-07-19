import java.io.FileInputStream
import java.io.FileReader
import java.util.Base64
import kotlin.ByteArray
import kotlin.system.exitProcess

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

private fun xor(a: ByteArray, b: Byte): ByteArray {
    val c = ByteArray(a.size)
    a.forEachIndexed { i, a2 -> c[i] = (a2.toInt() xor b.toInt()).toByte() }
    return c
}

private fun repeatXor(a: ByteArray, key: ByteArray): ByteArray {
    val c = ByteArray(a.size)
    a.forEachIndexed { i, a2 -> c[i] = (a2.toInt() xor key[i % key.size].toInt()).toByte() }
    return c
}

// Given a bytearray, return a map {byte: count}
private fun byteArrayToCharCountMap(byteArray: ByteArray): Map<Byte, Int> {
    return byteArray
            .map { it.toInt().toChar().lowercaseChar().code.toByte() } // Case doesn't matter
            .groupingBy { it }
            .eachCount()
}

// LOTR char count map
private val englishCharCountMap =
        FileInputStream("lotr.txt").use { byteArrayToCharCountMap(it.readBytes()) }
private val englishCharCountSum = englishCharCountMap.values.sum()

// Given a bytearray, return the amount that it differs from the english char count map
private fun englishDiff(byteArray: ByteArray): Double {
    val map = byteArrayToCharCountMap(byteArray)
    val sum = map.values.sum()
    return map
            .map { (letter, count) ->
                Math.abs(
                        (count.toDouble() / sum) -
                                (englishCharCountMap[letter] ?: 0).toDouble() / englishCharCountSum
                )
            }
            .sum()
}

private fun bestKey(block: ByteArray): Byte {
    var key = 0.toByte()
    var diff = Double.MAX_VALUE
    for (key2 in Byte.MIN_VALUE..Byte.MAX_VALUE) {
        val plaintext2 = xor(block, key2.toByte())
        val diff2 = englishDiff(plaintext2)
        if (diff2 < diff) {
            diff = diff2
            key = key2.toByte()
        }
    }
    return key
}

private fun keySizeHammingDistance(ciphertext: ByteArray, keySize: Int): Double {
    val pairs = ciphertext.asList().chunked(keySize).map { it.toByteArray() }.zipWithNext()
    return pairs.sumOf {
        val minLength = minOf(it.first.size, it.second.size)
        hammingDistance(
                        it.first.copyOfRange(0, minLength),
                        it.second.copyOfRange(0, minLength),
                )
                .toDouble() / keySize
    } / pairs.size
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

    val keySize = (2..40).asSequence().minByOrNull { keySizeHammingDistance(ciphertext, it) }!!
    val blocks = ciphertext.asList().chunked(keySize).map { it.toByteArray() }
    val transposed = (0..keySize-1).map { i -> blocks.mapNotNull { it.getOrNull(i) }.toByteArray() }
    val key = transposed.map { bestKey(it) }.toByteArray()
    System.out.write(repeatXor(ciphertext, key))
}
