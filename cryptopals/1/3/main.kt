import java.io.FileInputStream
import kotlin.ByteArray

private fun hexToBytes(hexString: String): ByteArray {
    return hexString.chunked(2).map { it.toInt(16).toByte() }.toByteArray()
}

private fun xor(a: ByteArray, b: Byte): ByteArray {
    val c = ByteArray(a.size)
    a.forEachIndexed { i, a2 -> c[i] = (a2.toInt() xor b.toInt()).toByte() }
    return c
}

// Given a bytearray, return a map {byte: count}
fun byteArrayToCharCountMap(byteArray: ByteArray): Map<Byte, Int> {
    return byteArray
            .map { it.toInt().toChar().lowercaseChar().code.toByte() } // Case doesn't matter
            .groupingBy { it }
            .eachCount()
}

// LOTR char count map
val englishCharCountMap =
        FileInputStream("lotr.txt").use { byteArrayToCharCountMap(it.readBytes()) }
val englishCharCountSum = englishCharCountMap.values.sum()

// Given a bytearray, return the amount that it differs from the english char count map
fun englishDiff(byteArray: ByteArray): Double {
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

fun main() {
    val ciphertext =
            hexToBytes("1b37373331363f78151b7f2b783431333d78397828372d363c78373e783a393b3736")
    var plaintext = ByteArray(0)
    var diff = Double.MAX_VALUE
    for (key in Byte.MIN_VALUE..Byte.MAX_VALUE) {
        val plaintext2 = xor(ciphertext, key.toByte())
        val diff2 = englishDiff(plaintext2)
        if (diff2 < diff) {
            diff = diff2
            plaintext = plaintext2
        }
        println(diff)
    }
    System.out.write(plaintext)
}
