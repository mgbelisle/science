import kotlin.ByteArray

private fun repeatXor(a: ByteArray, key: ByteArray): ByteArray {
    val c = ByteArray(a.size)
    a.forEachIndexed { i, a2 -> c[i] = (a2.toInt() xor key[i % key.size].toInt()).toByte() }
    return c
}

fun main() {
    val plaintext =
            "Burning 'em, if you ain't quick and nimble\nI go crazy when I hear a cymbal".toByteArray()
    val key = "ICE".toByteArray()
    val ciphertext = repeatXor(plaintext, key)
    println(ciphertext.joinToString("") { "%02x".format(it) })
}
