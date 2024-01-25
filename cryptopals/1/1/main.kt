import java.util.Base64

private fun hexToBytes(hexString: String): ByteArray {
    return hexString.chunked(2)
        .map { it.toInt(16).toByte() }
        .toByteArray()
}

fun main() {
    println(
        Base64.getEncoder().encodeToString(hexToBytes("49276d206b696c6c696e6720796f757220627261696e206c696b65206120706f69736f6e6f7573206d757368726f6f6d"))
        ==
        "SSdtIGtpbGxpbmcgeW91ciBicmFpbiBsaWtlIGEgcG9pc29ub3VzIG11c2hyb29t"
    )
}
