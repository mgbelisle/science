import java.util.Base64

fun hexToBase64(hexString: String): String {
    val bytes = hexString.chunked(2)
        .map { it.toInt(16).toByte() }
        .toByteArray()
    return Base64.getEncoder().encodeToString(bytes)
}

fun main() {
    val hexString = "49276d206b696c6c696e6720796f757220627261696e206c696b65206120706f69736f6e6f7573206d757368726f6f6d"
    val base64String = hexToBase64(hexString)
    println(base64String == "SSdtIGtpbGxpbmcgeW91ciBicmFpbiBsaWtlIGEgcG9pc29ub3VzIG11c2hyb29t")
}
