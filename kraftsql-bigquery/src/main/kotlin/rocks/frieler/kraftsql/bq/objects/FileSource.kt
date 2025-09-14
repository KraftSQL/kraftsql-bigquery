package rocks.frieler.kraftsql.bq.objects

import java.net.URI

class FileSource(
    val uris: List<URI>,
    val format: String,
    val fieldDelimiter: String? = null,
    val quote: Char? = null,
    val skipLeadingRows: Int? = null,
) {
    fun toOptions() = mutableMapOf<String, String>().also { options ->
        options["uris"] = "[${uris.joinToString(",") { "'$it'" }}]"
        options["format"] = "'${format}'"
        if (fieldDelimiter != null) { options["field_delimiter"] = "'${fieldDelimiter}'" }
        if (quote != null) { options["quote"] = "'${quote}'" }
        if (skipLeadingRows != null) { options["skip_leading_rows"] = "$skipLeadingRows"}
    }

    companion object {
        fun csv(uris: List<URI>, fieldDelimiter: String? = null, quote: Char? = null, skipLeadingRows: Int? = null) =
            FileSource(uris, "CSV", fieldDelimiter, quote, skipLeadingRows)
    }
}
