package eu.kanade.tachiyomi.data.ocr

import chimahon.ocr.EngineLine
import chimahon.ocr.NormalizedBBox
import chimahon.ocr.OcrLanguage
import chimahon.ocr.OcrEngine
import chimahon.ocr.WritingDirection
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.Response
import okhttp3.WebSocket
import okhttp3.WebSocketListener
import okio.ByteString
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

class OwOcrClient(
    private val httpClient: OkHttpClient,
    private val urlProvider: () -> String,
) : OcrEngine {
    override val name: String = "OWOCR"

    private val json = Json { ignoreUnknownKeys = true }

    override suspend fun recognize(bytes: ByteArray, language: OcrLanguage): List<EngineLine> =
        withContext(Dispatchers.IO) {
            val endpoint = normalizeUrl(urlProvider())
            require(endpoint.isNotEmpty()) { "OWOCR URL is not configured" }
            val response = withTimeout(60_000L) {
                request(endpoint, bytes)
            }
            response.toEngineLines(language)
        }

    private suspend fun request(endpoint: String, bytes: ByteArray): OwOcrResponse =
        suspendCancellableCoroutine { continuation ->
            var acknowledged = false
            val socket = httpClient.newWebSocket(
                Request.Builder().url(endpoint).build(),
                object : WebSocketListener() {
                    override fun onOpen(webSocket: WebSocket, response: Response) {
                        webSocket.send(ByteString.of(*bytes))
                    }

                    override fun onMessage(webSocket: WebSocket, text: String) {
                        if (!acknowledged && (text == "True" || text == "False")) {
                            acknowledged = true
                            if (text == "False") {
                                continuation.resumeWithException(
                                    IllegalStateException("OWOCR server rejected the image"),
                                )
                                webSocket.cancel()
                            }
                            return
                        }
                        try {
                            continuation.resume(json.decodeFromString<OwOcrResponse>(text))
                            webSocket.close(1000, "OCR complete")
                        } catch (error: Exception) {
                            continuation.resumeWithException(error)
                            webSocket.cancel()
                        }
                    }

                    override fun onFailure(webSocket: WebSocket, throwable: Throwable, response: Response?) {
                        if (continuation.isActive) continuation.resumeWithException(throwable)
                    }
                },
            )
            continuation.invokeOnCancellation { socket.cancel() }
        }

    private fun OwOcrResponse.toEngineLines(language: OcrLanguage): List<EngineLine> =
        paragraphs.flatMap { paragraph ->
            paragraph.lines.mapNotNull { line ->
                val text = line.text?.takeIf { it.isNotBlank() } ?: return@mapNotNull null
                val box = line.boundingBox ?: paragraph.boundingBox ?: return@mapNotNull null
                val left = box.centerX - box.width / 2.0
                val top = box.centerY - box.height / 2.0
                EngineLine(
                    text = text,
                    bbox = NormalizedBBox(
                        left = left,
                        top = top,
                        right = box.centerX + box.width / 2.0,
                        bottom = box.centerY + box.height / 2.0,
                        rotation = box.rotationZ ?: 0.0,
                    ),
                    writingDirection = line.writingDirection.toWritingDirection(),
                    language = language,
                )
            }
        }

    private fun String?.toWritingDirection(): WritingDirection? = when (this) {
        "LEFT_TO_RIGHT" -> WritingDirection.LTR
        "RIGHT_TO_LEFT" -> WritingDirection.RTL
        "TOP_TO_BOTTOM" -> WritingDirection.TTB
        else -> null
    }

    companion object {
        private fun normalizeUrl(value: String): String = value.trim().let {
            when {
                it.startsWith("ws://") || it.startsWith("wss://") -> it
                it.startsWith("https://") -> "wss://${it.removePrefix("https://")}"
                it.startsWith("http://") -> "ws://${it.removePrefix("http://")}"
                it.isEmpty() -> ""
                else -> "ws://$it"
            }
        }
    }
}

@Serializable
private data class OwOcrResponse(
    val paragraphs: List<OwOcrParagraph> = emptyList(),
)

@Serializable
private data class OwOcrParagraph(
    @SerialName("bounding_box") val boundingBox: OwOcrBoundingBox? = null,
    val lines: List<OwOcrLine> = emptyList(),
)

@Serializable
private data class OwOcrLine(
    @SerialName("bounding_box") val boundingBox: OwOcrBoundingBox? = null,
    val text: String? = null,
    @SerialName("writing_direction") val writingDirection: String? = null,
)

@Serializable
private data class OwOcrBoundingBox(
    @SerialName("center_x") val centerX: Double,
    @SerialName("center_y") val centerY: Double,
    val width: Double,
    val height: Double,
    @SerialName("rotation_z") val rotationZ: Double? = null,
)
