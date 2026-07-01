/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.kafka;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Decodes Avro-encoded Kafka message payloads to JSON bytes for downstream mapper processing.
 *
 * <p>Configuration is read from {@code avro.*} keys in the ingestion_source {@code param} map:
 * <ul>
 *   <li>{@code avro.skip_bytes} — bytes to strip from the start of each message (default 0;
 *       use 5 for Confluent wire format, or the appropriate value for other framing headers)</li>
 *   <li>{@code avro.schema_registry_url} — URL to fetch the Avro schema from</li>
 *   <li>{@code avro.schema_registry_headers.<name>} — HTTP headers for the registry request</li>
 *   <li>{@code avro.schema_registry_connect_timeout_ms} — connect timeout for schema registry
 *       HTTP requests in milliseconds (default 10000)</li>
 *   <li>{@code avro.schema_registry_request_timeout_ms} — request timeout for schema registry
 *       HTTP requests in milliseconds (default 10000)</li>
 *   <li>{@code avro.schema} — inline Avro JSON schema (alternative to registry URL)</li>
 *   <li>{@code avro.wrapper_schema} — outer envelope Avro schema with a placeholder field;
 *       must be set together with {@code avro.wrapper_field}</li>
 *   <li>{@code avro.wrapper_field} — field in the wrapper schema to substitute the inner
 *       schema into; must be set together with {@code avro.wrapper_schema}</li>
 *   <li>{@code avro.msg_field} — field in the decoded record to use as the document source</li>
 * </ul>
 *
 * <p>If no {@code avro.*} params are present, use {@link KafkaPayloadDecoder#PASSTHROUGH} instead.
 *
 * <p><b>Shadow-plugin note:</b> this class imports {@code org.apache.avro.*} which is provided
 * at compile time by the {@code compileOnly} dependency.  The shadow plugin rewrites every
 * {@code org.apache.avro.*} bytecode reference to {@code org.opensearch.plugin.kafka.shaded.avro.*}
 * at JAR-build time, so at runtime the class names match the relocated classes embedded in the
 * shadow JAR.  Avro must not be added as a non-{@code compileOnly}/{@code avroShade} dependency
 * or the rewriting step will be skipped and the plugin will fail at runtime.
 */
public class AvroPayloadDecoder implements KafkaPayloadDecoder {

    private static final Logger logger = LogManager.getLogger(AvroPayloadDecoder.class);

    static final String AVRO_PREFIX = "avro.";
    static final String PARAM_SKIP_BYTES = "avro.skip_bytes";
    static final String PARAM_SCHEMA = "avro.schema";
    static final String PARAM_SCHEMA_REGISTRY_URL = "avro.schema_registry_url";
    static final String PARAM_SCHEMA_REGISTRY_HEADERS_PREFIX = "avro.schema_registry_headers.";
    static final String PARAM_SCHEMA_REGISTRY_CONNECT_TIMEOUT_MS = "avro.schema_registry_connect_timeout_ms";
    static final String PARAM_SCHEMA_REGISTRY_REQUEST_TIMEOUT_MS = "avro.schema_registry_request_timeout_ms";
    static final String PARAM_WRAPPER_SCHEMA = "avro.wrapper_schema";
    static final String PARAM_WRAPPER_FIELD = "avro.wrapper_field";
    static final String PARAM_MSG_FIELD = "avro.msg_field";

    private static final int DEFAULT_REGISTRY_TIMEOUT_MS = 10_000;
    private static final int MAX_ERROR_BODY_LENGTH = 512;

    private final Schema schema;
    private final int skipBytes;
    private final String msgField;
    private final GenericDatumReader<GenericRecord> reader;

    /**
     * Constructs an {@code AvroPayloadDecoder} from the given {@code avro.*} parameters.
     *
     * @param params map of avro.* configuration keys to values
     */
    public AvroPayloadDecoder(Map<String, Object> params) {
        int rawSkipBytes = params.containsKey(PARAM_SKIP_BYTES)
            ? Integer.parseInt(String.valueOf(params.get(PARAM_SKIP_BYTES)))
            : 0;
        if (rawSkipBytes < 0) {
            throw new IllegalArgumentException("[avro.skip_bytes] must be non-negative, got: " + rawSkipBytes);
        }
        this.skipBytes = rawSkipBytes;
        this.msgField = params.containsKey(PARAM_MSG_FIELD) ? String.valueOf(params.get(PARAM_MSG_FIELD)) : null;

        String registryUrl = params.containsKey(PARAM_SCHEMA_REGISTRY_URL)
            ? String.valueOf(params.get(PARAM_SCHEMA_REGISTRY_URL))
            : null;

        // avro.schema value may arrive as a String or as a parsed Map if OpenSearch deserialized it
        String inlineSchema = null;
        if (params.containsKey(PARAM_SCHEMA)) {
            Object schemaVal = params.get(PARAM_SCHEMA);
            if (schemaVal instanceof String) {
                inlineSchema = (String) schemaVal;
            } else if (schemaVal instanceof Map) {
                // OpenSearch parsed the JSON string into a Map — convert back to JSON
                inlineSchema = toJsonString(schemaVal);
            } else if (schemaVal != null) {
                inlineSchema = String.valueOf(schemaVal);
            }
        }

        String wrapperSchemaJson = params.containsKey(PARAM_WRAPPER_SCHEMA)
            ? String.valueOf(params.get(PARAM_WRAPPER_SCHEMA))
            : null;
        String wrapperField = params.containsKey(PARAM_WRAPPER_FIELD) ? String.valueOf(params.get(PARAM_WRAPPER_FIELD)) : null;

        if (registryUrl == null && inlineSchema == null) {
            throw new IllegalArgumentException(
                "avro params present but neither [avro.schema_registry_url] nor [avro.schema] is configured"
            );
        }
        if ((wrapperSchemaJson == null) != (wrapperField == null)) {
            throw new IllegalArgumentException(
                "[avro.wrapper_schema] and [avro.wrapper_field] must both be set or both be absent"
            );
        }

        Schema innerSchema;
        if (registryUrl != null) {
            Map<String, String> headers = resolveHeaders(params);
            int connectTimeoutMs = params.containsKey(PARAM_SCHEMA_REGISTRY_CONNECT_TIMEOUT_MS)
                ? Integer.parseInt(String.valueOf(params.get(PARAM_SCHEMA_REGISTRY_CONNECT_TIMEOUT_MS)))
                : DEFAULT_REGISTRY_TIMEOUT_MS;
            int requestTimeoutMs = params.containsKey(PARAM_SCHEMA_REGISTRY_REQUEST_TIMEOUT_MS)
                ? Integer.parseInt(String.valueOf(params.get(PARAM_SCHEMA_REGISTRY_REQUEST_TIMEOUT_MS)))
                : DEFAULT_REGISTRY_TIMEOUT_MS;
            logger.info("AvroPayloadDecoder: fetching schema from [{}] headers={}", registryUrl, headers.keySet());
            innerSchema = fetchSchema(registryUrl, headers, connectTimeoutMs, requestTimeoutMs);
            logger.info("AvroPayloadDecoder: fetched schema [{}]", innerSchema.getFullName());
        } else {
            try {
                innerSchema = new Schema.Parser().parse(inlineSchema);
                logger.debug("AvroPayloadDecoder: parsed inline schema [{}]", innerSchema.getFullName());
            } catch (Throwable t) {
                logger.error("AvroPayloadDecoder: inline schema parse FAILED: {}", t.getMessage(), t);
                throw new IllegalArgumentException("Failed to parse avro.schema: " + t.getMessage(), t);
            }
        }

        if (wrapperSchemaJson != null) {
            Schema wrapper = new Schema.Parser().parse(wrapperSchemaJson);
            this.schema = substituteInnerSchema(wrapper, innerSchema, wrapperField);
            logger.info(
                "AvroPayloadDecoder: combined schema [{}] fields={}",
                this.schema.getFullName(),
                this.schema.getFields().stream().map(Schema.Field::name).toList()
            );
        } else {
            this.schema = innerSchema;
        }
        this.reader = new GenericDatumReader<>(this.schema);
        logger.info(
            "AvroPayloadDecoder: initialized — schema=[{}] skipBytes={} msgField={}",
            this.schema.getFullName(),
            this.skipBytes,
            this.msgField
        );
    }

    /**
     * Decodes raw Kafka message bytes: strips the configured header, deserializes Avro binary,
     * optionally extracts a nested envelope field, and returns the result as JSON bytes.
     * Returns {@code null} for tombstone records (null or empty payload after skip).
     */
    @Override
    @SuppressWarnings("unchecked")
    public byte[] decode(byte[] raw) {
        if (raw == null) {
            return null;
        }
        int remaining = raw.length - skipBytes;
        if (remaining < 1) {
            throw new IllegalArgumentException(
                "Avro payload too short after skipping " + skipBytes + " bytes (" + remaining + " remaining)"
            );
        }

        if (logger.isDebugEnabled()) {
            int previewLen = Math.min(remaining, 16);
            StringBuilder hex = new StringBuilder();
            for (int i = 0; i < previewLen; i++) {
                hex.append(String.format("%02x ", raw[skipBytes + i]));
            }
            logger.debug(
                "Decoding Avro payload: total={} skip={} remaining={} first_bytes=[{}]",
                raw.length,
                skipBytes,
                remaining,
                hex.toString().trim()
            );
        }

        try {
            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(raw, skipBytes, remaining), null);
            GenericRecord record = reader.read(null, decoder);
            logger.debug("Decoded Avro record: schema=[{}]", record.getSchema().getFullName());

            Map<String, Object> map = recordToMap(record);

            if (msgField != null) {
                Object nested = map.get(msgField);
                if (nested == null) {
                    logger.debug("avro.msg_field [{}] is null — tombstone, returning null", msgField);
                    return null;
                }
                if (nested instanceof Map == false) {
                    throw new IllegalArgumentException(
                        "avro.msg_field [" + msgField + "] must be a record, found: " + nested.getClass().getSimpleName()
                    );
                }
                map = (Map<String, Object>) nested;
                logger.debug("Extracted msg_field=[{}] with {} fields", msgField, map.size());
            }

            byte[] jsonBytes = toJsonBytes(map);
            logger.debug("Avro decode complete: json_bytes={}", jsonBytes.length);
            return jsonBytes;
        } catch (IOException e) {
            logger.error("Failed to decode Avro message: schema=[{}] payload_length={}", schema.getFullName(), raw.length, e);
            throw new IllegalArgumentException("Failed to decode Avro message: " + e.getMessage(), e);
        } catch (AvroRuntimeException e) {
            logger.error("Failed to decode Avro message: schema=[{}] payload_length={}", schema.getFullName(), raw.length, e);
            throw new IllegalArgumentException("Failed to decode Avro message: " + e.getMessage(), e);
        }
    }

    private static Map<String, String> resolveHeaders(Map<String, Object> params) {
        Map<String, String> headers = new HashMap<>();
        params.forEach((k, v) -> {
            if (k.startsWith(PARAM_SCHEMA_REGISTRY_HEADERS_PREFIX)) {
                headers.put(k.substring(PARAM_SCHEMA_REGISTRY_HEADERS_PREFIX.length()), String.valueOf(v));
            }
        });
        return headers;
    }

    private static Schema fetchSchema(String url, Map<String, String> headers, int connectTimeoutMs, int requestTimeoutMs) {
        try {
            HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(connectTimeoutMs))
                .build();
            HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofMillis(requestTimeoutMs))
                .GET();
            headers.forEach(builder::header);
            HttpResponse<String> response = client.send(builder.build(), HttpResponse.BodyHandlers.ofString());
            logger.debug("Schema registry HTTP {} body_length={}", response.statusCode(), response.body().length());
            if (response.statusCode() != 200) {
                String body = response.body();
                String bodyPreview = body.length() > MAX_ERROR_BODY_LENGTH
                    ? body.substring(0, MAX_ERROR_BODY_LENGTH) + "..."
                    : body;
                logger.debug("Schema registry error response body: {}", body);
                throw new IllegalStateException(
                    "Schema registry returned HTTP " + response.statusCode() + " for [" + url + "]: " + bodyPreview
                );
            }
            return parseSchemaFromBody(response.body());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while fetching Avro schema from [" + url + "]", e);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to fetch Avro schema from [" + url + "]: " + e.getMessage(), e);
        }
    }

    static Schema parseSchemaFromBody(String body) {
        try {
            Schema schema = new Schema.Parser().parse(body);
            logger.debug("Parsed Avro schema directly, name=[{}]", schema.getFullName());
            return schema;
        } catch (Throwable e) {
            if (!(e instanceof SchemaParseException)) {
                logger.error("AvroPayloadDecoder: schema parse threw unexpected error: {}", e.getMessage(), e);
                throw new IllegalStateException("Schema.Parser failed with unexpected error: " + e.getMessage(), e);
            }
            // Fall back to Confluent-style {"schema":"<escaped-json>"} wrapper
            logger.debug("Direct parse failed ({}), trying Confluent schema-field extraction", e.getMessage());
            int keyIdx = body.indexOf("\"schema\"");
            if (keyIdx < 0) {
                throw new IllegalArgumentException(
                    "Unable to parse Avro schema from registry response: " + e.getMessage(),
                    e
                );
            }
            int colonIdx = body.indexOf(':', keyIdx);
            int quoteStart = body.indexOf('"', colonIdx + 1);
            if (quoteStart < 0) {
                throw new IllegalArgumentException("Malformed schema registry response");
            }
            StringBuilder sb = new StringBuilder();
            int i = quoteStart + 1;
            while (i < body.length()) {
                char c = body.charAt(i);
                if (c == '\\' && i + 1 < body.length()) {
                    char next = body.charAt(i + 1);
                    switch (next) {
                        case '"':
                            sb.append('"');
                            i += 2;
                            break;
                        case '\\':
                            sb.append('\\');
                            i += 2;
                            break;
                        case 'n':
                            sb.append('\n');
                            i += 2;
                            break;
                        case 'r':
                            sb.append('\r');
                            i += 2;
                            break;
                        case 't':
                            sb.append('\t');
                            i += 2;
                            break;
                        case 'b':
                            sb.append('\b');
                            i += 2;
                            break;
                        case 'f':
                            sb.append('\f');
                            i += 2;
                            break;
                        case 'u':
                            if (i + 5 < body.length()) {
                                String hex = body.substring(i + 2, i + 6);
                                try {
                                    sb.append((char) Integer.parseInt(hex, 16));
                                    i += 6;
                                } catch (NumberFormatException nfe) {
                                    sb.append(next);
                                    i += 2;
                                }
                            } else {
                                sb.append(next);
                                i += 2;
                            }
                            break;
                        default:
                            sb.append(next);
                            i += 2;
                            break;
                    }
                } else if (c == '"') {
                    break;
                } else {
                    sb.append(c);
                    i++;
                }
            }
            Schema schema = new Schema.Parser().parse(sb.toString());
            logger.debug("Parsed Avro schema via Confluent extraction, name=[{}]", schema.getFullName());
            return schema;
        }
    }

    static Schema substituteInnerSchema(Schema wrapper, Schema inner, String wrapperField) {
        if (wrapper.getType() != Schema.Type.RECORD) {
            throw new IllegalArgumentException(
                "[avro.wrapper_schema] must be a record schema, found: " + wrapper.getType()
            );
        }
        List<Schema.Field> newFields = new ArrayList<>();
        boolean found = false;
        for (Schema.Field f : wrapper.getFields()) {
            if (f.name().equals(wrapperField)) {
                found = true;
                newFields.add(new Schema.Field(f.name(), inner, f.doc()));
            } else {
                Schema.Field copy = new Schema.Field(f.name(), f.schema(), f.doc(), f.defaultVal(), f.order());
                for (String alias : f.aliases()) {
                    copy.addAlias(alias);
                }
                newFields.add(copy);
            }
        }
        if (!found) {
            throw new IllegalArgumentException(
                "[avro.wrapper_field] [" + wrapperField + "] not found in [avro.wrapper_schema] " + wrapper.getFullName()
            );
        }
        Schema combined = Schema.createRecord(wrapper.getName(), wrapper.getDoc(), wrapper.getNamespace(), wrapper.isError());
        combined.setFields(newFields);
        return combined;
    }

    static Map<String, Object> recordToMap(GenericRecord record) {
        Map<String, Object> map = new HashMap<>();
        for (Schema.Field field : record.getSchema().getFields()) {
            map.put(field.name(), convertAvroValue(record.get(field.name())));
        }
        return map;
    }

    @SuppressWarnings("unchecked")
    private static Object convertAvroValue(Object value) {
        if (value == null) return null;
        if (value instanceof GenericRecord) return recordToMap((GenericRecord) value);
        if (value instanceof GenericFixed) {
            return Base64.getEncoder().encodeToString(((GenericFixed) value).bytes());
        }
        if (value instanceof ByteBuffer) {
            ByteBuffer bb = ((ByteBuffer) value).duplicate();
            byte[] bytes = new byte[bb.remaining()];
            bb.get(bytes);
            return Base64.getEncoder().encodeToString(bytes);
        }
        if (value instanceof Collection) {
            List<Object> list = new ArrayList<>();
            for (Object item : (Collection<?>) value) {
                list.add(convertAvroValue(item));
            }
            return list;
        }
        if (value instanceof Map) {
            Map<String, Object> result = new HashMap<>();
            ((Map<Object, Object>) value).forEach((k, v) -> result.put(String.valueOf(k), convertAvroValue(v)));
            return result;
        }
        if (value instanceof CharSequence) return value.toString();
        return value;
    }

    private static byte[] toJsonBytes(Map<String, Object> map) {
        return toJsonString(map).getBytes(StandardCharsets.UTF_8);
    }

    @SuppressWarnings("unchecked")
    static String toJsonString(Object value) {
        if (value == null) return "null";
        if (value instanceof Boolean) return value.toString();
        if (value instanceof Number) return value.toString();
        if (value instanceof Map) {
            StringBuilder sb = new StringBuilder("{");
            boolean first = true;
            for (Map.Entry<String, Object> e : ((Map<String, Object>) value).entrySet()) {
                if (!first) sb.append(",");
                sb.append('"').append(escapeJson(e.getKey())).append("\":").append(toJsonString(e.getValue()));
                first = false;
            }
            return sb.append("}").toString();
        }
        if (value instanceof List) {
            StringBuilder sb = new StringBuilder("[");
            boolean first = true;
            for (Object item : (List<?>) value) {
                if (!first) sb.append(",");
                sb.append(toJsonString(item));
                first = false;
            }
            return sb.append("]").toString();
        }
        return '"' + escapeJson(value.toString()) + '"';
    }

    private static String escapeJson(String s) {
        StringBuilder sb = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '\\': sb.append("\\\\"); break;
                case '"':  sb.append("\\\""); break;
                case '\n': sb.append("\\n");  break;
                case '\r': sb.append("\\r");  break;
                case '\t': sb.append("\\t");  break;
                case '\b': sb.append("\\b");  break;
                case '\f': sb.append("\\f");  break;
                default:
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
            }
        }
        return sb.toString();
    }
}
