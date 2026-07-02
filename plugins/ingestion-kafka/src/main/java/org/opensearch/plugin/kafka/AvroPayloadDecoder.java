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
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.indices.pollingingest.PayloadDecoder;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
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
 *       mutually exclusive with {@code avro.wrapper_schema_registry_url};
 *       must be set together with {@code avro.wrapper_field}</li>
 *   <li>{@code avro.wrapper_schema_registry_url} — URL to fetch the wrapper schema from;
 *       mutually exclusive with {@code avro.wrapper_schema};
 *       must be set together with {@code avro.wrapper_field}</li>
 *   <li>{@code avro.wrapper_schema_registry_headers.<name>} — HTTP headers for the wrapper
 *       schema registry request (independent of {@code avro.schema_registry_headers.*})</li>
 *   <li>{@code avro.wrapper_schema_registry_connect_timeout_ms} — connect timeout for wrapper
 *       registry requests in milliseconds (default 10000)</li>
 *   <li>{@code avro.wrapper_schema_registry_request_timeout_ms} — request timeout for wrapper
 *       registry requests in milliseconds (default 10000)</li>
 *   <li>{@code avro.wrapper_field} — field in the wrapper schema to substitute the inner
 *       schema into; required when any wrapper schema source is configured</li>
 *   <li>{@code avro.msg_field} — field in the decoded record to use as the document source</li>
 * </ul>
 *
 * <p>If no {@code avro.*} params are present, use {@link PayloadDecoder#PASSTHROUGH} instead.
 *
 * <p><b>Known limitations</b>
 *
 * <p><b>Schema evolution:</b> this decoder uses the configured schema as both the writer schema
 * and the reader schema ({@code new GenericDatumReader<>(schema)}). This is correct when every
 * producer on the topic writes with exactly that schema version. True Avro schema evolution —
 * where a reader schema with added/defaulted fields decodes bytes written with an older writer
 * schema — requires the writer schema to be embedded in or derivable from each message (e.g. the
 * 5-byte Confluent wire format carries a schema registry ID). Support for writer/reader schema
 * resolution is a future enhancement; for now the configured schema must match the producer schema.
 *
 * <p><b>Logical types:</b> Avro logical types are decoded as their underlying primitive Java type:
 * {@code timestamp-millis/micros} and {@code time-millis/micros} become {@code long}/{@code int},
 * {@code date} becomes {@code int} (days since epoch), {@code uuid} becomes {@code String}.
 * {@code decimal} (bytes or fixed) is base64-encoded — it does <em>not</em> become a numeric or
 * string decimal. If your schema uses logical types and the downstream field mappings expect
 * formatted values, pre-process messages before ingestion or post-process with an ingest pipeline.
 *
 * <p><b>Shadow-plugin note:</b> this class imports {@code org.apache.avro.*} which is provided
 * at compile time by the {@code compileOnly} dependency.  The shadow plugin rewrites every
 * {@code org.apache.avro.*} bytecode reference to {@code org.opensearch.plugin.kafka.shaded.avro.*}
 * at JAR-build time, so at runtime the class names match the relocated classes embedded in the
 * shadow JAR.  Avro must not be added as a non-{@code compileOnly}/{@code avroShade} dependency
 * or the rewriting step will be skipped and the plugin will fail at runtime.
 */
public class AvroPayloadDecoder implements PayloadDecoder {

    private static final Logger logger = LogManager.getLogger(AvroPayloadDecoder.class);

    static final String AVRO_PREFIX = "avro.";
    static final String PARAM_SKIP_BYTES = "avro.skip_bytes";
    static final String PARAM_SCHEMA = "avro.schema";
    static final String PARAM_SCHEMA_REGISTRY_URL = "avro.schema_registry_url";
    static final String PARAM_SCHEMA_REGISTRY_HEADERS_PREFIX = "avro.schema_registry_headers.";
    static final String PARAM_SCHEMA_REGISTRY_CONNECT_TIMEOUT_MS = "avro.schema_registry_connect_timeout_ms";
    static final String PARAM_SCHEMA_REGISTRY_REQUEST_TIMEOUT_MS = "avro.schema_registry_request_timeout_ms";
    static final String PARAM_WRAPPER_SCHEMA = "avro.wrapper_schema";
    static final String PARAM_WRAPPER_SCHEMA_REGISTRY_URL = "avro.wrapper_schema_registry_url";
    static final String PARAM_WRAPPER_SCHEMA_REGISTRY_HEADERS_PREFIX = "avro.wrapper_schema_registry_headers.";
    static final String PARAM_WRAPPER_SCHEMA_REGISTRY_CONNECT_TIMEOUT_MS = "avro.wrapper_schema_registry_connect_timeout_ms";
    static final String PARAM_WRAPPER_SCHEMA_REGISTRY_REQUEST_TIMEOUT_MS = "avro.wrapper_schema_registry_request_timeout_ms";
    static final String PARAM_WRAPPER_FIELD = "avro.wrapper_field";
    static final String PARAM_MSG_FIELD = "avro.msg_field";

    private static final int DEFAULT_REGISTRY_TIMEOUT_MS = 10_000;
    private static final int MAX_ERROR_BODY_LENGTH = 512;
    private static final DecoderFactory DECODER_FACTORY = DecoderFactory.get();

    private final Schema schema;
    private final int skipBytes;
    private final String msgField;

    // ThreadLocal reuse: decode() may be called from multiple processor threads concurrently.
    // GenericDatumReader, BinaryDecoder, and GenericRecord are not thread-safe, so each thread
    // gets its own instance, reused across successive messages on that thread.
    private final ThreadLocal<GenericDatumReader<GenericRecord>> threadLocalReader;
    private final ThreadLocal<BinaryDecoder> threadLocalBinaryDecoder = new ThreadLocal<>();
    private final ThreadLocal<GenericRecord> threadLocalRecord = new ThreadLocal<>();

    /**
     * Creates an {@code AvroPayloadDecoder} from the given {@code avro.*} parameters.
     * This includes resolving the Avro schema — either by parsing the inline {@code avro.schema}
     * value or by fetching it from the configured {@code avro.schema_registry_url}.
     * All I/O (HTTP fetch) and validation happens here, not in the constructor.
     *
     * @param params map of avro.* configuration keys to values
     * @return a fully initialized decoder ready to decode messages
     */
    public static AvroPayloadDecoder create(Map<String, Object> params) {
        int rawSkipBytes = 0;
        if (params.containsKey(PARAM_SKIP_BYTES)) {
            Object skipVal = params.get(PARAM_SKIP_BYTES);
            try {
                rawSkipBytes = skipVal instanceof Number
                    ? ((Number) skipVal).intValue()
                    : Integer.parseInt(String.valueOf(skipVal).trim());
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("[avro.skip_bytes] must be an integer, got: " + skipVal);
            }
        }
        if (rawSkipBytes < 0) {
            throw new IllegalArgumentException("[avro.skip_bytes] must be non-negative, got: " + rawSkipBytes);
        }
        int skipBytes = rawSkipBytes;
        String msgField = getParam(params, PARAM_MSG_FIELD);

        String registryUrl = params.containsKey(PARAM_SCHEMA_REGISTRY_URL)
            ? String.valueOf(params.get(PARAM_SCHEMA_REGISTRY_URL)).trim()
            : null;

        // avro.schema value may arrive as a String or as a parsed Map if OpenSearch deserialized it
        String inlineSchema = null;
        if (params.containsKey(PARAM_SCHEMA)) {
            Object schemaVal = params.get(PARAM_SCHEMA);
            if (schemaVal instanceof String) {
                inlineSchema = (String) schemaVal;
            } else if (schemaVal instanceof Map) {
                try {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> schemaMap = (Map<String, Object>) schemaVal;
                    inlineSchema = BytesReference.bytes(XContentFactory.jsonBuilder().map(schemaMap)).utf8ToString();
                } catch (IOException ex) {
                    throw new IllegalArgumentException("Failed to re-serialize avro.schema map to JSON", ex);
                }
            } else if (schemaVal != null) {
                inlineSchema = String.valueOf(schemaVal);
            }
        }

        if (registryUrl == null && inlineSchema == null) {
            throw new IllegalArgumentException(
                "avro params present but neither [avro.schema_registry_url] nor [avro.schema] is configured"
            );
        }

        String wrapperSchemaJson = null;
        if (params.containsKey(PARAM_WRAPPER_SCHEMA)) {
            Object wsVal = params.get(PARAM_WRAPPER_SCHEMA);
            if (wsVal instanceof Map) {
                try {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> wsMap = (Map<String, Object>) wsVal;
                    wrapperSchemaJson = BytesReference.bytes(XContentFactory.jsonBuilder().map(wsMap)).utf8ToString();
                } catch (IOException ex) {
                    throw new IllegalArgumentException("Failed to re-serialize avro.wrapper_schema map to JSON", ex);
                }
            } else if (wsVal != null) {
                wrapperSchemaJson = String.valueOf(wsVal);
            }
        }
        String wrapperRegistryUrl = params.containsKey(PARAM_WRAPPER_SCHEMA_REGISTRY_URL)
            ? String.valueOf(params.get(PARAM_WRAPPER_SCHEMA_REGISTRY_URL)).trim()
            : null;
        String wrapperField = getParam(params, PARAM_WRAPPER_FIELD);

        if (wrapperSchemaJson != null && wrapperRegistryUrl != null) {
            throw new IllegalArgumentException(
                "[avro.wrapper_schema] and [avro.wrapper_schema_registry_url] are mutually exclusive"
            );
        }
        boolean hasWrapperSource = wrapperSchemaJson != null || wrapperRegistryUrl != null;
        if (hasWrapperSource && wrapperField == null) {
            throw new IllegalArgumentException(
                "[avro.wrapper_field] is required when [avro.wrapper_schema] or [avro.wrapper_schema_registry_url] is configured"
            );
        }
        if (wrapperField != null && !hasWrapperSource) {
            throw new IllegalArgumentException(
                "[avro.wrapper_field] requires either [avro.wrapper_schema] or [avro.wrapper_schema_registry_url] to be configured"
            );
        }

        // Resolve inner schema
        Map<String, String> innerHeaders = resolveHeaders(params, PARAM_SCHEMA_REGISTRY_HEADERS_PREFIX);
        int connectTimeoutMs = parsePositiveMs(
            params.get(PARAM_SCHEMA_REGISTRY_CONNECT_TIMEOUT_MS),
            PARAM_SCHEMA_REGISTRY_CONNECT_TIMEOUT_MS,
            DEFAULT_REGISTRY_TIMEOUT_MS
        );
        int requestTimeoutMs = parsePositiveMs(
            params.get(PARAM_SCHEMA_REGISTRY_REQUEST_TIMEOUT_MS),
            PARAM_SCHEMA_REGISTRY_REQUEST_TIMEOUT_MS,
            DEFAULT_REGISTRY_TIMEOUT_MS
        );
        Schema innerSchema = resolveSchema(registryUrl, inlineSchema, innerHeaders, connectTimeoutMs, requestTimeoutMs);

        Schema schema;
        if (hasWrapperSource) {
            // Resolve wrapper schema — may use a different registry with its own headers and timeouts
            Map<String, String> wrapperHeaders = resolveHeaders(params, PARAM_WRAPPER_SCHEMA_REGISTRY_HEADERS_PREFIX);
            int wrapperConnectMs = parsePositiveMs(
                params.get(PARAM_WRAPPER_SCHEMA_REGISTRY_CONNECT_TIMEOUT_MS),
                PARAM_WRAPPER_SCHEMA_REGISTRY_CONNECT_TIMEOUT_MS,
                DEFAULT_REGISTRY_TIMEOUT_MS
            );
            int wrapperRequestMs = parsePositiveMs(
                params.get(PARAM_WRAPPER_SCHEMA_REGISTRY_REQUEST_TIMEOUT_MS),
                PARAM_WRAPPER_SCHEMA_REGISTRY_REQUEST_TIMEOUT_MS,
                DEFAULT_REGISTRY_TIMEOUT_MS
            );
            Schema wrapper = resolveSchema(wrapperRegistryUrl, wrapperSchemaJson, wrapperHeaders, wrapperConnectMs, wrapperRequestMs);
            schema = substituteInnerSchema(wrapper, innerSchema, wrapperField);
            logger.info(
                "AvroPayloadDecoder: combined schema [{}] fields={}",
                schema.getFullName(),
                schema.getFields().stream().map(Schema.Field::name).toList()
            );
        } else {
            schema = innerSchema;
        }

        if (schema.getType() != Schema.Type.RECORD) {
            throw new IllegalArgumentException(
                "[avro.schema] must be a RECORD schema, found: " + schema.getType()
                    + " — only top-level record schemas are supported"
            );
        }
        if (msgField != null && schema.getField(msgField) == null) {
            throw new IllegalArgumentException(
                "[avro.msg_field] field [" + msgField + "] does not exist in schema [" + schema.getFullName() + "]"
            );
        }
        logger.info("AvroPayloadDecoder: initialized — schema=[{}] skipBytes={} msgField={}", schema.getFullName(), skipBytes, msgField);
        return new AvroPayloadDecoder(schema, skipBytes, msgField);
    }

    private static Schema resolveSchema(String registryUrl, String inlineSchema, Map<String, String> headers,
                                        int connectTimeoutMs, int requestTimeoutMs) {
        if (registryUrl != null) {
            logger.info("AvroPayloadDecoder: fetching schema from [{}] headers={}", registryUrl, headers.keySet());
            Schema schema = fetchSchema(registryUrl, headers, connectTimeoutMs, requestTimeoutMs);
            logger.info("AvroPayloadDecoder: fetched schema [{}]", schema.getFullName());
            return schema;
        } else {
            // The inline schema may be either direct Avro JSON or a Confluent-style
            // {"schema":"..."} body — parseSchemaFromBody handles both in one parse.
            Schema schema = parseSchemaFromBody(inlineSchema);
            logger.debug("AvroPayloadDecoder: parsed inline schema [{}]", schema.getFullName());
            return schema;
        }
    }

    /**
     * Private constructor — use {@link #create(Map)} instead.
     */
    private AvroPayloadDecoder(Schema schema, int skipBytes, String msgField) {
        this.schema = schema;
        this.skipBytes = skipBytes;
        this.msgField = msgField;
        this.threadLocalReader = ThreadLocal.withInitial(() -> new GenericDatumReader<>(schema));
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
        if (remaining == 0) {
            return null; // empty payload after skip — treat as tombstone
        }
        if (remaining < 0) {
            throw new IllegalArgumentException(
                "Avro payload too short after skipping " + skipBytes + " bytes (payload length=" + raw.length + ")"
            );
        }

        logger.debug(() -> {
            int previewLen = Math.min(remaining, 16);
            StringBuilder hex = new StringBuilder();
            for (int i = skipBytes; i < skipBytes + previewLen; i++) {
                hex.append(String.format("%02x ", raw[i]));
            }
            return new ParameterizedMessage(
                "Decoding Avro payload: total={} skip={} remaining={} first_bytes=[{}]",
                raw.length, skipBytes, remaining, hex.toString().trim()
            );
        });

        try {
            // Reuse per-thread instances across successive messages — avoids allocation on the hot path.
            BinaryDecoder binaryDecoder = DECODER_FACTORY.binaryDecoder(
                new ByteArrayInputStream(raw, skipBytes, remaining),
                threadLocalBinaryDecoder.get()
            );
            threadLocalBinaryDecoder.set(binaryDecoder);
            GenericRecord record = threadLocalReader.get().read(threadLocalRecord.get(), binaryDecoder);
            threadLocalRecord.set(record);
            logger.debug(() -> new ParameterizedMessage("Decoded Avro record: schema=[{}]", record.getSchema().getFullName()));

            Map<String, Object> map;
            if (msgField != null) {
                Object rawNested = record.get(msgField);
                if (rawNested == null) {
                    // The field exists in the schema (validated at construction) but its value is
                    // null. This is a nullable union field set to null — not a Kafka tombstone.
                    throw new IllegalArgumentException(
                        "avro.msg_field [" + msgField + "] value is null in decoded record — "
                            + "if null payloads are valid tombstones, remove avro.msg_field and handle "
                            + "the envelope fields in the index mapping instead"
                    );
                }
                if (rawNested instanceof GenericRecord nestedRecord) {
                    map = recordToMap(nestedRecord);
                    logger.debug(() -> new ParameterizedMessage("Extracted msg_field=[{}] with {} fields", msgField, map.size()));
                } else {
                    throw new IllegalArgumentException(
                        "avro.msg_field [" + msgField + "] must be a record, found: " + rawNested.getClass().getSimpleName()
                    );
                }
            } else {
                map = recordToMap(record);
            }

            // TODO: the decoded record is serialized to JSON bytes here and then parsed back to a Map
            // in MessageProcessorRunnable. A future optimization could pass the Map directly through
            // the ingestion pipeline to avoid the serialize/parse round-trip.
            byte[] jsonBytes = toJsonBytes(map);
            logger.debug("Avro decode complete: json_bytes={}", jsonBytes.length);
            return jsonBytes;
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to decode Avro message: " + e.getMessage(), e);
        } catch (AvroRuntimeException e) {
            throw new IllegalArgumentException("Failed to decode Avro message: " + e.getMessage(), e);
        }
    }

    private static Map<String, String> resolveHeaders(Map<String, Object> params, String prefix) {
        Map<String, String> headers = new HashMap<>();
        params.forEach((k, v) -> {
            if (k.startsWith(prefix)) {
                headers.put(k.substring(prefix.length()), String.valueOf(v));
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

    /**
     * Parses an Avro schema from a string body that is either direct Avro JSON or a
     * Confluent-style {@code {"schema":"<escaped-json>", ...}} registry response.
     * Both paths parse exactly once — no double parse.
     */
    static Schema parseSchemaFromBody(String body) {
        try {
            // Try direct Avro JSON first — single parse, no validation pre-pass.
            Schema schema = new Schema.Parser().parse(body);
            logger.debug("Parsed Avro schema directly, name=[{}]", schema.getFullName());
            return schema;
        } catch (SchemaParseException e) {
            // Fall back to Confluent-style {"schema":"<escaped-json>"} wrapper.
            logger.debug("Direct parse failed ({}), trying Confluent schema-field extraction", e.getMessage());
            try (XContentParser parser = XContentType.JSON.xContent()
                .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, body)) {
                Map<String, Object> map = parser.map();
                Object schemaValue = map.get("schema");
                if (schemaValue == null) {
                    throw new IllegalArgumentException(
                        "Unable to parse Avro schema from registry response: no 'schema' field found"
                    );
                }
                Schema schema = new Schema.Parser().parse(String.valueOf(schemaValue));
                logger.debug("Parsed Avro schema via Confluent wrapper, name=[{}]", schema.getFullName());
                return schema;
            } catch (IOException ex) {
                throw new IllegalArgumentException(
                    "Unable to parse Avro schema from registry response: " + ex.getMessage(), ex
                );
            }
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
                Schema.Field substituted = new Schema.Field(f.name(), inner, f.doc(), null, f.order());
                for (String alias : f.aliases()) {
                    substituted.addAlias(alias);
                }
                newFields.add(substituted);
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

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static Object convertAvroValue(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof GenericRecord r) {
            return recordToMap(r);
        }
        if (value instanceof GenericFixed f) {
            return Base64.getEncoder().encodeToString(f.bytes());
        }
        if (value instanceof GenericEnumSymbol<?> e) {
            return e.toString();
        }
        if (value instanceof ByteBuffer bb) {
            ByteBuffer dup = bb.duplicate();
            dup.rewind();
            byte[] bytes = new byte[dup.remaining()];
            dup.get(bytes);
            return Base64.getEncoder().encodeToString(bytes);
        }
        if (value instanceof Float f) {
            if (Float.isNaN(f) || Float.isInfinite(f)) {
                logger.warn("Non-finite float value [{}] in Avro field replaced with null", f);
                return null;
            }
            return f;
        }
        if (value instanceof Double d) {
            if (Double.isNaN(d) || Double.isInfinite(d)) {
                logger.warn("Non-finite double value [{}] in Avro field replaced with null", d);
                return null;
            }
            return d;
        }
        if (value instanceof Collection<?> c) {
            List<Object> list = new ArrayList<>();
            for (Object item : c) {
                list.add(convertAvroValue(item));
            }
            return list;
        }
        if (value instanceof Map<?, ?> m) {
            Map<String, Object> result = new HashMap<>();
            m.forEach((k, v) -> result.put(String.valueOf(k), convertAvroValue(v)));
            return result;
        }
        if (value instanceof CharSequence cs) {
            return cs.toString();
        }
        return value;
    }

    /** Returns {@code String.valueOf(params.get(key))} if the key is present, else {@code null}. */
    private static String getParam(Map<String, Object> params, String key) {
        return params.containsKey(key) ? String.valueOf(params.get(key)) : null;
    }

    private static int parsePositiveMs(Object value, String paramName, int defaultMs) {        if (value == null) return defaultMs;
        int ms;
        try {
            ms = value instanceof Number ? ((Number) value).intValue() : Integer.parseInt(String.valueOf(value).trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("[" + paramName + "] must be an integer, got: " + value);
        }
        if (ms <= 0) {
            throw new IllegalArgumentException("[" + paramName + "] must be positive, got: " + ms);
        }
        return ms;
    }

    private static byte[] toJsonBytes(Map<String, Object> map) {
        try {
            return BytesReference.toBytes(BytesReference.bytes(XContentFactory.jsonBuilder().map(map)));
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to serialize decoded Avro record as JSON", e);
        }
    }
}
