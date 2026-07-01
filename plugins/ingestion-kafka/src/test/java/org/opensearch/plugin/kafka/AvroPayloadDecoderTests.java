/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.kafka;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for {@link AvroPayloadDecoder}.
 *
 * <p>The test dep {@code org.apache.avro:avro} lives at {@code org.apache.avro.*};
 * the production copy embedded in the shadow JAR lives at
 * {@code org.opensearch.plugin.kafka.shaded.avro.*} (relocated by the shadow plugin).
 * These are different class names, so the OpenSearch jar-hell check in
 * {@code BootstrapForTesting} sees no duplicates.
 *
 * <p>The only values that cross the boundary between the two copies are
 * {@code byte[]} (Avro wire format) and {@code String} (JSON output) — both
 * plain Java types, so there is no {@code ClassCastException} risk.
 *
 * <p>Static helpers that return Avro types ({@code parseSchemaFromBody},
 * {@code substituteInnerSchema}, {@code recordToMap}) are exercised indirectly
 * through {@link AvroPayloadDecoder#decode(byte[])} rather than called directly,
 * because the return types they produce are the relocated shaded versions which
 * cannot be assigned to test-side {@code org.apache.avro.*} variables.
 */
public class AvroPayloadDecoderTests extends OpenSearchTestCase {

    // -----------------------------------------------------------------------
    // Schemas and encoding helpers
    // -----------------------------------------------------------------------

    private static final String SIMPLE_SCHEMA_JSON = "{"
        + "\"type\":\"record\","
        + "\"name\":\"TestRecord\","
        + "\"namespace\":\"org.test\","
        + "\"fields\":["
        + "  {\"name\":\"id\",\"type\":\"int\"},"
        + "  {\"name\":\"name\",\"type\":\"string\"}"
        + "]}";

    private static final String OUTER_SCHEMA_JSON = "{"
        + "\"type\":\"record\","
        + "\"name\":\"Outer\","
        + "\"namespace\":\"org.test\","
        + "\"fields\":["
        + "  {\"name\":\"meta\",\"type\":\"string\"},"
        + "  {\"name\":\"payload\","
        + "   \"type\":{\"type\":\"record\",\"name\":\"Inner\",\"namespace\":\"org.test\","
        + "            \"fields\":[{\"name\":\"value\",\"type\":\"string\"}]}}"
        + "]}";

    private static final String NULLABLE_OUTER_SCHEMA_JSON = "{"
        + "\"type\":\"record\","
        + "\"name\":\"Outer\","
        + "\"namespace\":\"org.test\","
        + "\"fields\":["
        + "  {\"name\":\"payload\","
        + "   \"type\":[\"null\",{\"type\":\"record\",\"name\":\"Inner\",\"namespace\":\"org.test\","
        + "                      \"fields\":[{\"name\":\"v\",\"type\":\"string\"}]}],"
        + "   \"default\":null}"
        + "]}";

    /** Encode a GenericRecord to Avro binary format. */
    private static byte[] encode(Schema schema, GenericRecord record) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        writer.write(record, encoder);
        encoder.flush();
        return out.toByteArray();
    }

    private static Map<String, Object> schemaParams(String schemaJson) {
        Map<String, Object> p = new HashMap<>();
        p.put("avro.schema", schemaJson);
        return p;
    }

    /** Decode bytes to JSON string via a fresh decoder. */
    private static String decodeToJson(Map<String, Object> params, byte[] bytes) {
        return new String(new AvroPayloadDecoder(params).decode(bytes), StandardCharsets.UTF_8);
    }

    // -----------------------------------------------------------------------
    // PASSTHROUGH
    // -----------------------------------------------------------------------

    public void testPassthroughReturnsIdenticalReference() {
        byte[] raw = "hello".getBytes(StandardCharsets.UTF_8);
        assertSame("PASSTHROUGH must return the same reference", raw, KafkaPayloadDecoder.PASSTHROUGH.decode(raw));
    }

    // -----------------------------------------------------------------------
    // Constructor validation
    // -----------------------------------------------------------------------

    public void testMissingBothSchemaAndRegistryThrows() {
        Map<String, Object> params = new HashMap<>();
        params.put("avro.skip_bytes", "0");
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> new AvroPayloadDecoder(params));
        assertTrue(ex.getMessage().contains("avro.schema_registry_url") || ex.getMessage().contains("avro.schema"));
    }

    public void testInvalidInlineSchemaThrows() {
        Map<String, Object> params = schemaParams("{\"type\":\"broken}");
        expectThrows(IllegalArgumentException.class, () -> new AvroPayloadDecoder(params));
    }

    // -----------------------------------------------------------------------
    // Basic decode
    // -----------------------------------------------------------------------

    public void testDecodeSimpleRecord() throws Exception {
        Schema schema = new Schema.Parser().parse(SIMPLE_SCHEMA_JSON);
        GenericRecord record = new GenericData.Record(schema);
        record.put("id", 42);
        record.put("name", "alice");

        String json = decodeToJson(schemaParams(SIMPLE_SCHEMA_JSON), encode(schema, record));
        assertTrue(json.contains("\"id\""));
        assertTrue(json.contains("42"));
        assertTrue(json.contains("\"name\""));
        assertTrue(json.contains("\"alice\""));
    }

    // -----------------------------------------------------------------------
    // skip_bytes
    // -----------------------------------------------------------------------

    public void testDecodeWithSkipBytesString() throws Exception {
        Schema schema = new Schema.Parser().parse(SIMPLE_SCHEMA_JSON);
        GenericRecord record = new GenericData.Record(schema);
        record.put("id", 7);
        record.put("name", "bob");
        byte[] payload = encode(schema, record);

        byte[] withHeader = new byte[8 + payload.length];
        Arrays.fill(withHeader, 0, 8, (byte) 0xFF);
        System.arraycopy(payload, 0, withHeader, 8, payload.length);

        Map<String, Object> params = schemaParams(SIMPLE_SCHEMA_JSON);
        params.put("avro.skip_bytes", "8");
        String json = decodeToJson(params, withHeader);
        assertTrue(json.contains("\"bob\""));
        assertTrue(json.contains("7"));
    }

    public void testDecodeWithSkipBytesInteger() throws Exception {
        Schema schema = new Schema.Parser().parse(SIMPLE_SCHEMA_JSON);
        GenericRecord record = new GenericData.Record(schema);
        record.put("id", 5);
        record.put("name", "dave");
        byte[] payload = encode(schema, record);

        byte[] withHeader = new byte[4 + payload.length];
        System.arraycopy(payload, 0, withHeader, 4, payload.length);

        Map<String, Object> params = schemaParams(SIMPLE_SCHEMA_JSON);
        params.put("avro.skip_bytes", 4); // Integer, not String
        String json = decodeToJson(params, withHeader);
        assertTrue(json.contains("\"dave\""));
        assertTrue(json.contains("5"));
    }

    public void testPayloadTooShortAfterSkipThrows() {
        Map<String, Object> params = schemaParams(SIMPLE_SCHEMA_JSON);
        params.put("avro.skip_bytes", "100");
        AvroPayloadDecoder decoder = new AvroPayloadDecoder(params);
        byte[] tiny = new byte[5];
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> decoder.decode(tiny));
        assertTrue(ex.getMessage().contains("too short") || ex.getMessage().contains("remaining"));
    }

    // -----------------------------------------------------------------------
    // msg_field extraction  (exercises recordToMap + msg_field path indirectly)
    // -----------------------------------------------------------------------

    public void testDecodeWithMsgFieldExtractsInnerRecord() throws Exception {
        Schema outerSchema = new Schema.Parser().parse(OUTER_SCHEMA_JSON);
        Schema innerSchema = outerSchema.getField("payload").schema();

        GenericRecord inner = new GenericData.Record(innerSchema);
        inner.put("value", "the-doc");
        GenericRecord outer = new GenericData.Record(outerSchema);
        outer.put("meta", "ignored");
        outer.put("payload", inner);

        Map<String, Object> params = schemaParams(OUTER_SCHEMA_JSON);
        params.put("avro.msg_field", "payload");
        String json = decodeToJson(params, encode(outerSchema, outer));
        assertTrue(json.contains("\"value\""));
        assertTrue(json.contains("\"the-doc\""));
        assertFalse("outer field 'meta' must not appear after extraction", json.contains("\"meta\""));
    }

    public void testDecodeWithNullMsgFieldReturnsTombstone() throws Exception {
        Schema outerSchema = new Schema.Parser().parse(NULLABLE_OUTER_SCHEMA_JSON);
        GenericRecord outer = new GenericData.Record(outerSchema);
        outer.put("payload", null);

        Map<String, Object> params = schemaParams(NULLABLE_OUTER_SCHEMA_JSON);
        params.put("avro.msg_field", "payload");
        byte[] result = new AvroPayloadDecoder(params).decode(encode(outerSchema, outer));
        assertNull("null msg_field must return null (tombstone)", result);
    }

    public void testDecodeWithMsgFieldPointingToScalarThrows() throws Exception {
        Schema schema = new Schema.Parser().parse(SIMPLE_SCHEMA_JSON);
        GenericRecord record = new GenericData.Record(schema);
        record.put("id", 1);
        record.put("name", "x");

        Map<String, Object> params = schemaParams(SIMPLE_SCHEMA_JSON);
        params.put("avro.msg_field", "name"); // "name" is a string, not a record
        AvroPayloadDecoder decoder = new AvroPayloadDecoder(params);
        expectThrows(IllegalArgumentException.class, () -> decoder.decode(encode(schema, record)));
    }

    // -----------------------------------------------------------------------
    // wrapper_schema + wrapper_field  (exercises substituteInnerSchema indirectly)
    // -----------------------------------------------------------------------

    private static final String WRAPPER_SCHEMA_JSON = "{"
        + "\"type\":\"record\","
        + "\"name\":\"Wrapper\","
        + "\"namespace\":\"org.test\","
        + "\"fields\":["
        + "  {\"name\":\"ts\",\"type\":\"long\"},"
        + "  {\"name\":\"payload\",\"type\":\"string\"}" // placeholder replaced by inner
        + "]}";

    private static final String INNER_SCHEMA_JSON = "{"
        + "\"type\":\"record\","
        + "\"name\":\"Inner\","
        + "\"namespace\":\"org.test\","
        + "\"fields\":[{\"name\":\"v\",\"type\":\"int\"}]"
        + "}";

    public void testDecodeWithWrapperSchema() throws Exception {
        // Combined schema after substitution: Wrapper { ts:long, payload:Inner{v:int} }
        // We encode against the combined schema to produce a valid message.
        String combinedSchemaJson = "{"
            + "\"type\":\"record\",\"name\":\"Wrapper\",\"namespace\":\"org.test\","
            + "\"fields\":["
            + "  {\"name\":\"ts\",\"type\":\"long\"},"
            + "  {\"name\":\"payload\","
            + "   \"type\":{\"type\":\"record\",\"name\":\"Inner\",\"namespace\":\"org.test\","
            + "            \"fields\":[{\"name\":\"v\",\"type\":\"int\"}]}}"
            + "]}";
        Schema combinedSchema = new Schema.Parser().parse(combinedSchemaJson);
        Schema innerSchema = combinedSchema.getField("payload").schema();

        GenericRecord inner = new GenericData.Record(innerSchema);
        inner.put("v", 3);
        GenericRecord wrapper = new GenericData.Record(combinedSchema);
        wrapper.put("ts", 1L);
        wrapper.put("payload", inner);

        Map<String, Object> params = new HashMap<>();
        params.put("avro.schema", INNER_SCHEMA_JSON);
        params.put("avro.wrapper_schema", WRAPPER_SCHEMA_JSON);
        params.put("avro.wrapper_field", "payload");
        String json = decodeToJson(params, encode(combinedSchema, wrapper));
        assertTrue(json.contains("\"ts\""));
        assertTrue(json.contains("\"v\""));
    }

    public void testDecodeWithWrapperSchemaPlusMsgField() throws Exception {
        String combinedSchemaJson = "{"
            + "\"type\":\"record\",\"name\":\"Wrapper\",\"namespace\":\"org.test\","
            + "\"fields\":["
            + "  {\"name\":\"ts\",\"type\":\"long\"},"
            + "  {\"name\":\"payload\","
            + "   \"type\":{\"type\":\"record\",\"name\":\"Inner\",\"namespace\":\"org.test\","
            + "            \"fields\":[{\"name\":\"v\",\"type\":\"int\"}]}}"
            + "]}";
        Schema combinedSchema = new Schema.Parser().parse(combinedSchemaJson);
        Schema innerSchema = combinedSchema.getField("payload").schema();

        GenericRecord inner = new GenericData.Record(innerSchema);
        inner.put("v", 7);
        GenericRecord wrapper = new GenericData.Record(combinedSchema);
        wrapper.put("ts", 100L);
        wrapper.put("payload", inner);

        Map<String, Object> params = new HashMap<>();
        params.put("avro.schema", INNER_SCHEMA_JSON);
        params.put("avro.wrapper_schema", WRAPPER_SCHEMA_JSON);
        params.put("avro.wrapper_field", "payload");
        params.put("avro.msg_field", "payload");
        String json = decodeToJson(params, encode(combinedSchema, wrapper));
        assertTrue(json.contains("\"v\""));
        assertFalse("outer 'ts' must not appear after msg_field extraction", json.contains("\"ts\""));
    }

    public void testWrapperFieldNotFoundThrows() {
        Map<String, Object> params = new HashMap<>();
        params.put("avro.schema", INNER_SCHEMA_JSON);
        params.put("avro.wrapper_schema", WRAPPER_SCHEMA_JSON);
        params.put("avro.wrapper_field", "nonexistent");
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> new AvroPayloadDecoder(params));
        assertTrue(ex.getMessage().contains("nonexistent"));
    }

    // -----------------------------------------------------------------------
    // parseSchemaFromBody — direct schema JSON path exercised via constructor
    // -----------------------------------------------------------------------

    public void testInlineSchemaAsStringAccepted() throws Exception {
        Schema schema = new Schema.Parser().parse(SIMPLE_SCHEMA_JSON);
        GenericRecord record = new GenericData.Record(schema);
        record.put("id", 99);
        record.put("name", "carol");
        String json = decodeToJson(schemaParams(SIMPLE_SCHEMA_JSON), encode(schema, record));
        assertTrue(json.contains("99"));
        assertTrue(json.contains("\"carol\""));
    }

    public void testInlineSchemaAsMapAccepted() throws Exception {
        // OpenSearch may deserialize JSON settings into a Map rather than leaving them as strings.
        Schema schema = new Schema.Parser().parse(SIMPLE_SCHEMA_JSON);
        GenericRecord record = new GenericData.Record(schema);
        record.put("id", 1);
        record.put("name", "x");

        Map<String, Object> schemaMap = new HashMap<>();
        schemaMap.put("type", "record");
        schemaMap.put("name", "TestRecord");
        schemaMap.put("namespace", "org.test");
        schemaMap.put("fields", List.of(
            Map.of("name", "id", "type", "int"),
            Map.of("name", "name", "type", "string")
        ));
        Map<String, Object> params = new HashMap<>();
        params.put("avro.schema", schemaMap);
        String json = decodeToJson(params, encode(schema, record));
        assertTrue(json.contains("1"));
        assertTrue(json.contains("\"x\""));
    }

    // -----------------------------------------------------------------------
    // extractSchemaJson -- Confluent-style wrapper
    // -----------------------------------------------------------------------

    public void testExtractSchemaJsonConfluentStyle() {
        // Confluent schema registry returns {"schema":"<escaped avro json>","id":1,...}
        String escaped = SIMPLE_SCHEMA_JSON.replace("\\", "\\\\").replace("\"", "\\\"");
        String confluentBody = "{\"id\":1,\"schema\":\"" + escaped + "\"}";
        // extractSchemaJson returns a String — safe to call from test side (no relocated types)
        String schemaJson = AvroPayloadDecoder.extractSchemaJson(confluentBody);
        Schema parsed = new Schema.Parser().parse(schemaJson);
        assertEquals("TestRecord", parsed.getName());
        assertEquals("org.test", parsed.getNamespace());
    }

    public void testExtractSchemaJsonDirectAvro() {
        // Plain Avro JSON should be returned as-is
        String schemaJson = AvroPayloadDecoder.extractSchemaJson(SIMPLE_SCHEMA_JSON);
        Schema parsed = new Schema.Parser().parse(schemaJson);
        assertEquals("TestRecord", parsed.getName());
    }

    // -----------------------------------------------------------------------
    // null tombstone
    // -----------------------------------------------------------------------

    public void testDecodeNullRawReturnsTombstone() {
        AvroPayloadDecoder decoder = new AvroPayloadDecoder(schemaParams(SIMPLE_SCHEMA_JSON));
        assertNull("null raw payload must return null (tombstone)", decoder.decode(null));
    }

    // -----------------------------------------------------------------------
    // Negative skipBytes (P2-1)
    // -----------------------------------------------------------------------

    public void testNegativeSkipBytesThrows() {
        Map<String, Object> params = schemaParams(SIMPLE_SCHEMA_JSON);
        params.put("avro.skip_bytes", "-1");
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> new AvroPayloadDecoder(params));
        assertTrue(ex.getMessage().contains("skip_bytes") || ex.getMessage().contains("non-negative"));
    }

    // -----------------------------------------------------------------------
    // Partial wrapper config validation (P1-2)
    // -----------------------------------------------------------------------

    public void testOnlyWrapperSchemaWithoutWrapperFieldThrows() {
        Map<String, Object> params = new HashMap<>();
        params.put("avro.schema", INNER_SCHEMA_JSON);
        params.put("avro.wrapper_schema", WRAPPER_SCHEMA_JSON);
        // avro.wrapper_field intentionally absent
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> new AvroPayloadDecoder(params));
        assertTrue(
            "must mention both params",
            ex.getMessage().contains("wrapper_schema") || ex.getMessage().contains("wrapper_field")
        );
    }

    public void testOnlyWrapperFieldWithoutWrapperSchemaThrows() {
        Map<String, Object> params = new HashMap<>();
        params.put("avro.schema", INNER_SCHEMA_JSON);
        params.put("avro.wrapper_field", "payload");
        // avro.wrapper_schema intentionally absent
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> new AvroPayloadDecoder(params));
        assertTrue(
            "must mention both params",
            ex.getMessage().contains("wrapper_schema") || ex.getMessage().contains("wrapper_field")
        );
    }

    // -----------------------------------------------------------------------
    // GenericFixed field (P1-1)
    // -----------------------------------------------------------------------

    public void testDecodeGenericFixedFieldEncodedAsBase64() throws Exception {
        // MD5 type is a 16-byte Fixed type in Avro
        String fixedSchemaJson = "{"
            + "\"type\":\"record\","
            + "\"name\":\"HasFixed\","
            + "\"namespace\":\"org.test\","
            + "\"fields\":["
            + "  {\"name\":\"digest\","
            + "   \"type\":{\"type\":\"fixed\",\"name\":\"MD5\",\"size\":16}}"
            + "]}";

        Schema schema = new Schema.Parser().parse(fixedSchemaJson);
        Schema fixedType = schema.getField("digest").schema();

        byte[] fixedBytes = new byte[16];
        for (int i = 0; i < 16; i++) fixedBytes[i] = (byte) (i + 1);
        GenericFixed fixedValue = new GenericData.Fixed(fixedType, fixedBytes);

        GenericRecord record = new GenericData.Record(schema);
        record.put("digest", fixedValue);

        String json = decodeToJson(schemaParams(fixedSchemaJson), encode(schema, record));
        String expectedBase64 = Base64.getEncoder().encodeToString(fixedBytes);
        assertTrue("GenericFixed must be base64-encoded in JSON output", json.contains(expectedBase64));
    }
}
