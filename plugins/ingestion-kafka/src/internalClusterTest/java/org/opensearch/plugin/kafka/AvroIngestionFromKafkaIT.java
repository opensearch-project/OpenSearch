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
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.ByteArrayOutputStream;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.is;

/**
 * Integration tests for Avro payload decoding in the Kafka ingestion plugin.
 *
 * <p>Each test produces Avro-binary messages to Kafka, creates an OpenSearch index
 * configured with {@code avro.*} params, and verifies the decoded documents are
 * searchable with the expected field values.
 *
 * <p>The Avro schema encodes the full ingestion envelope:
 * {@code _id}, {@code _op_type}, and {@code _source} (a nested record).
 * After decoding, the JSON produced by {@link AvroPayloadDecoder} has the same
 * structure as the plain-JSON messages used in {@link IngestFromKafkaIT}, so no
 * changes to the ingestion pipeline are required.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class AvroIngestionFromKafkaIT extends KafkaIngestionBaseIT {

    /**
     * Full ingestion envelope schema.  The {@code _source} field is a nested record
     * so that after Avro→JSON conversion it becomes a proper JSON object, matching the
     * format the pipeline already expects from plain-JSON Kafka messages.
     */
    private static final String ENVELOPE_SCHEMA_JSON = "{"
        + "\"type\":\"record\","
        + "\"name\":\"KafkaDocument\","
        + "\"namespace\":\"org.opensearch.plugin.kafka\","
        + "\"fields\":["
        + "  {\"name\":\"_id\",\"type\":\"string\"},"
        + "  {\"name\":\"_op_type\",\"type\":\"string\"},"
        + "  {\"name\":\"_source\","
        + "   \"type\":{\"type\":\"record\",\"name\":\"Source\","
        + "            \"namespace\":\"org.opensearch.plugin.kafka\","
        + "            \"fields\":["
        + "              {\"name\":\"name\",\"type\":\"string\"},"
        + "              {\"name\":\"age\",\"type\":\"int\"}"
        + "            ]}}"
        + "]}";

    private static final String INDEX_MAPPING = "{\"properties\":{"
        + "\"name\":{\"type\":\"text\"},"
        + "\"age\":{\"type\":\"integer\"}}}";

    private static final Schema ENVELOPE_SCHEMA = new Schema.Parser().parse(ENVELOPE_SCHEMA_JSON);
    private static final Schema SOURCE_SCHEMA = ENVELOPE_SCHEMA.getField("_source").schema();

    private Producer<byte[], byte[]> avroProducer;

    @Before
    public void setupAvroProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", kafka.getBootstrapServers());
        avroProducer = new KafkaProducer<>(props, new ByteArraySerializer(), new ByteArraySerializer());
    }

    @After
    public void teardownAvroProducer() {
        if (avroProducer != null) {
            avroProducer.close();
        }
    }

    /** Encodes a document to Avro binary and publishes it to the test topic. */
    private void produceAvroMessage(String id, String name, int age) throws Exception {
        produceAvroMessage(id, "index", name, age, /* headerBytes= */ 0);
    }

    private void produceAvroMessage(String id, String opType, String name, int age, int headerBytes) throws Exception {
        GenericRecord source = new GenericData.Record(SOURCE_SCHEMA);
        source.put("name", name);
        source.put("age", age);

        GenericRecord envelope = new GenericData.Record(ENVELOPE_SCHEMA);
        envelope.put("_id", id);
        envelope.put("_op_type", opType);
        envelope.put("_source", source);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(new byte[headerBytes]); // prepend opaque header bytes for skip_bytes tests
        GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(ENVELOPE_SCHEMA);
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        writer.write(envelope, encoder);
        encoder.flush();

        avroProducer.send(new ProducerRecord<>(topicName, null, defaultMessageTimestamp, null, out.toByteArray()));
        avroProducer.flush();
    }

    private Settings.Builder avroIndexSettings(String extraAvroParams) {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("ingestion_source.type", "kafka")
            .put("ingestion_source.pointer.init.reset", "earliest")
            .put("ingestion_source.param.topic", topicName)
            .put("ingestion_source.param.bootstrap_servers", kafka.getBootstrapServers())
            .put("index.replication.type", "SEGMENT")
            .put("ingestion_source.param.avro.schema", ENVELOPE_SCHEMA_JSON);
    }

    // -----------------------------------------------------------------------
    // Tests
    // -----------------------------------------------------------------------

    /** Avro messages are decoded and indexed with the correct field values. */
    public void testAvroIngestionBasic() throws Exception {
        produceAvroMessage("1", "alice", 30);
        produceAvroMessage("2", "bob", 17);

        createIndex(indexName, avroIndexSettings("").build(), INDEX_MAPPING);
        ensureGreen(indexName);

        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            refresh(indexName);
            SearchResponse response = client().prepareSearch(indexName)
                .setQuery(new TermQueryBuilder("_id", "1"))
                .get();
            assertThat(response.getHits().getTotalHits().value(), is(1L));
            Object age = response.getHits().getHits()[0].getSourceAsMap().get("age");
            assertThat(age, is(30));
        });

        // Verify second document
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            refresh(indexName);
            SearchResponse response = client().prepareSearch(indexName)
                .setQuery(new TermQueryBuilder("_id", "2"))
                .get();
            assertThat(response.getHits().getTotalHits().value(), is(1L));
            Object name = response.getHits().getHits()[0].getSourceAsMap().get("name");
            assertThat(name, is("bob"));
        });
    }

    /**
     * When messages have an opaque header prefix, {@code avro.skip_bytes} causes the
     * decoder to strip it before deserializing the Avro payload.
     */
    public void testAvroIngestionWithSkipBytes() throws Exception {
        int headerSize = 5;
        produceAvroMessage("10", "index", "carol", 42, headerSize);

        Settings settings = avroIndexSettings("")
            .put("ingestion_source.param.avro.skip_bytes", headerSize)
            .build();
        createIndex(indexName, settings, INDEX_MAPPING);
        ensureGreen(indexName);

        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            refresh(indexName);
            SearchResponse response = client().prepareSearch(indexName)
                .setQuery(new TermQueryBuilder("_id", "10"))
                .get();
            assertThat(response.getHits().getTotalHits().value(), is(1L));
            assertThat(response.getHits().getHits()[0].getSourceAsMap().get("age"), is(42));
        });
    }

    /** Avro-encoded delete operations remove previously indexed documents. */
    public void testAvroIngestionDelete() throws Exception {
        produceAvroMessage("20", "index", "dave", 55, 0);
        produceAvroMessage("20", "delete", "dave", 55, 0);

        createIndex(indexName, avroIndexSettings("").build(), INDEX_MAPPING);
        ensureGreen(indexName);

        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            refresh(indexName);
            SearchResponse response = client().prepareSearch(indexName)
                .setQuery(new TermQueryBuilder("_id", "20"))
                .get();
            assertThat(response.getHits().getTotalHits().value(), is(0L));
        });
    }

    /**
     * A batch that contains a corrupted (un-decodeable) message should not stop
     * ingestion: the bad message is skipped and logged, and the remaining valid
     * messages in the same batch are indexed normally.
     */
    public void testCorruptMessageInBatchIsSkipped() throws Exception {
        // Valid message
        produceAvroMessage("30", "index", "eve", 28, 0);
        // Garbage bytes — not valid Avro binary for our schema
        avroProducer.send(new ProducerRecord<>(topicName, null, defaultMessageTimestamp, null, new byte[] { 0x00, 0x01, 0x02 }));
        avroProducer.flush();
        // Another valid message after the corrupt one
        produceAvroMessage("31", "index", "frank", 35, 0);

        createIndex(indexName, avroIndexSettings("").build(), INDEX_MAPPING);
        ensureGreen(indexName);

        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            refresh(indexName);
            // Both valid messages should be indexed
            SearchResponse response = client().prepareSearch(indexName).setSize(10).get();
            assertThat(response.getHits().getTotalHits().value(), is(2L));
        });
    }

    /**
     * Schema evolution: messages encoded with a schema that has an additional
     * optional field (union with null default) are decoded correctly when the
     * decoder is configured with the extended schema.
     */
    public void testSchemaEvolutionWithOptionalField() throws Exception {
        // Extended schema adds an optional "email" field with null default
        String extendedEnvelopeSchemaJson = "{"
            + "\"type\":\"record\","
            + "\"name\":\"KafkaDocument\","
            + "\"namespace\":\"org.opensearch.plugin.kafka\","
            + "\"fields\":["
            + "  {\"name\":\"_id\",\"type\":\"string\"},"
            + "  {\"name\":\"_op_type\",\"type\":\"string\"},"
            + "  {\"name\":\"_source\","
            + "   \"type\":{\"type\":\"record\",\"name\":\"Source\","
            + "            \"namespace\":\"org.opensearch.plugin.kafka\","
            + "            \"fields\":["
            + "              {\"name\":\"name\",\"type\":\"string\"},"
            + "              {\"name\":\"age\",\"type\":\"int\"},"
            + "              {\"name\":\"email\",\"type\":[\"null\",\"string\"],\"default\":null}"
            + "            ]}}"
            + "]}";

        Schema extSchema = new Schema.Parser().parse(extendedEnvelopeSchemaJson);
        Schema extSourceSchema = extSchema.getField("_source").schema();

        // Message with the email field set
        GenericRecord sourceWithEmail = new GenericData.Record(extSourceSchema);
        sourceWithEmail.put("name", "grace");
        sourceWithEmail.put("age", 40);
        sourceWithEmail.put("email", "grace@example.com");
        GenericRecord envelopeWithEmail = new GenericData.Record(extSchema);
        envelopeWithEmail.put("_id", "40");
        envelopeWithEmail.put("_op_type", "index");
        envelopeWithEmail.put("_source", sourceWithEmail);

        // Message with email null (absent)
        GenericRecord sourceNoEmail = new GenericData.Record(extSourceSchema);
        sourceNoEmail.put("name", "henry");
        sourceNoEmail.put("age", 22);
        sourceNoEmail.put("email", null);
        GenericRecord envelopeNoEmail = new GenericData.Record(extSchema);
        envelopeNoEmail.put("_id", "41");
        envelopeNoEmail.put("_op_type", "index");
        envelopeNoEmail.put("_source", sourceNoEmail);

        for (GenericRecord record : new GenericRecord[] { envelopeWithEmail, envelopeNoEmail }) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(extSchema);
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
            writer.write(record, encoder);
            encoder.flush();
            avroProducer.send(new ProducerRecord<>(topicName, null, defaultMessageTimestamp, null, out.toByteArray()));
        }
        avroProducer.flush();

        String extMapping = "{\"properties\":{"
            + "\"name\":{\"type\":\"text\"},"
            + "\"age\":{\"type\":\"integer\"},"
            + "\"email\":{\"type\":\"keyword\"}}}";

        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("ingestion_source.type", "kafka")
            .put("ingestion_source.pointer.init.reset", "earliest")
            .put("ingestion_source.param.topic", topicName)
            .put("ingestion_source.param.bootstrap_servers", kafka.getBootstrapServers())
            .put("index.replication.type", "SEGMENT")
            .put("ingestion_source.param.avro.schema", extendedEnvelopeSchemaJson)
            .build();
        createIndex(indexName, settings, extMapping);
        ensureGreen(indexName);

        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            refresh(indexName);
            assertThat(client().prepareSearch(indexName).setSize(0).get().getHits().getTotalHits().value(), is(2L));
        });

        // The document with the email field should have it set
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            refresh(indexName);
            SearchResponse response = client().prepareSearch(indexName)
                .setQuery(new TermQueryBuilder("_id", "40"))
                .get();
            assertThat(response.getHits().getTotalHits().value(), is(1L));
            assertThat(response.getHits().getHits()[0].getSourceAsMap().get("email"), is("grace@example.com"));
        });
    }
}
