/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.plugin.arrow;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.Murmur3HashFunction;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.IngestionShardConsumer.ReadResult;
import org.opensearch.test.OpenSearchTestCase;

public class ArrowShardConsumerTests extends OpenSearchTestCase {

    private static final int NUM_SHARDS = 3;
    private static final String SHARDING_KEY = "id";

    private static Schema schemaWithColumns(Field... extra) {
        List<Field> fields = new ArrayList<>();
        fields.add(new Field(SHARDING_KEY, FieldType.nullable(new ArrowType.Utf8()), null));
        for (Field f : extra) {
            fields.add(f);
        }
        return new Schema(fields);
    }

    private static int shardFor(String routingKey) {
        return Math.floorMod(Murmur3HashFunction.hash(routingKey), NUM_SHARDS);
    }

    private static ArrowIngestionConfig ingestionConfig(Map<String, Object> extraParams) {
        Map<String, Object> params = new HashMap<>();
        params.put(ArrowIngestionConfig.DATASET_TYPE_PROP_KEY, "arrow_ipc");
        params.put(ArrowIngestionConfig.SHARDING_KEY_PROP_KEY, SHARDING_KEY);
        params.putAll(extraParams);
        IndexMetadata indexMetadata = IndexMetadata.builder("test-index")
                .settings(settings(Version.CURRENT))
                .numberOfShards(NUM_SHARDS)
                .numberOfReplicas(0)
                .build();
        return new ArrowIngestionConfig(params, indexMetadata);
    }

    private static ArrowSource sourceFrom(VectorSchemaRoot root) throws IOException {
        byte[] ipcBytes = ArrowTestUtils.writeIpc(root);
        SeekableByteChannel channel = new ByteArrayReadableSeekableByteChannel(ipcBytes);
        return ArrowIpcSource.fromChannel(channel, 555L, "v1");
    }

    private static Map<String, Object> parsePayload(byte[] payload) throws IOException {
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY, DeprecationHandler.IGNORE_DEPRECATIONS, payload)) {
            return parser.map();
        }
    }

    public void testReadNextParsesFieldsAndFiltersByShard() throws IOException, TimeoutException {
        BufferAllocator allocator = new RootAllocator();
        Schema schema = schemaWithColumns(
                new Field("name", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("score", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null),
                new Field("active", FieldType.nullable(new ArrowType.Bool()), null));
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        String[] ids = { "row-0", "row-1", "row-2", "row-3", "row-4" };
        VarCharVector idVector = (VarCharVector) root.getVector(SHARDING_KEY);
        VarCharVector nameVector = (VarCharVector) root.getVector("name");
        IntVector ageVector = (IntVector) root.getVector("age");
        Float8Vector scoreVector = (Float8Vector) root.getVector("score");
        BitVector activeVector = (BitVector) root.getVector("active");
        idVector.allocateNew(ids.length);
        nameVector.allocateNew(ids.length);
        ageVector.allocateNew(ids.length);
        scoreVector.allocateNew(ids.length);
        activeVector.allocateNew(ids.length);
        for (int i = 0; i < ids.length; i++) {
            idVector.setSafe(i, ids[i].getBytes(StandardCharsets.UTF_8));
            nameVector.setSafe(i, ("name-" + i).getBytes(StandardCharsets.UTF_8));
            ageVector.setSafe(i, 20 + i);
            scoreVector.setSafe(i, i * 1.5);
            activeVector.setSafe(i, i % 2);
        }
        root.setRowCount(ids.length);

        try (ArrowSource source = sourceFrom(root)) {
            for (int shardId = 0; shardId < NUM_SHARDS; shardId++) {
                ArrowIngestionConfig config = ingestionConfig(Map.of());
                ArrowShardConsumer consumer = new ArrowShardConsumer(shardId, source, config);
                List<ReadResult<ArrowOffset, ArrowMessage>> results = consumer.readNext(100, 1000);

                List<Integer> expectedRows = new ArrayList<>();
                for (int i = 0; i < ids.length; i++) {
                    if (shardFor(ids[i]) == shardId) {
                        expectedRows.add(i);
                    }
                }

                assertEquals("shard " + shardId, expectedRows.size(), results.size());
                for (int j = 0; j < expectedRows.size(); j++) {
                    int rowIndex = expectedRows.get(j);
                    ReadResult<ArrowOffset, ArrowMessage> result = results.get(j);
                    assertEquals(rowIndex, result.getPointer().getOffset());
                    Map<String, Object> payload = parsePayload(result.getMessage().getPayload());
                    assertEquals(String.valueOf(rowIndex), payload.get("_id"));
                    assertEquals("v1", payload.get("_version"));
                    assertEquals("index", payload.get("_op_type"));
                    @SuppressWarnings("unchecked")
                    Map<String, Object> source1 = (Map<String, Object>) payload.get("_source");
                    assertEquals(ids[rowIndex], source1.get(SHARDING_KEY));
                    assertEquals("name-" + rowIndex, source1.get("name"));
                    assertEquals(20 + rowIndex, ((Number) source1.get("age")).intValue());
                    assertEquals(rowIndex * 1.5, ((Number) source1.get("score")).doubleValue(), 0.0001);
                    assertEquals(rowIndex % 2 != 0, source1.get("active"));
                }
            }
        }
        root.close();
        allocator.close();
    }

    public void testNullValuesAreOmittedFromSource() throws IOException, TimeoutException {
        BufferAllocator allocator = new RootAllocator();
        Schema schema = schemaWithColumns(new Field("name", FieldType.nullable(new ArrowType.Utf8()), null));
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        VarCharVector idVector = (VarCharVector) root.getVector(SHARDING_KEY);
        VarCharVector nameVector = (VarCharVector) root.getVector("name");
        idVector.allocateNew(1);
        nameVector.allocateNew(1);
        idVector.setSafe(0, "only-row".getBytes(StandardCharsets.UTF_8));
        nameVector.setNull(0);
        root.setRowCount(1);

        try (ArrowSource source = sourceFrom(root)) {
            int shardId = shardFor("only-row");
            ArrowShardConsumer consumer = new ArrowShardConsumer(shardId, source, ingestionConfig(Map.of()));
            List<ReadResult<ArrowOffset, ArrowMessage>> results = consumer.readNext(10, 1000);
            assertEquals(1, results.size());
            Map<String, Object> payload = parsePayload(results.get(0).getMessage().getPayload());
            @SuppressWarnings("unchecked")
            Map<String, Object> source1 = (Map<String, Object>) payload.get("_source");
            assertFalse(source1.containsKey("name"));
        }
        root.close();
        allocator.close();
    }

    public void testListColumnParsedAsArray() throws IOException, TimeoutException {
        BufferAllocator allocator = new RootAllocator();
        Field tagsField = new Field(
                "tags",
                FieldType.nullable(new ArrowType.List()),
                List.of(new Field("item", FieldType.nullable(new ArrowType.Utf8()), null)));
        Schema schema = schemaWithColumns(tagsField);
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        VarCharVector idVector = (VarCharVector) root.getVector(SHARDING_KEY);
        ListVector tagsVector = (ListVector) root.getVector("tags");
        idVector.allocateNew(1);
        tagsVector.allocateNew();
        idVector.setSafe(0, "list-row".getBytes(StandardCharsets.UTF_8));

        org.apache.arrow.vector.complex.impl.UnionListWriter writer = tagsVector.getWriter();
        writer.setPosition(0);
        writer.startList();
        writer.varChar().writeVarChar("a");
        writer.varChar().writeVarChar("b");
        writer.endList();
        writer.setValueCount(1);
        root.setRowCount(1);

        try (ArrowSource source = sourceFrom(root)) {
            int shardId = shardFor("list-row");
            ArrowShardConsumer consumer = new ArrowShardConsumer(shardId, source, ingestionConfig(Map.of()));
            List<ReadResult<ArrowOffset, ArrowMessage>> results = consumer.readNext(10, 1000);
            assertEquals(1, results.size());
            Map<String, Object> payload = parsePayload(results.get(0).getMessage().getPayload());
            @SuppressWarnings("unchecked")
            Map<String, Object> source1 = (Map<String, Object>) payload.get("_source");
            @SuppressWarnings("unchecked")
            List<Object> tags = (List<Object>) source1.get("tags");
            assertEquals(List.of("a", "b"), tags);
        }
        root.close();
        allocator.close();
    }

    public void testIdColumnConfiguredUsesColumnValueAsId() throws IOException, TimeoutException {
        BufferAllocator allocator = new RootAllocator();
        Schema schema = schemaWithColumns(new Field("doc_id", FieldType.nullable(new ArrowType.Utf8()), null));
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        VarCharVector idVector = (VarCharVector) root.getVector(SHARDING_KEY);
        VarCharVector docIdVector = (VarCharVector) root.getVector("doc_id");
        idVector.allocateNew(1);
        docIdVector.allocateNew(1);
        idVector.setSafe(0, "row-a".getBytes(StandardCharsets.UTF_8));
        docIdVector.setSafe(0, "custom-id-1".getBytes(StandardCharsets.UTF_8));
        root.setRowCount(1);

        try (ArrowSource source = sourceFrom(root)) {
            int shardId = shardFor("row-a");
            ArrowShardConsumer consumer = new ArrowShardConsumer(
                    shardId, source, ingestionConfig(Map.of(ArrowIngestionConfig.ID_COLUMN, "doc_id")));
            List<ReadResult<ArrowOffset, ArrowMessage>> results = consumer.readNext(10, 1000);
            assertEquals(1, results.size());
            Map<String, Object> payload = parsePayload(results.get(0).getMessage().getPayload());
            assertEquals("custom-id-1", payload.get("_id"));
        }
        root.close();
        allocator.close();
    }

    public void testNullIdColumnValueThrows() throws IOException {
        BufferAllocator allocator = new RootAllocator();
        Schema schema = schemaWithColumns(new Field("doc_id", FieldType.nullable(new ArrowType.Utf8()), null));
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        VarCharVector idVector = (VarCharVector) root.getVector(SHARDING_KEY);
        VarCharVector docIdVector = (VarCharVector) root.getVector("doc_id");
        idVector.allocateNew(1);
        docIdVector.allocateNew(1);
        idVector.setSafe(0, "row-a".getBytes(StandardCharsets.UTF_8));
        docIdVector.setNull(0);
        root.setRowCount(1);

        try (ArrowSource source = sourceFrom(root)) {
            int shardId = shardFor("row-a");
            ArrowShardConsumer consumer = new ArrowShardConsumer(
                    shardId, source, ingestionConfig(Map.of(ArrowIngestionConfig.ID_COLUMN, "doc_id")));
            expectThrows(IllegalArgumentException.class, () -> consumer.readNext(10, 1000));
        }
        root.close();
        allocator.close();
    }

    public void testUnknownShardingKeyThrows() throws IOException {
        BufferAllocator allocator = new RootAllocator();
        Schema schema = schemaWithColumns();
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        VarCharVector idVector = (VarCharVector) root.getVector(SHARDING_KEY);
        idVector.allocateNew(1);
        idVector.setSafe(0, "row-a".getBytes(StandardCharsets.UTF_8));
        root.setRowCount(1);

        try (ArrowSource source = sourceFrom(root)) {
            Map<String, Object> params = new HashMap<>();
            params.put(ArrowIngestionConfig.DATASET_TYPE_PROP_KEY, "arrow_ipc");
            params.put(ArrowIngestionConfig.SHARDING_KEY_PROP_KEY, "does_not_exist");
            IndexMetadata indexMetadata = IndexMetadata.builder("test-index")
                    .settings(settings(Version.CURRENT))
                    .numberOfShards(NUM_SHARDS)
                    .numberOfReplicas(0)
                    .build();
            ArrowIngestionConfig config = new ArrowIngestionConfig(params, indexMetadata);
            ArrowShardConsumer consumer = new ArrowShardConsumer(0, source, config);
            expectThrows(IllegalArgumentException.class, () -> consumer.readNext(10, 1000));
        }
        root.close();
        allocator.close();
    }

    public void testNullShardingKeyValueThrows() throws IOException {
        BufferAllocator allocator = new RootAllocator();
        Schema schema = schemaWithColumns();
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        VarCharVector idVector = (VarCharVector) root.getVector(SHARDING_KEY);
        idVector.allocateNew(1);
        idVector.setNull(0);
        root.setRowCount(1);

        try (ArrowSource source = sourceFrom(root)) {
            ArrowShardConsumer consumer = new ArrowShardConsumer(0, source, ingestionConfig(Map.of()));
            expectThrows(IllegalArgumentException.class, () -> consumer.readNext(10, 1000));
        }
        root.close();
        allocator.close();
    }

    public void testRowBoundLimitsScannedRows() throws IOException, TimeoutException {
        BufferAllocator allocator = new RootAllocator();
        Schema schema = schemaWithColumns();
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        VarCharVector idVector = (VarCharVector) root.getVector(SHARDING_KEY);
        String[] ids = { "a", "b", "c", "d", "e" };
        idVector.allocateNew(ids.length);
        for (int i = 0; i < ids.length; i++) {
            idVector.setSafe(i, ids[i].getBytes(StandardCharsets.UTF_8));
        }
        root.setRowCount(ids.length);

        try (ArrowSource source = sourceFrom(root)) {
            ArrowIngestionConfig config = ingestionConfig(Map.of(ArrowIngestionConfig.ROW_BOUND, "2"));
            // earliestPointer/latestPointer are shard-agnostic in this implementation, so
            // verify the bound regardless of which shard 'this' consumer represents.
            ArrowShardConsumer consumer = new ArrowShardConsumer(0, source, config);
            assertEquals(0L, ((ArrowOffset) consumer.earliestPointer()).getOffset());
            assertEquals(2L, ((ArrowOffset) consumer.latestPointer()).getOffset());
        }
        root.close();
        allocator.close();
    }

    public void testPointerFromOffsetAndLag() throws IOException {
        BufferAllocator allocator = new RootAllocator();
        Schema schema = schemaWithColumns();
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        VarCharVector idVector = (VarCharVector) root.getVector(SHARDING_KEY);
        idVector.allocateNew(1);
        idVector.setSafe(0, "a".getBytes(StandardCharsets.UTF_8));
        root.setRowCount(1);

        try (ArrowSource source = sourceFrom(root)) {
            ArrowShardConsumer consumer = new ArrowShardConsumer(0, source, ingestionConfig(Map.of()));
            ArrowOffset parsed = (ArrowOffset) consumer.pointerFromOffset("5");
            assertEquals(5L, parsed.getOffset());

            // dataset has 1 row, so latestPointer (rowBound) is 1; lag from start pointer 0 is 1.
            long lag = consumer.getPointerBasedLag(new ArrowOffset(0));
            assertEquals(1L, lag);

            assertEquals(0, consumer.getShardId());
            consumer.close();
        }
        root.close();
        allocator.close();
    }

    public void testUnsupportedPointerFromTimestamp() throws IOException {
        BufferAllocator allocator = new RootAllocator();
        Schema schema = schemaWithColumns();
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        VarCharVector idVector = (VarCharVector) root.getVector(SHARDING_KEY);
        idVector.allocateNew(1);
        idVector.setSafe(0, "a".getBytes(StandardCharsets.UTF_8));
        root.setRowCount(1);

        try (ArrowSource source = sourceFrom(root)) {
            ArrowShardConsumer consumer = new ArrowShardConsumer(0, source, ingestionConfig(Map.of()));
            expectThrows(UnsupportedOperationException.class, () -> consumer.pointerFromTimestampMillis(0));
        }
        root.close();
        allocator.close();
    }
}
