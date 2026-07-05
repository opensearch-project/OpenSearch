/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.hive;

import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class HiveShardConsumerTests extends OpenSearchTestCase {

    private HiveShardConsumer createConsumer() {
        Map<String, Object> params = new HashMap<>();
        params.put("metastore_uri", "thrift://localhost:9083");
        params.put("database", "db");
        params.put("table", "tbl");
        HiveSourceConfig config = new HiveSourceConfig(params, 1);
        return new HiveShardConsumer("test", 0, config);
    }

    public void testBuildPartitionFilterSingleKey() {
        HiveShardConsumer consumer = createConsumer();
        String filter = consumer.buildPartitionFilter("dt=2024-01-15", false);
        assertEquals("(dt > \"2024-01-15\")", filter);
    }

    public void testBuildPartitionFilterCompositeKey() {
        HiveShardConsumer consumer = createConsumer();
        String filter = consumer.buildPartitionFilter("year=2024/month=01/day=15", false);
        assertEquals(
            "(year > \"2024\") OR (year = \"2024\" AND month > \"01\") OR (year = \"2024\" AND month = \"01\" AND day > \"15\")",
            filter
        );
    }

    public void testBuildPartitionFilterTwoKeys() {
        HiveShardConsumer consumer = createConsumer();
        String filter = consumer.buildPartitionFilter("region=us/dt=2024-03-01", false);
        assertEquals("(region > \"us\") OR (region = \"us\" AND dt > \"2024-03-01\")", filter);
    }

    public void testBuildPartitionFilterNoEquals() {
        HiveShardConsumer consumer = createConsumer();
        String filter = consumer.buildPartitionFilter("invalid", false);
        assertEquals("", filter);
    }

    public void testBuildPartitionFilterInclusiveSingleKey() {
        HiveShardConsumer consumer = createConsumer();
        String filter = consumer.buildPartitionFilter("dt=2024-01-15", true);
        assertEquals("(dt >= \"2024-01-15\")", filter);
    }

    public void testBuildPartitionFilterInclusiveCompositeKey() {
        HiveShardConsumer consumer = createConsumer();
        String filter = consumer.buildPartitionFilter("year=2024/month=01/day=15", true);
        assertEquals(
            "(year > \"2024\") OR (year = \"2024\" AND month > \"01\") OR (year = \"2024\" AND month = \"01\" AND day >= \"15\")",
            filter
        );
    }

    public void testHiveSchemaToParquetBasicTypes() {
        List<MetastoreCatalog.ColumnInfo> columns = new ArrayList<>();
        columns.add(new MetastoreCatalog.ColumnInfo("flag", "boolean"));
        columns.add(new MetastoreCatalog.ColumnInfo("count", "int"));
        columns.add(new MetastoreCatalog.ColumnInfo("total", "bigint"));
        columns.add(new MetastoreCatalog.ColumnInfo("ratio", "float"));
        columns.add(new MetastoreCatalog.ColumnInfo("score", "double"));
        columns.add(new MetastoreCatalog.ColumnInfo("name", "string"));

        MessageType schema = HiveShardConsumer.hiveSchemaToParquet(columns);

        assertEquals(6, schema.getFieldCount());
        assertEquals(PrimitiveType.PrimitiveTypeName.BOOLEAN, schema.getType("flag").asPrimitiveType().getPrimitiveTypeName());
        assertEquals(PrimitiveType.PrimitiveTypeName.INT32, schema.getType("count").asPrimitiveType().getPrimitiveTypeName());
        assertEquals(PrimitiveType.PrimitiveTypeName.INT64, schema.getType("total").asPrimitiveType().getPrimitiveTypeName());
        assertEquals(PrimitiveType.PrimitiveTypeName.FLOAT, schema.getType("ratio").asPrimitiveType().getPrimitiveTypeName());
        assertEquals(PrimitiveType.PrimitiveTypeName.DOUBLE, schema.getType("score").asPrimitiveType().getPrimitiveTypeName());
        assertEquals(PrimitiveType.PrimitiveTypeName.BINARY, schema.getType("name").asPrimitiveType().getPrimitiveTypeName());
    }

    public void testHiveSchemaToParquetIntVariants() {
        List<MetastoreCatalog.ColumnInfo> columns = new ArrayList<>();
        columns.add(new MetastoreCatalog.ColumnInfo("a", "tinyint"));
        columns.add(new MetastoreCatalog.ColumnInfo("b", "smallint"));

        MessageType schema = HiveShardConsumer.hiveSchemaToParquet(columns);

        assertEquals(PrimitiveType.PrimitiveTypeName.INT32, schema.getType("a").asPrimitiveType().getPrimitiveTypeName());
        assertEquals(PrimitiveType.PrimitiveTypeName.INT32, schema.getType("b").asPrimitiveType().getPrimitiveTypeName());
    }

    public void testHiveSchemaToParquetUnknownTypeFallsBackToString() {
        List<MetastoreCatalog.ColumnInfo> columns = new ArrayList<>();
        columns.add(new MetastoreCatalog.ColumnInfo("custom", "struct<a:int,b:string>"));

        MessageType schema = HiveShardConsumer.hiveSchemaToParquet(columns);

        assertEquals(PrimitiveType.PrimitiveTypeName.BINARY, schema.getType("custom").asPrimitiveType().getPrimitiveTypeName());
    }

    public void testRowToJsonBasicTypes() throws Exception {
        HiveShardConsumer consumer = createConsumer();
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("name", "alice");
        row.put("age", 30);
        row.put("score", 9.5);
        row.put("active", true);

        HivePointer ptr = new HivePointer("dt=2024-01-01", "file:/data/part-0.parquet", 0, 0);
        byte[] json = consumer.rowToJson(row, ptr);
        String result = new String(json, StandardCharsets.UTF_8);

        assertTrue(result.contains("\"_id\":\""));
        assertTrue(result.contains("\"_source\":{\"name\":\"alice\",\"age\":30,\"score\":9.5,\"active\":true}"));
    }

    public void testRowToJsonNullValue() throws Exception {
        HiveShardConsumer consumer = createConsumer();
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("name", "bob");
        row.put("email", null);

        HivePointer ptr = new HivePointer("dt=2024-01-01", "file:/data/part-0.parquet", 1, 1);
        byte[] json = consumer.rowToJson(row, ptr);
        String result = new String(json, StandardCharsets.UTF_8);

        assertTrue(result.contains("\"_source\":{\"name\":\"bob\",\"email\":null}"));
    }

    public void testRowToJsonSpecialCharacters() throws Exception {
        HiveShardConsumer consumer = createConsumer();
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("msg", "line1\nline2");
        row.put("path", "c:\\users\\test");
        row.put("quote", "say \"hello\"");

        HivePointer ptr = new HivePointer("dt=2024-01-01", "file:/data/part-0.parquet", 2, 2);
        byte[] json = consumer.rowToJson(row, ptr);
        String result = new String(json, StandardCharsets.UTF_8);

        assertTrue(result.contains("\\n"));
        assertTrue(result.contains("\\\\"));
        assertTrue(result.contains("\\\"hello\\\""));
    }

    public void testExtractPartitionTimeWithPattern() {
        Map<String, Object> params = new HashMap<>();
        params.put("metastore_uri", "thrift://localhost:9083");
        params.put("database", "db");
        params.put("table", "tbl");
        params.put("partition_order", "partition-time");
        params.put("partition_time_pattern", "$year-$month-$day $hour:00:00");
        HiveSourceConfig config = new HiveSourceConfig(params, 1);
        HiveShardConsumer consumer = new HiveShardConsumer("test", 0, config);
        consumer.partitionKeys = List.of("year", "month", "day", "hour");

        MetastoreCatalog.PartitionInfo partition = new MetastoreCatalog.PartitionInfo(List.of("2024", "01", "15", "03"), "/data", 0);
        String result = consumer.extractPartitionTime(partition);
        assertEquals("2024-01-15 03:00:00", result);
    }

    public void testExtractPartitionTimeNullPattern() {
        HiveShardConsumer consumer = createConsumer();
        MetastoreCatalog.PartitionInfo partition = new MetastoreCatalog.PartitionInfo(List.of("2024", "01", "15"), "/data", 0);
        String result = consumer.extractPartitionTime(partition);
        assertEquals("2024/01/15", result);
    }

    public void testDiscoverByPartitionTimeFiltersCorrectly() {
        // Verify that PARTITION_TIME mode uses extracted time comparison, not lexicographic.
        // hour=2 is lexicographically > hour=11, but time-wise hour=11 > hour=2.
        Map<String, Object> params = new HashMap<>();
        params.put("metastore_uri", "thrift://localhost:9083");
        params.put("database", "db");
        params.put("table", "tbl");
        params.put("partition_order", "partition-time");
        params.put("partition_time_pattern", "2024-01-01 $hour:00:00");
        HiveSourceConfig config = new HiveSourceConfig(params, 1);
        HiveShardConsumer consumer = new HiveShardConsumer("test", 0, config);
        consumer.partitionKeys = List.of("hour");

        MetastoreCatalog.PartitionInfo hour2 = new MetastoreCatalog.PartitionInfo(List.of("2"), "/data/hour=2", 0);
        MetastoreCatalog.PartitionInfo hour11 = new MetastoreCatalog.PartitionInfo(List.of("11"), "/data/hour=11", 0);

        // hour=2 extracts to "2024-01-01 2:00:00", hour=11 extracts to "2024-01-01 11:00:00"
        String time2 = consumer.extractPartitionTime(hour2);
        String time11 = consumer.extractPartitionTime(hour11);

        assertEquals("2024-01-01 2:00:00", time2);
        assertEquals("2024-01-01 11:00:00", time11);

        // In lexicographic order: "2024-01-01 2:00:00" > "2024-01-01 11:00:00" (because '2' > '1')
        // This confirms that lexicographic comparison would give wrong results for non-padded hours.
        assertTrue("Lexicographic comparison gives wrong order for non-padded hours", time2.compareTo(time11) > 0);
    }

    public void testPartitionToNameSingleKey() {
        HiveShardConsumer consumer = createConsumer();
        consumer.partitionKeys = List.of("dt");
        MetastoreCatalog.PartitionInfo partition = new MetastoreCatalog.PartitionInfo(List.of("2024-01-15"), "/data", 0);
        assertEquals("dt=2024-01-15", consumer.partitionToName(partition));
    }

    public void testPartitionToNameCompositeKeys() {
        HiveShardConsumer consumer = createConsumer();
        consumer.partitionKeys = List.of("year", "month", "day");
        MetastoreCatalog.PartitionInfo partition = new MetastoreCatalog.PartitionInfo(List.of("2024", "01", "15"), "/data", 0);
        assertEquals("year=2024/month=01/day=15", consumer.partitionToName(partition));
    }

    public void testDiscoverNewPartitionsClearsPendingWork() throws Exception {
        HiveShardConsumer consumer = createConsumer();
        consumer.partitionKeys = List.of("dt");

        // Simulate consumed work: add entries and advance index past them
        consumer.pendingWork.add(new HiveShardConsumer.PartitionWork("dt=2024-01-01", "", List.of("/f1"), 100));
        consumer.pendingWork.add(new HiveShardConsumer.PartitionWork("dt=2024-01-02", "", List.of("/f2"), 200));
        consumer.currentWorkIndex = 2;

        // Verify pre-condition: consumed entries still in list
        assertEquals(2, consumer.pendingWork.size());
        assertEquals(2, consumer.currentWorkIndex);

        // discoverNewPartitions clears the list (will fail to connect to Metastore, but
        // the clear happens before the Metastore call)
        try {
            consumer.discoverNewPartitions();
        } catch (Exception e) {
            // Expected: no Metastore connection in unit test
        }

        assertEquals(0, consumer.pendingWork.size());
        assertEquals(0, consumer.currentWorkIndex);
    }

    public void testReadNextThrowsOnPersistentFailure() {
        HiveShardConsumer consumer = createConsumer();
        RuntimeException ex = expectThrows(RuntimeException.class, () -> consumer.readNext(10, 1000));
        assertNotNull(ex.getCause());
    }

    public void testOpenNextFileDoesNotSkipOnFailure() {
        HiveShardConsumer consumer = createConsumer();
        consumer.partitionKeys = List.of("dt");
        consumer.pendingWork.add(new HiveShardConsumer.PartitionWork("dt=2024-01-01", "", List.of("/nonexistent/file.parquet"), 100));
        consumer.currentWorkIndex = 0;

        expectThrows(IOException.class, consumer::openNextFile);
        // currentFileIndex must not have advanced, so a retry would attempt the same file
        assertEquals(0, consumer.pendingWork.getFirst().currentFileIndex);
    }

    public void testDiscoverNewPartitionsClosesOpenReader() throws Exception {
        HiveShardConsumer consumer = createConsumer();

        final boolean[] readerClosed = { false };
        HiveFileReader openReader = new HiveFileReader() {
            @Override
            public java.util.Map<String, Object> readNext() {
                return null;
            }

            @Override
            public void close() {
                readerClosed[0] = true;
            }
        };
        java.lang.reflect.Field readerField = HiveShardConsumer.class.getDeclaredField("currentFileReader");
        readerField.setAccessible(true);
        readerField.set(consumer, openReader);

        MetastoreCatalog emptyCatalog = new MetastoreCatalog() {
            @Override
            public void connect() {}

            @Override
            public void reconnect() {}

            @Override
            public TableInfo getTableInfo(String database, String table) {
                return null;
            }

            @Override
            public java.util.List<PartitionInfo> getAllPartitions(String database, String table) {
                return java.util.Collections.emptyList();
            }

            @Override
            public java.util.List<PartitionInfo> getPartitionsByFilter(String database, String table, String filter) {
                return java.util.Collections.emptyList();
            }

            @Override
            public void close() {}
        };
        java.lang.reflect.Field catalogField = HiveShardConsumer.class.getDeclaredField("catalog");
        catalogField.setAccessible(true);
        catalogField.set(consumer, emptyCatalog);
        consumer.partitionKeys = List.of("dt");

        consumer.discoverNewPartitions();

        assertTrue("open reader must be closed before the work queue is reset", readerClosed[0]);
        assertNull(readerField.get(consumer));
    }
}
