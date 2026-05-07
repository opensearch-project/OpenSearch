/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.hive;

import com.carrotsearch.randomizedtesting.ThreadFilter;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.test.OpenSearchTestCase;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

@SuppressForbidden(reason = "Parquet APIs require java.io.File")
@ThreadLeakFilters(filters = ParquetHiveFileReaderTests.HadoopThreadLeakFilter.class)
public class ParquetHiveFileReaderTests extends OpenSearchTestCase {

    private Configuration hadoopConf;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        hadoopConf = new Configuration();
        hadoopConf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
    }

    public void testReadPrimitiveTypes() throws IOException {
        MessageType schema = MessageTypeParser.parseMessageType(
            "message test {\n"
                + "  optional boolean flag;\n"
                + "  optional int32 count;\n"
                + "  optional int64 total;\n"
                + "  optional float ratio;\n"
                + "  optional double score;\n"
                + "  optional binary name (UTF8);\n"
                + "}"
        );

        File file = writeParquet(schema, factory -> {
            Group g = factory.newGroup();
            g.append("flag", true);
            g.append("count", 42);
            g.append("total", 1000L);
            g.append("ratio", 3.14f);
            g.append("score", 9.99);
            g.append("name", "alice");
            return g;
        });

        try (ParquetHiveFileReader reader = new ParquetHiveFileReader(file.getAbsolutePath(), hadoopConf, schema)) {
            Map<String, Object> row = reader.readNext();
            assertNotNull(row);
            assertEquals(true, row.get("flag"));
            assertEquals(42, row.get("count"));
            assertEquals(1000L, row.get("total"));
            assertEquals(3.14f, (float) row.get("ratio"), 0.001f);
            assertEquals(9.99, (double) row.get("score"), 0.001);
            assertEquals("alice", row.get("name"));

            assertNull(reader.readNext());
        }
    }

    public void testReadBinaryNonString() throws IOException {
        MessageType schema = MessageTypeParser.parseMessageType("message test {\n  optional binary data;\n}");

        File file = writeParquet(schema, factory -> {
            Group g = factory.newGroup();
            g.append("data", Binary.fromConstantByteArray(new byte[] { 0x01, 0x02, 0x03 }));
            return g;
        });

        try (ParquetHiveFileReader reader = new ParquetHiveFileReader(file.getAbsolutePath(), hadoopConf, schema)) {
            Map<String, Object> row = reader.readNext();
            assertNotNull(row);
            // Non-string BINARY should be Base64 encoded
            String encoded = (String) row.get("data");
            byte[] decoded = java.util.Base64.getDecoder().decode(encoded);
            assertArrayEquals(new byte[] { 0x01, 0x02, 0x03 }, decoded);
        }
    }

    public void testReadNullValues() throws IOException {
        MessageType schema = MessageTypeParser.parseMessageType("message test {\n  optional binary name (UTF8);\n  optional int32 age;\n}");

        File file = writeParquet(schema, factory -> {
            // Only set name, leave age null
            Group g = factory.newGroup();
            g.append("name", "bob");
            return g;
        });

        try (ParquetHiveFileReader reader = new ParquetHiveFileReader(file.getAbsolutePath(), hadoopConf, schema)) {
            Map<String, Object> row = reader.readNext();
            assertNotNull(row);
            assertEquals("bob", row.get("name"));
            assertNull(row.get("age"));
        }
    }

    public void testReadRepeatedPrimitive() throws IOException {
        MessageType schema = MessageTypeParser.parseMessageType("message test {\n  repeated binary tag (UTF8);\n}");

        File file = writeParquet(schema, factory -> {
            Group g = factory.newGroup();
            g.append("tag", "a");
            g.append("tag", "b");
            g.append("tag", "c");
            return g;
        });

        try (ParquetHiveFileReader reader = new ParquetHiveFileReader(file.getAbsolutePath(), hadoopConf, schema)) {
            Map<String, Object> row = reader.readNext();
            assertNotNull(row);
            Object tags = row.get("tag");
            assertTrue(tags instanceof List);
            assertEquals(List.of("a", "b", "c"), tags);
        }
    }

    public void testReadMultipleRows() throws IOException {
        MessageType schema = MessageTypeParser.parseMessageType("message test {\n  optional int32 id;\n}");

        File file = writeParquetMultiple(schema, factory -> {
            Group[] groups = new Group[3];
            for (int i = 0; i < 3; i++) {
                groups[i] = factory.newGroup().append("id", i);
            }
            return groups;
        });

        try (ParquetHiveFileReader reader = new ParquetHiveFileReader(file.getAbsolutePath(), hadoopConf, schema)) {
            for (int i = 0; i < 3; i++) {
                Map<String, Object> row = reader.readNext();
                assertNotNull(row);
                assertEquals(i, row.get("id"));
            }
            assertNull(reader.readNext());
        }
    }

    @FunctionalInterface
    interface GroupFactory {
        Group create(SimpleGroupFactory factory);
    }

    @FunctionalInterface
    interface MultiGroupFactory {
        Group[] create(SimpleGroupFactory factory);
    }

    private File writeParquet(MessageType schema, GroupFactory groupFactory) throws IOException {
        File file = getTestParquetPath().toFile();
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);
        org.apache.parquet.io.OutputFile outputFile = new org.apache.parquet.io.LocalOutputFile(file.toPath());
        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile).withType(schema).build()) {
            writer.write(groupFactory.create(factory));
        }
        return file;
    }

    private File writeParquetMultiple(MessageType schema, MultiGroupFactory groupFactory) throws IOException {
        File file = getTestParquetPath().toFile();
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);
        org.apache.parquet.io.OutputFile outputFile = new org.apache.parquet.io.LocalOutputFile(file.toPath());
        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile).withType(schema).build()) {
            for (Group g : groupFactory.create(factory)) {
                writer.write(g);
            }
        }
        return file;
    }

    private java.nio.file.Path getTestParquetPath() throws IOException {
        return createTempDir().resolve("test.parquet");
    }

    public static final class HadoopThreadLeakFilter implements ThreadFilter {
        @Override
        public boolean reject(Thread t) {
            return t.getName().contains("StatisticsDataReferenceCleaner");
        }
    }
}
