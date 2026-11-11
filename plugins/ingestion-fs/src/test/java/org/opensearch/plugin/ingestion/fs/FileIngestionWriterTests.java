/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.ingestion.fs;

import org.opensearch.Version;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IngestionShardConsumer;
import org.opensearch.index.IngestionShardPointer;
import org.opensearch.index.IngestionWriter;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class FileIngestionWriterTests extends OpenSearchTestCase {

    public void testWritesAppendToThePartitionFileWithLineOffsets() throws Exception {
        Path baseDir = createTempDir();
        try (IngestionWriter writer = new FileIngestionWriter(config(baseDir))) {
            assertEquals(new FileOffset(0), write(writer, 1, "{\"_id\":\"a\"}"));
            assertEquals(new FileOffset(1), write(writer, 1, "{\"_id\":\"b\"}"));
            assertEquals(new FileOffset(0), write(writer, 0, "{\"_id\":\"c\"}"));
        }

        assertEquals(List.of("{\"_id\":\"a\"}", "{\"_id\":\"b\"}"), Files.readAllLines(baseDir.resolve("stream/1.ndjson")));
        assertEquals(List.of("{\"_id\":\"c\"}"), Files.readAllLines(baseDir.resolve("stream/0.ndjson")));
    }

    public void testOffsetsContinueAfterExistingLines() throws Exception {
        Path baseDir = createTempDir();
        Files.createDirectories(baseDir.resolve("stream"));
        Files.writeString(baseDir.resolve("stream/0.ndjson"), "{\"_id\":\"old1\"}\n{\"_id\":\"old2\"}\n");

        try (IngestionWriter writer = new FileIngestionWriter(config(baseDir))) {
            assertEquals(new FileOffset(2), write(writer, 0, "{\"_id\":\"new\"}"));
        }
    }

    public void testAnUnterminatedLastLineIsTerminatedFirst() throws Exception {
        Path baseDir = createTempDir();
        Files.createDirectories(baseDir.resolve("stream"));
        Files.writeString(baseDir.resolve("stream/0.ndjson"), "{\"_id\":\"a\"}");

        try (IngestionWriter writer = new FileIngestionWriter(config(baseDir))) {
            assertEquals(new FileOffset(1), write(writer, 0, "{\"_id\":\"b\"}"));
        }
        assertEquals(List.of("{\"_id\":\"a\"}", "{\"_id\":\"b\"}"), Files.readAllLines(baseDir.resolve("stream/0.ndjson")));
    }

    public void testNonUtf8ContentDoesNotBreakTheCount() throws Exception {
        Path baseDir = createTempDir();
        try (IngestionWriter writer = new FileIngestionWriter(config(baseDir))) {
            PlainActionFuture<IngestionShardPointer> future = new PlainActionFuture<>();
            writer.write(0, "key", new byte[] { (byte) 0xFF, (byte) 0xFE }, future);
            assertEquals(new FileOffset(0), future.get());
        }

        // a new writer counts the existing lines again
        try (IngestionWriter writer = new FileIngestionWriter(config(baseDir))) {
            assertEquals(new FileOffset(1), write(writer, 0, "{\"_id\":\"b\"}"));
        }
    }

    public void testWritersOnTheSameFileReportTheRightOffsets() throws Exception {
        // several writers in one JVM append to the same partition file, as coordinating nodes in a test cluster do
        Path baseDir = createTempDir();
        try (
            IngestionWriter first = new FileIngestionWriter(config(baseDir));
            IngestionWriter second = new FileIngestionWriter(config(baseDir))
        ) {
            assertEquals(new FileOffset(0), write(first, 0, "{\"_id\":\"a\"}"));
            assertEquals(new FileOffset(1), write(second, 0, "{\"_id\":\"b\"}"));
            assertEquals(new FileOffset(2), write(first, 0, "{\"_id\":\"c\"}"));
        }
        assertEquals(
            List.of("{\"_id\":\"a\"}", "{\"_id\":\"b\"}", "{\"_id\":\"c\"}"),
            Files.readAllLines(baseDir.resolve("stream/0.ndjson"))
        );
    }

    public void testTheConsumerReadsWhatTheWriterWrote() throws Exception {
        Path baseDir = createTempDir();
        try (IngestionWriter writer = new FileIngestionWriter(config(baseDir))) {
            write(writer, 0, "{\"_id\":\"a\"}");
            write(writer, 0, "{\"_id\":\"b\"}");
        }

        try (FilePartitionConsumer consumer = new FilePartitionConsumer(config(baseDir), 0)) {
            List<IngestionShardConsumer.ReadResult<FileOffset, FileMessage>> results = consumer.readNext(new FileOffset(0), true, 10, 1000);
            assertEquals(2, results.size());
            assertEquals(new FileOffset(1), results.get(1).getPointer());
            assertEquals("{\"_id\":\"b\"}", new String(results.get(1).getMessage().getPayload(), StandardCharsets.UTF_8));
        }
    }

    public void testAMultiLineMessageIsRejected() throws Exception {
        Path baseDir = createTempDir();
        try (IngestionWriter writer = new FileIngestionWriter(config(baseDir))) {
            ExecutionException e = expectThrows(ExecutionException.class, () -> write(writer, 0, "{\"_id\":\n\"a\"}"));
            assertTrue(e.getCause() instanceof IllegalArgumentException);
        }
        assertFalse(Files.exists(baseDir.resolve("stream/0.ndjson")));
    }

    public void testAWriteParamCannotReplaceAReaderParam() {
        Path sharedDir = createTempDir();
        Path writeDir = createTempDir();
        IndexMetadata indexMetadata = writeEnabledIndex(
            sharedDir,
            Settings.builder().put("index.ingestion_source.write.param.base_directory", writeDir.toString())
        );

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new FileWriterFactory().createWriter(indexMetadata)
        );
        assertTrue(e.getMessage().contains("[index.ingestion_source.write.param.base_directory]"));
        assertFalse(Files.exists(writeDir.resolve("stream/0.ndjson")));
    }

    public void testAWriteOnlyParamIsAddedAndWritesGoToTheFileTheShardReads() throws Exception {
        Path sharedDir = createTempDir();
        IndexMetadata indexMetadata = writeEnabledIndex(
            sharedDir,
            Settings.builder().put("index.ingestion_source.write.param.flush", "true")
        );

        try (IngestionWriter writer = new FileWriterFactory().createWriter(indexMetadata)) {
            write(writer, 0, "{\"_id\":\"a\"}");
        }
        assertTrue(Files.exists(sharedDir.resolve("stream/0.ndjson")));
    }

    private static IndexMetadata writeEnabledIndex(Path baseDir, Settings.Builder writeSettings) {
        return IndexMetadata.builder("idx")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.INGESTION_SOURCE_TYPE_SETTING.getKey(), FilePlugin.TYPE)
                    .put(IndexMetadata.INDEX_REPLICATION_TYPE_SETTING.getKey(), ReplicationType.SEGMENT)
                    .put("index.ingestion_source.param.stream", "stream")
                    .put("index.ingestion_source.param.base_directory", baseDir.toString())
                    .put(IndexMetadata.INGESTION_SOURCE_WRITE_ENABLED_SETTING.getKey(), true)
                    .put(writeSettings.build())
            )
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
    }

    public void testThePluginProvidesAFileWriter() {
        Map<String, ?> factories = new FilePlugin().getIngestionWriterFactories();
        assertEquals(1, factories.size());
        assertTrue(factories.get(FilePlugin.TYPE) instanceof FileWriterFactory);
    }

    private static FileSourceConfig config(Path baseDir) {
        return new FileSourceConfig(Map.of("stream", "stream", "base_directory", baseDir.toString()));
    }

    private static IngestionShardPointer write(IngestionWriter writer, int partition, String message) throws Exception {
        PlainActionFuture<IngestionShardPointer> future = new PlainActionFuture<>();
        writer.write(partition, "key", message.getBytes(StandardCharsets.UTF_8), future);
        return future.get();
    }
}
