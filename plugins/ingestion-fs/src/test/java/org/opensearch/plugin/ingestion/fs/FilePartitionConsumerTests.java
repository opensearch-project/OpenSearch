/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.ingestion.fs;

import org.opensearch.index.IngestionShardConsumer;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Map;

public class FilePartitionConsumerTests extends OpenSearchTestCase {

    public void testPartialLineIsReadOnceComplete() throws Exception {
        Path baseDir = createTempDir();
        Path file = baseDir.resolve("stream/0.ndjson");
        Files.createDirectories(file.getParent());
        Files.writeString(file, "{\"_id\":");

        try (FilePartitionConsumer consumer = new FilePartitionConsumer(config(baseDir), 0)) {
            // the line has no newline yet, as when a write is still in progress
            assertTrue(consumer.readNext(new FileOffset(0), true, 10, 1000).isEmpty());

            Files.writeString(file, "\"a\"}\n{\"_id\":\"b\"}\n", StandardOpenOption.APPEND);
            List<IngestionShardConsumer.ReadResult<FileOffset, FileMessage>> results = consumer.readNext(10, 1000);
            assertEquals(2, results.size());
            assertEquals(new FileOffset(0), results.get(0).getPointer());
            assertEquals("{\"_id\":\"a\"}", payload(results.get(0)));
            assertEquals(new FileOffset(1), results.get(1).getPointer());
            assertEquals("{\"_id\":\"b\"}", payload(results.get(1)));
        }
    }

    public void testLatestPointerIgnoresPartialLine() throws Exception {
        Path baseDir = createTempDir();
        Path file = baseDir.resolve("stream/0.ndjson");
        Files.createDirectories(file.getParent());
        Files.writeString(file, "{\"_id\":\"a\"}\n{\"_id\":");

        try (FilePartitionConsumer consumer = new FilePartitionConsumer(config(baseDir), 0)) {
            assertEquals(new FileOffset(1), consumer.latestPointer());
        }
    }

    private static String payload(IngestionShardConsumer.ReadResult<FileOffset, FileMessage> result) {
        return new String(result.getMessage().getPayload(), StandardCharsets.UTF_8);
    }

    private static FileSourceConfig config(Path baseDir) {
        return new FileSourceConfig(Map.of("stream", "stream", "base_directory", baseDir.toString()));
    }
}
