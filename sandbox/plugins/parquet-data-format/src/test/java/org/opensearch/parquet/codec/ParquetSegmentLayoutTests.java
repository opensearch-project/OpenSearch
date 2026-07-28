/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.Version;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

/**
 * Unit tests for {@link ParquetSegmentLayout} (task 4.3): attribute-based resolution, the
 * directory-scan fallback (including the sibling {@code parquet/} directory), and the
 * "no Parquet file" case.
 */
public class ParquetSegmentLayoutTests extends OpenSearchTestCase {

    public void testResolveByAttribute() throws Exception {
        Path dir = createTempDir();
        Path parquet = dir.resolve("_parquet_file_generation_1.parquet");
        Files.write(parquet, new byte[] { 1, 2, 3 });

        try (FSDirectory directory = FSDirectory.open(dir)) {
            SegmentReadState state = newState(directory, Map.of(ParquetSegmentLayout.PARQUET_FILE_ATTRIBUTE, parquet.toString()));
            Path resolved = ParquetSegmentLayout.resolve(state);
            assertNotNull(resolved);
            assertEquals(parquet.toString(), resolved.toString());
        }
    }

    public void testResolveBySiblingParquetDirectory() throws Exception {
        Path dir = createTempDir();
        Path parquetDir = dir.resolve("parquet");
        Files.createDirectories(parquetDir);
        Path parquet = parquetDir.resolve("_parquet_file_generation_a.parquet");
        Files.write(parquet, new byte[] { 1 });

        try (FSDirectory directory = FSDirectory.open(dir)) {
            SegmentReadState state = newState(directory, Map.of());
            Path resolved = ParquetSegmentLayout.resolve(state);
            assertNotNull(resolved);
            assertEquals(parquet.getFileName().toString(), resolved.getFileName().toString());
            assertTrue(resolved.toString().endsWith("parquet/_parquet_file_generation_a.parquet"));
        }
    }

    public void testResolveByDirectoryScan() throws Exception {
        Path dir = createTempDir();
        Path parquet = dir.resolve("segment_data.parquet");
        Files.write(parquet, new byte[] { 9 });

        try (FSDirectory directory = FSDirectory.open(dir)) {
            SegmentReadState state = newState(directory, Map.of());
            Path resolved = ParquetSegmentLayout.resolve(state);
            assertNotNull(resolved);
            assertEquals("segment_data.parquet", resolved.getFileName().toString());
        }
    }

    public void testResolveReturnsNullWhenNoParquetFile() throws Exception {
        Path dir = createTempDir();
        try (FSDirectory directory = FSDirectory.open(dir)) {
            SegmentReadState state = newState(directory, Map.of());
            assertNull(ParquetSegmentLayout.resolve(state));
        }
    }

    public void testResolveReturnsNullWhenAttributePathMissing() throws Exception {
        Path dir = createTempDir();
        try (FSDirectory directory = FSDirectory.open(dir)) {
            SegmentReadState state = newState(
                directory,
                Map.of(ParquetSegmentLayout.PARQUET_FILE_ATTRIBUTE, dir.resolve("does_not_exist.parquet").toString())
            );
            assertNull(ParquetSegmentLayout.resolve(state));
        }
    }

    private static SegmentReadState newState(FSDirectory directory, Map<String, String> attributes) {
        SegmentInfo si = new SegmentInfo(
            directory,
            Version.LATEST,
            Version.LATEST,
            "_0",
            3,
            false,
            false,
            Codec.getDefault(),
            Map.of(),
            StringHelper.randomId(),
            attributes,
            null
        );
        return new SegmentReadState(directory, si, FieldInfos.EMPTY, IOContext.DEFAULT);
    }
}
