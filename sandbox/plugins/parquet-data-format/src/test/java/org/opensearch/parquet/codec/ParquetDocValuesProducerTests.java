/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec;

import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.parquet.bridge.ParquetFileMetadata;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Path;

public class ParquetDocValuesProducerTests extends OpenSearchTestCase {

    /**
     * The producer gates once per file on the stamped format version, admitting the inclusive
     * {@code [MIN, MAX]} window and rejecting everything outside it - too old, too new, and unstamped -
     * mirroring Lucene's {@code checkIndexHeader}. Driven with synthetic version longs so each boundary
     * is covered without writing a file per case.
     */
    public void testFormatVersionGateAcceptsRangeAndRejectsOutside() throws Exception {
        Path file = createTempDir().resolve("gate.parquet");

        // In range: the bounds themselves must pass.
        ParquetDocValuesProducer.checkFormatVersion(ParquetDocValuesProducer.MIN_SUPPORTED_FORMAT_VERSION, file);
        ParquetDocValuesProducer.checkFormatVersion(ParquetDocValuesProducer.MAX_SUPPORTED_FORMAT_VERSION, file);

        // Too old: one tick below the floor.
        expectThrows(
            IOException.class,
            () -> ParquetDocValuesProducer.checkFormatVersion(ParquetDocValuesProducer.MIN_SUPPORTED_FORMAT_VERSION - 1, file)
        );

        // Unstamped: the unknown sentinel is reported as carrying no version, not as a numeric one.
        IOException unstamped = expectThrows(
            IOException.class,
            () -> ParquetDocValuesProducer.checkFormatVersion(ParquetFileMetadata.FORMAT_VERSION_UNKNOWN, file)
        );
        assertTrue(
            "unstamped file must be reported as carrying no version",
            unstamped.getMessage().contains("no parseable opensearch.format_version")
        );

        // Too new: one tick above the ceiling must be refused rather than read with current-version logic.
        expectThrows(
            IOException.class,
            () -> ParquetDocValuesProducer.checkFormatVersion(ParquetDocValuesProducer.MAX_SUPPORTED_FORMAT_VERSION + 1, file)
        );
    }

    /**
     * The range's top must track the writer's current version, so a writer bump forces a deliberate
     * codec bump rather than silently reading a newer file with today's decode logic.
     */
    public void testSupportedRangeTracksTheWriterVersion() {
        assertTrue(
            "min must not exceed max",
            ParquetDocValuesProducer.MIN_SUPPORTED_FORMAT_VERSION <= ParquetDocValuesProducer.MAX_SUPPORTED_FORMAT_VERSION
        );
        assertEquals(
            "the range's top must equal the writer's current version",
            ParquetDataFormatPlugin.PARQUET_FORMAT_VERSION,
            ParquetDocValuesProducer.MAX_SUPPORTED_FORMAT_VERSION
        );
    }
}
