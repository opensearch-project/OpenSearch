/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.docvalues;

import org.opensearch.be.datafusion.docvalues.bridge.ParquetCodecBridge;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.parquet.bridge.ParquetFileMetadata;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Path;

public class ParquetDocValuesProducerTests extends OpenSearchTestCase {

    /**
     * The producer gates once per file on the stamped format version, admitting the inclusive
     * {@code [MIN, MAX]} window and rejecting everything outside it: too old, too new, and unstamped.
     * Driven with synthetic version longs so each boundary is covered without writing a file per case.
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

    /**
     * The producer cross-checks the footer's writer generation against the segment's
     * {@code writer_generation} attribute, failing closed: equal passes, and a mismatch, a missing
     * segment attribute, and an unstamped footer are each rejected.
     */
    public void testWriterGenerationGateMatchesSegmentAndFailsClosed() throws Exception {
        Path file = createTempDir().resolve("gate.parquet");
        String segment = "_0";

        // Equal generations identify the file written alongside this segment: no throw.
        ParquetDocValuesProducer.checkWriterGeneration("7", 7L, file, segment);

        // Mismatch: the message must carry both generations so the discrepancy is diagnosable.
        IOException mismatch = expectThrows(
            IOException.class,
            () -> ParquetDocValuesProducer.checkWriterGeneration("7", 8L, file, segment)
        );
        assertTrue("mismatch message must name the footer generation", mismatch.getMessage().contains("8"));
        assertTrue("mismatch message must name the segment generation", mismatch.getMessage().contains("7"));

        // A segment with no stamped generation cannot be cross-checked, so it is rejected.
        IOException noAttr = expectThrows(IOException.class, () -> ParquetDocValuesProducer.checkWriterGeneration(null, 7L, file, segment));
        assertTrue(
            "missing-attribute message must name the writer_generation attribute",
            noAttr.getMessage().contains(ParquetSegmentLayout.WRITER_GENERATION_ATTRIBUTE)
        );

        // An unstamped footer carries the unknown sentinel, which cannot match any segment generation.
        expectThrows(
            IOException.class,
            () -> ParquetDocValuesProducer.checkWriterGeneration("7", ParquetCodecBridge.WRITER_GENERATION_UNKNOWN, file, segment)
        );
    }
}
