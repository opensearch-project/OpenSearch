/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.parquet.bridge.ParquetFileMetadata;
import org.opensearch.parquet.bridge.RustBridge;
import org.opensearch.parquet.codec.bridge.ParquetColumnReader;
import org.opensearch.parquet.codec.iter.ParquetNumericDocValues;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

/**
 * Read-only {@link DocValuesProducer} that serves single-valued numeric doc values from a Parquet
 * file through Lucene's DocValues iterator API.
 *
 * <p>The constructor resolves the backing file and sanity-checks its row count against the segment's
 * {@code maxDoc}, but opens no cursor. Each {@code getNumeric}/{@code getSortedNumeric} opens its own
 * dedicated {@link ParquetColumnReader}: a native cursor is forward-only, so one shared across
 * concurrent segment-search slices would be driven backwards by one slice while another advances it.
 * A reader per iterator keeps each slice's scan independent. {@link #close()} releases every reader
 * and is idempotent.
 */
public final class ParquetDocValuesProducer extends DocValuesProducer {

    private static final Logger logger = LogManager.getLogger(ParquetDocValuesProducer.class);

    static final long MIN_SUPPORTED_FORMAT_VERSION = 1_000_000L;

    static final long MAX_SUPPORTED_FORMAT_VERSION = 1_000_000L;

    private final Path parquetFile;
    private final MapperService mapperService;
    private final int maxDoc;
    private final long parquetRowCount;

    private final List<ParquetColumnReader> dedicatedReaders = Collections.synchronizedList(new ArrayList<>());
    private volatile boolean closed;

    /**
     * @param mapperService resolves OpenSearch mapping types for DV-type validation (may be
     *                      {@code null} only in low-level tests that bypass type validation)
     * @throws IOException if the backing Parquet file for the segment cannot be resolved
     * @throws IllegalStateException if the Parquet row count does not match the segment's {@code maxDoc}
     */
    public ParquetDocValuesProducer(SegmentReadState state, MapperService mapperService) throws IOException {
        this.mapperService = mapperService;
        this.maxDoc = state.segmentInfo.maxDoc();

        Path resolved = ParquetSegmentLayout.resolve(state);
        if (resolved == null) {
            throw new IOException(
                String.format(
                    Locale.ROOT,
                    "no Parquet file bound to segment '%s' (maxDoc=%d); cannot serve Parquet doc values",
                    state.segmentInfo.name,
                    maxDoc
                )
            );
        }
        this.parquetFile = resolved;

        ParquetFileMetadata metadata = RustBridge.getFileMetadata(parquetFile.toString());
        checkFormatVersion(metadata.opensearchFormatVersion(), parquetFile);
        this.parquetRowCount = metadata.numRows();
        if (parquetRowCount != maxDoc) {
            throw new IllegalStateException(
                String.format(
                    Locale.ROOT,
                    "Parquet/Lucene row-count mismatch for segment '%s': Lucene maxDoc=%d but Parquet numRows=%d (file=%s)",
                    state.segmentInfo.name,
                    maxDoc,
                    parquetRowCount,
                    parquetFile
                )
            );
        }
    }

    @Override
    public NumericDocValues getNumeric(FieldInfo field) throws IOException {
        ensureOpen();
        validate(field, DocValuesType.NUMERIC);
        return new ParquetNumericDocValues(dedicatedReaderFor(field), maxDoc);
    }

    @Override
    public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
        ensureOpen();
        validate(field, DocValuesType.SORTED_NUMERIC);
        // Ingest rejects multi-valued numerics (ParquetDocumentInput), so every numeric column on disk
        // is single-valued and this singleton wrap is exact; OpenSearch value sources recover the inner
        // iterator via DocValues.unwrapSingleton.
        // TODO(multi-value): genuine arrays would need a write-path change first, then a repeated read
        // in the bridge plus a repeated iterator.
        return DocValues.singleton(new ParquetNumericDocValues(dedicatedReaderFor(field), maxDoc));
    }

    @Override
    public BinaryDocValues getBinary(FieldInfo field) {
        throw unsupported("binary", field);
    }

    @Override
    public SortedDocValues getSorted(FieldInfo field) {
        throw unsupported("sorted", field);
    }

    @Override
    public SortedSetDocValues getSortedSet(FieldInfo field) {
        throw unsupported("sorted-set", field);
    }

    /** No DocValues skip index is served; the synthetic {@code FieldInfo}s advertise skip type NONE. */
    @Override
    public DocValuesSkipper getSkipper(FieldInfo field) {
        return null;
    }

    /**
     * Verifies the backing Parquet file is still accessible and its row count matches the value
     * cached at construction.
     *
     * <p>Not currently invoked: this producer is a search-time overlay, not a registered
     * {@code DocValuesFormat}, so codec-driven integrity checks (CheckIndex, merge-time verification)
     * do not reach it. Kept correct for the {@link DocValuesProducer} contract and future wiring.
     */
    @Override
    public void checkIntegrity() throws IOException {
        ParquetFileMetadata metadata = RustBridge.getFileMetadata(parquetFile.toString());
        if (metadata.numRows() != parquetRowCount) {
            throw new IOException(
                String.format(
                    Locale.ROOT,
                    "checkIntegrity: Parquet numRows changed for %s: expected %d, found %d",
                    parquetFile,
                    parquetRowCount,
                    metadata.numRows()
                )
            );
        }
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        IOException first = null;
        synchronized (dedicatedReaders) {
            for (ParquetColumnReader reader : dedicatedReaders) {
                try {
                    reader.close();
                } catch (IOException e) {
                    if (first == null) {
                        first = e;
                    }
                } catch (RuntimeException e) {
                    // Keep closing the rest, but keep the failure visible.
                    logger.warn("Failed to close Parquet column reader for [{}]", parquetFile, e);
                }
            }
            dedicatedReaders.clear();
        }
        if (first != null) {
            throw first;
        }
    }

    /**
     * Rejects a file this codec cannot decode: unstamped, older than {@link #MIN_SUPPORTED_FORMAT_VERSION},
     * or newer than {@link #MAX_SUPPORTED_FORMAT_VERSION}, failing on an out-of-range file rather than reading it
     * with assumptions that may not hold.
     */
    static void checkFormatVersion(long formatVersion, Path file) throws IOException {
        if (formatVersion == ParquetFileMetadata.FORMAT_VERSION_UNKNOWN) {
            throw new IOException(
                String.format(
                    Locale.ROOT,
                    "Parquet file %s carries no parseable opensearch.format_version; this doc-values codec requires a stamped version in %s",
                    file,
                    supportedRange()
                )
            );
        }
        if (formatVersion < MIN_SUPPORTED_FORMAT_VERSION || formatVersion > MAX_SUPPORTED_FORMAT_VERSION) {
            throw new IOException(
                String.format(
                    Locale.ROOT,
                    "Parquet file %s has OpenSearch format version %s, outside this doc-values codec's supported range %s",
                    file,
                    describeFormatVersion(formatVersion),
                    supportedRange()
                )
            );
        }
    }

    /** Renders the inclusive supported version range for an error message. */
    private static String supportedRange() {
        return "[" + describeFormatVersion(MIN_SUPPORTED_FORMAT_VERSION) + ", " + describeFormatVersion(MAX_SUPPORTED_FORMAT_VERSION) + "]";
    }

    /** Renders a long-encoded format version as {@code major.minor.patch} for an error message. */
    private static String describeFormatVersion(long formatVersion) {
        long major = formatVersion / 1_000_000L;
        long minor = formatVersion / 1_000L % 1_000L;
        long patch = formatVersion % 1_000L;
        return major + "." + minor + "." + patch;
    }

    /** Validates the field's mapping type supports the requested DV type, when a mapper is present. */
    private void validate(FieldInfo field, DocValuesType requested) {
        if (mapperService == null) {
            return; // low-level tests may bypass mapping validation
        }
        FieldTypeMapping.validate(field.getName(), mappingType(field), requested);
    }

    private String mappingType(FieldInfo field) {
        MappedFieldType mft = mapperService.fieldType(field.getName());
        if (mft == null) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "field '%s' has no mapping; cannot resolve Parquet column type", field.getName())
            );
        }
        return mft.typeName();
    }

    /** Opens a dedicated forward-only cursor for one iterator, registered for close with this producer. */
    private ParquetColumnReader dedicatedReaderFor(FieldInfo field) throws IOException {
        ParquetColumnReader reader = ParquetColumnReader.open(parquetFile, field.getName());
        dedicatedReaders.add(reader);
        return reader;
    }

    private UnsupportedOperationException unsupported(String kind, FieldInfo field) {
        return new UnsupportedOperationException(
            String.format(
                Locale.ROOT,
                "Parquet DocValues codec does not serve %s doc values (field '%s'); numeric only",
                kind,
                field.getName()
            )
        );
    }

    /** Whether {@link #close()} has run. */
    boolean isClosed() {
        return closed;
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("ParquetDocValuesProducer is closed");
        }
    }
}
