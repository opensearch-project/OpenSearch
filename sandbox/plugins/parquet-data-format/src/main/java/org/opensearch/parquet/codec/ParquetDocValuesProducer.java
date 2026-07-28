/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
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
import org.opensearch.parquet.bridge.ParquetColumnReader;
import org.opensearch.parquet.bridge.ParquetFileMetadata;
import org.opensearch.parquet.bridge.RustBridge;
import org.opensearch.parquet.codec.cache.BufferPool;
import org.opensearch.parquet.codec.cache.QueryParquetStats;
import org.opensearch.parquet.codec.iter.ParquetBinaryDocValues;
import org.opensearch.parquet.codec.iter.ParquetNumericDocValues;
import org.opensearch.parquet.codec.iter.ParquetSortedDocValues;
import org.opensearch.parquet.codec.iter.ParquetSortedNumericDocValues;
import org.opensearch.parquet.codec.iter.ParquetSortedSetDocValues;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Read-only {@link DocValuesProducer} that materializes per-document values from a Parquet
 * file through Lucene's DocValues iterator API, for <b>flat indices only</b>.
 *
 * <h2>Row ID = Doc ID invariant (precondition)</h2>
 * This producer relies on the composite indexing engine's guarantee that Lucene document
 * {@code N} corresponds to Parquet row position {@code N} within the same segment's Parquet
 * file, with one Lucene document per Parquet row and no translation table. The invariant is
 * verified at construction by asserting the Parquet file's {@code numRows} equals the segment's
 * {@code maxDoc}; a mismatch throws {@link IllegalStateException}. Nested documents are out of
 * scope.
 *
 * <h2>Laziness and lifecycle</h2>
 * The constructor resolves the Parquet file and checks the invariant but opens no native
 * column-reader handle. Each {@code getX(field)} lazily opens (and caches) a
 * {@link ParquetColumnReader}; {@code getSorted}/{@code getSortedSet} additionally build (and
 * cache) an {@link OrdinalTable} on first access. {@link #close()} releases every reader,
 * ordinal table, and the shared {@link BufferPool}, and is idempotent.
 *
 * <p>Not thread-safe: one producer serves one segment on one query thread.
 */
public final class ParquetDocValuesProducer extends DocValuesProducer {

    private final Path parquetFile;
    private final MapperService mapperService;
    private final int maxDoc;
    private final long parquetRowCount;

    private final BufferPool bufferPool = new BufferPool();
    private final Map<String, ParquetColumnReader> columnReaders = new HashMap<>();
    private final Map<String, OrdinalTable> ordinalTables = new HashMap<>();

    /** Optional per-query accumulator; propagated to each column reader so its stats roll up at close. */
    private QueryParquetStats queryStats;

    private boolean closed;

    /**
     * Constructs the producer for {@code state}'s segment.
     *
     * @param mapperService resolves OpenSearch mapping types for DV-type validation (may be
     *                      {@code null} only in low-level tests that bypass type validation)
     * @throws IOException if the Parquet file for the segment cannot be resolved (Req 9.3)
     * @throws IllegalStateException if the Row ID = Doc ID invariant is violated (Req 12.3)
     */
    public ParquetDocValuesProducer(SegmentReadState state, MapperService mapperService) throws IOException {
        this.mapperService = mapperService;
        this.maxDoc = state.segmentInfo.maxDoc();

        Path resolved = ParquetSegmentLayout.resolve(state);
        if (resolved == null) {
            throw new IOException(
                String.format(
                    Locale.ROOT,
                    "no Parquet file found for segment '%s' (maxDoc=%d); cannot serve Parquet doc values",
                    state.segmentInfo.name,
                    maxDoc
                )
            );
        }
        this.parquetFile = resolved;

        ParquetFileMetadata metadata = RustBridge.getFileMetadata(parquetFile.toString());
        this.parquetRowCount = metadata.numRows();
        if (parquetRowCount != maxDoc) {
            throw new IllegalStateException(
                String.format(
                    Locale.ROOT,
                    "Parquet/Lucene row-count mismatch for segment '%s': Lucene maxDoc=%d but Parquet numRows=%d (file=%s). "
                        + "The resolved Parquet file must contain exactly the segment's rows; docId→row translation is handled "
                        + "separately via __row_id__.",
                    state.segmentInfo.name,
                    maxDoc,
                    parquetRowCount,
                    parquetFile
                )
            );
        }
    }

    /**
     * Attaches the per-query accumulator. The accumulator is propagated to every column reader
     * (existing and future) so each reader's stats roll up into the query total when it closes.
     */
    public void setQueryStats(QueryParquetStats queryStats) {
        this.queryStats = queryStats;
        if (queryStats != null) {
            for (ParquetColumnReader reader : columnReaders.values()) {
                reader.setQueryStats(queryStats);
            }
        }
    }

    // ── DocValuesProducer API ──

    @Override
    public NumericDocValues getNumeric(FieldInfo field) throws IOException {
        ensureOpen();
        validate(field, DocValuesType.NUMERIC);
        ParquetColumnReader reader = readerFor(field, false);
        return new ParquetNumericDocValues(reader, maxDoc);
    }

    @Override
    public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
        ensureOpen();
        validate(field, DocValuesType.SORTED_NUMERIC);
        ParquetColumnReader reader = readerFor(field, true);
        return new ParquetSortedNumericDocValues(reader, maxDoc);
    }

    @Override
    public BinaryDocValues getBinary(FieldInfo field) throws IOException {
        ensureOpen();
        validate(field, DocValuesType.BINARY);
        ParquetColumnReader reader = readerFor(field, false);
        return new ParquetBinaryDocValues(reader, maxDoc);
    }

    @Override
    public SortedDocValues getSorted(FieldInfo field) throws IOException {
        ensureOpen();
        validate(field, DocValuesType.SORTED);
        OrdinalTable table = ordinalTableFor(field, false);
        return new ParquetSortedDocValues(table, maxDoc);
    }

    @Override
    public SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
        ensureOpen();
        validate(field, DocValuesType.SORTED_SET);
        OrdinalTable table = ordinalTableFor(field, true);
        return new ParquetSortedSetDocValues(table, maxDoc);
    }

    /**
     * Always returns {@code null}: this producer serves no {@link DocValuesSkipper} in the
     * DocValues-codec base. The per-page Parquet {@code ColumnIndex} min/max needed to skip whole
     * pages during range work is loaded (see {@code ColumnPageIndex}) but not exposed as a skipper
     * here. Range/terms filter routing — the sole consumer of a skipper — is a separate change that
     * will own the {@code DocValuesSkipper} implementation and decide the float/double raw-bits vs
     * numeric-order question (IEEE-754 bit order diverges from numeric order for negatives, so a
     * naive bit-min/max skipper would be wrong for float/double).
     *
     * <p>The synthetic {@code FieldInfo}s produced by {@code ParquetDocValuesLeafReader} declare
     * {@link org.apache.lucene.index.DocValuesSkipIndexType#NONE} to stay in lockstep with this:
     * no field claims a RANGE skip index that this method cannot serve.
     */
    // TODO(skipper): deferred to the range/terms filter-routing change. That change re-introduces
    // ParquetDocValuesSkipper over ColumnPageIndex (INT32/INT64/BOOL only) and resolves the
    // float/double order-preserving-encoding question before enabling a skipper for them.
    @Override
    public DocValuesSkipper getSkipper(FieldInfo field) throws IOException {
        ensureOpen();
        return null;
    }

    /**
     * Verifies the underlying Parquet file is accessible and its metadata is consistent: the
     * file opens, {@code numRows} matches the value cached at construction, and the metadata
     * round-trip (which includes the writer-side CRC) succeeds.
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
        // Cache-effectiveness is summarized once per query on the dedicated stats channel
        // ([PARQUET_DV_QUERY_STATS]); no per-segment detail line here.
        closed = true;
        IOException first = null;
        for (ParquetColumnReader reader : columnReaders.values()) {
            try {
                reader.close();
            } catch (IOException | RuntimeException e) {
                if (first == null && e instanceof IOException io) {
                    first = io;
                }
                // Suppress per-reader errors so every reader gets a chance to close.
            }
        }
        columnReaders.clear();
        ordinalTables.clear();
        bufferPool.close();
        if (first != null) {
            throw first;
        }
    }

    // ── internals ──

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

    /** Resolves the Parquet physical type for a field from its mapping (or infers for tests). */
    private ParquetPhysicalType physicalType(FieldInfo field) {
        if (mapperService != null) {
            return FieldTypeMapping.forType(mappingType(field)).physical();
        }
        // Without a mapper, infer from the Lucene DV type recorded on the field.
        return switch (field.getDocValuesType()) {
            case BINARY, SORTED, SORTED_SET -> ParquetPhysicalType.BYTE_ARRAY;
            default -> ParquetPhysicalType.INT64;
        };
    }

    private ParquetColumnReader readerFor(FieldInfo field, boolean repeated) throws IOException {
        ParquetColumnReader reader = columnReaders.get(field.getName());
        if (reader == null) {
            reader = ParquetColumnReader.open(parquetFile, field.getName(), physicalType(field), repeated, bufferPool);
            reader.setQueryStats(queryStats);
            columnReaders.put(field.getName(), reader);
        }
        return reader;
    }

    private OrdinalTable ordinalTableFor(FieldInfo field, boolean multiValued) throws IOException {
        OrdinalTable table = ordinalTables.get(field.getName());
        if (table == null) {
            ParquetColumnReader reader = readerFor(field, multiValued);
            table = multiValued ? OrdinalTable.buildMultiValued(reader, maxDoc) : OrdinalTable.buildSingleValued(reader, maxDoc);
            ordinalTables.put(field.getName(), table);
        }
        return table;
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("ParquetDocValuesProducer is closed");
        }
    }
}
