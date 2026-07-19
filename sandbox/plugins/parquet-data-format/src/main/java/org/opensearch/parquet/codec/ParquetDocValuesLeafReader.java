/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec;

import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.IOContext;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.lucene.index.SequentialStoredFieldsLeafReader;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.parquet.codec.cache.QueryParquetStats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A {@link FilterLeafReader} that serves doc values for Parquet-resident fields from a
 * {@link ParquetDocValuesProducer}, while delegating everything else to the underlying Lucene
 * leaf reader.
 *
 * <p>This is the read-time integration of the Parquet DocValues codec for the case where a field
 * is <b>Parquet-only</b> — i.e. it has no {@link FieldInfo} in the Lucene segment at all (the
 * composite engine's Lucene secondary writes only text/keyword inverted indexes plus the row-id
 * doc values; numeric fields like {@code age} live solely in Parquet). Lucene's
 * {@code PerFieldDocValuesFormat} cannot route to such a field because there is no segment
 * {@code FieldInfo} to carry the format name. This reader closes that gap by:
 *
 * <ol>
 *   <li><b>Synthesizing {@link FieldInfo}s</b> — for every mapped field that the Parquet codec
 *       supports and that is absent (or DV-less) in the delegate's {@link FieldInfos}, a synthetic
 *       {@code FieldInfo} with the appropriate {@link DocValuesType} (from {@link FieldTypeMapping})
 *       is added so OpenSearch's value-source layer believes the doc values exist and asks for
 *       them.</li>
 *   <li><b>Overriding the five DV accessors</b> — for those synthetic fields the iterators come
 *       from a per-segment {@link ParquetDocValuesProducer}; all other fields delegate to the
 *       underlying reader unchanged.</li>
 * </ol>
 *
 * <p>One producer is built lazily per segment and closed when this reader closes. Not shared
 * across segments.
 *
 * <h2>Known limitation: multi-valued (array) fields</h2>
 * This reader currently serves every Parquet field as <b>single-valued</b>:
 * {@link #getSortedNumericDocValues} and {@link #getSortedSetDocValues} wrap the single-valued
 * iterator in {@link DocValues#singleton}, so a genuinely multi-valued (array) field returns only
 * its first value. The producer's multi-valued read path exists but is not wired here yet. This is
 * an intentional, documented gap for the DocValues-codec base; wiring the multi-valued path (with
 * per-column repetition-level detection) is a separate change. See the {@code TODO(multi-value)}
 * markers on those two methods.
 *
 * <p>Extends {@link SequentialStoredFieldsLeafReader} (rather than plain {@link FilterLeafReader})
 * so the fetch phase can retrieve {@code _source}/stored fields on {@code size > 0} queries. On a
 * Parquet-primary index derived source is force-enabled, so the reader stack is
 * {@code DerivedSourceLeafReader -> ParquetDocValuesLeafReader -> SegmentReader}. When the fetch
 * phase asks the outer {@code DerivedSourceLeafReader} for a sequential stored-fields reader, it
 * unwraps to this reader; a plain {@code FilterLeafReader} is neither a {@code CodecReader} nor a
 * {@code SequentialStoredFieldsLeafReader}, so the unwrap threw and only {@code size:0} worked.
 * As a {@code SequentialStoredFieldsLeafReader} this reader is transparent to that unwrap: it
 * passes the underlying segment's stored-fields reader straight through, and the derived-source
 * layer still synthesizes {@code _source} from doc values on top of it.
 */
public final class ParquetDocValuesLeafReader extends SequentialStoredFieldsLeafReader {

    private final MapperService mapperService;

    /** Lazily constructed Parquet producer for this segment; null until first DV access. */
    private ParquetDocValuesProducer producer;
    private boolean producerInitialized;

    /** Synthetic + real merged field infos, computed once. */
    private final FieldInfos mergedFieldInfos;

    /** Field name -> synthetic FieldInfo for Parquet-resident DV fields served by this reader. */
    private final Map<String, FieldInfo> parquetFields;

    /** The segment read state used to build the producer (captured at construction). */
    private final SegmentReadState segmentReadState;

    /** Per-query stats accumulator shared across all leaves of one search; may be null in tests. */
    private final QueryParquetStats queryStats;

    private ParquetDocValuesLeafReader(
        LeafReader in,
        MapperService mapperService,
        SegmentReadState segmentReadState,
        Map<String, FieldInfo> parquetFields,
        FieldInfos mergedFieldInfos,
        QueryParquetStats queryStats
    ) {
        super(in);
        this.mapperService = mapperService;
        this.segmentReadState = segmentReadState;
        this.parquetFields = parquetFields;
        this.mergedFieldInfos = mergedFieldInfos;
        this.queryStats = queryStats;
    }

    /**
     * Builds a {@link ParquetDocValuesLeafReader} for {@code in} if a Parquet file resolves for the
     * segment and the mapping declares at least one Parquet-codec-supported field that is missing
     * doc values in the Lucene segment. Otherwise returns {@code in} unwrapped.
     */
    public static LeafReader wrapIfApplicable(LeafReader in, MapperService mapperService, QueryParquetStats queryStats) throws IOException {
        SegmentReader segmentReader;
        try {
            segmentReader = Lucene.segmentReader(in);
        } catch (RuntimeException e) {
            // Not a segment-backed leaf (e.g. an in-memory test reader) — nothing to wrap.
            return in;
        }

        SegmentReadState state = new SegmentReadState(
            segmentReader.directory(),
            segmentReader.getSegmentInfo().info,
            segmentReader.getFieldInfos(),
            IOContext.DEFAULT
        );

        // Only proceed if a Parquet file exists for this segment.
        if (ParquetSegmentLayout.resolve(state) == null) {
            return in;
        }

        FieldInfos existing = in.getFieldInfos();
        Map<String, FieldInfo> parquetFields = new LinkedHashMap<>();
        List<FieldInfo> merged = new ArrayList<>();
        int maxNumber = -1;
        for (FieldInfo fi : existing) {
            merged.add(fi);
            maxNumber = Math.max(maxNumber, fi.number);
        }

        // Walk the mapping. For each field the Parquet codec supports whose doc values are NOT
        // already present in the Lucene segment, synthesize a FieldInfo with the mapped DV type.
        for (MappedFieldType mft : mapperService.fieldTypes()) {
            String name = mft.name();
            if (mapperService.isMetadataField(name)) {
                continue;
            }
            if (FieldTypeMapping.isSupported(mft.typeName()) == false) {
                continue;
            }
            FieldInfo realFi = existing.fieldInfo(name);
            if (realFi != null && realFi.getDocValuesType() != DocValuesType.NONE) {
                // Lucene already serves doc values for this field — leave it to the native reader.
                continue;
            }
            FieldTypeMapping.Mapping mapping = FieldTypeMapping.forType(mft.typeName());
            DocValuesType dvType = mapping.singleValued();
            FieldInfo synthetic = newDocValuesFieldInfo(name, ++maxNumber, dvType, skipIndexTypeFor(mapping));
            parquetFields.put(name, synthetic);
            // If a DV-less FieldInfo already exists for this field, replace it with the synthetic
            // one carrying the DV type; otherwise append.
            if (realFi != null) {
                merged.removeIf(fi -> fi.name.equals(name));
            }
            merged.add(synthetic);
        }

        if (parquetFields.isEmpty()) {
            // Nothing for us to serve — don't wrap.
            return in;
        }

        FieldInfos mergedInfos = new FieldInfos(merged.toArray(new FieldInfo[0]));
        return new ParquetDocValuesLeafReader(in, mapperService, state, parquetFields, mergedInfos, queryStats);
    }

    /**
     * Skip-index declaration for a synthetic field: RANGE for integer-shaped columns whose
     * Parquet ColumnIndex min/max the producer can serve through a {@link DocValuesSkipper}
     * (raw-bits order == numeric order), NONE otherwise. Must stay in sync with
     * {@link ParquetDocValuesProducer#getSkipper}'s physical-type gate: declaring RANGE for a
     * field whose getSkipper returns null would break consumers that trust the declaration.
     */
    private static DocValuesSkipIndexType skipIndexTypeFor(FieldTypeMapping.Mapping mapping) {
        ParquetPhysicalType phys = mapping.physical();
        boolean skippable = phys == ParquetPhysicalType.INT32 || phys == ParquetPhysicalType.INT64 || phys == ParquetPhysicalType.BOOL;
        return skippable ? DocValuesSkipIndexType.RANGE : DocValuesSkipIndexType.NONE;
    }

    /** Builds a synthetic doc-values {@link FieldInfo} carrying the given DV type. */
    private static FieldInfo newDocValuesFieldInfo(String name, int number, DocValuesType dvType, DocValuesSkipIndexType skipType) {
        return new FieldInfo(
            name,
            number,
            false,                       // storeTermVector
            true,                        // omitNorms
            false,                       // storePayloads
            IndexOptions.NONE,           // not indexed via this reader
            dvType,
            skipType,
            -1,                          // dvGen
            new HashMap<>(),             // attributes (mutable, per FieldInfo contract)
            0,                           // pointDimensionCount
            0,                           // pointIndexDimensionCount
            0,                           // pointNumBytes
            0,                           // vectorDimension
            VectorEncoding.FLOAT32,
            VectorSimilarityFunction.EUCLIDEAN,
            false,                       // softDeletes
            false                        // isParentField
        );
    }

    private synchronized ParquetDocValuesProducer producer() throws IOException {
        if (producerInitialized == false) {
            producer = new ParquetDocValuesProducer(segmentReadState, mapperService);
            producer.setQueryStats(queryStats);
            producerInitialized = true;
        }
        return producer;
    }

    /** Returns the synthetic FieldInfo if the given field is served from Parquet, else null. */
    private FieldInfo parquetFieldInfo(String field) {
        return parquetFields.get(field);
    }

    /**
     * Builds a {@link RowIdResolver} that translates this segment's {@code docId}s to Parquet row
     * positions by reading the underlying leaf's {@code __row_id__} doc values. Each codec iterator
     * needs its own resolver (its own {@code __row_id__} iterator), so this is called per DV accessor.
     * Falls back to identity when the segment has no {@code __row_id__} field.
     */
    private RowIdResolver newRowIdResolver() throws IOException {
        // The write path GUARANTEES rowId == docId in every finished segment: row ids are rewritten to
        // sequential 0..maxDoc-1 after any sort/merge (SequentialRowIdProducer) and verified by
        // LuceneWriter.assertRowIdsSequential. So the per-doc __row_id__ lookup is pure waste here — it
        // re-reads a value that always equals the docId. Skip it: use the no-op IDENTITY resolver.
        //
        // Backed by an -ea assert that mirrors the writer's invariant; if a future write path ever
        // produced a non-identity segment, this trips in dev/test. (Costs nothing in prod.)
        //
        // TODO(rowid-guard): the IDENTITY shortcut is currently guarded ONLY by the -ea assert below,
        // which is a no-op in production (assertions disabled). A non-identity segment reaching this
        // path in prod would silently return values for the wrong documents. Before production
        // hardening, replace this with a real runtime guard (e.g. fall back to the RowIdRemappingDocValues
        // path when the invariant does not hold, rather than trusting it unconditionally).
        assert assertRowIdsAreIdentity() : "non-identity __row_id__ segment reached read path; IDENTITY shortcut is unsafe here";
        return RowIdResolver.IDENTITY;
    }

    /** -ea-only check mirroring {@code LuceneWriter.assertRowIdsSequential}: every doc's __row_id__ == docId. */
    private boolean assertRowIdsAreIdentity() throws IOException {
        SortedNumericDocValues rowId = in.getSortedNumericDocValues(DocumentInput.ROW_ID_FIELD);
        if (rowId == null) {
            return true; // no row-id field => identity by definition
        }
        for (int docId = 0; docId < maxDoc(); docId++) {
            if (rowId.advanceExact(docId) == false) {
                return false;
            }
            if (rowId.nextValue() != docId) {
                return false;
            }
        }
        return true;
    }

    @Override
    public FieldInfos getFieldInfos() {
        return mergedFieldInfos;
    }

    @Override
    public NumericDocValues getNumericDocValues(String field) throws IOException {
        FieldInfo fi = parquetFieldInfo(field);
        if (fi != null && fi.getDocValuesType() == DocValuesType.NUMERIC) {
            RowIdResolver resolver = newRowIdResolver();
            NumericDocValues numeric = producer().getNumeric(fi);
            // IDENTITY (the guaranteed case — see newRowIdResolver) means docId == Parquet row, so the
            // remap wrapper is pure indirection: return the delegate, already in docId space, directly.
            return resolver == RowIdResolver.IDENTITY ? numeric : RowIdRemappingDocValues.numeric(numeric, resolver, maxDoc());
        }
        return in.getNumericDocValues(field);
    }

    @Override
    public SortedNumericDocValues getSortedNumericDocValues(String field) throws IOException {
        FieldInfo fi = parquetFieldInfo(field);
        if (fi != null) {
            // OpenSearch numeric value sources request SORTED_NUMERIC even for single-valued fields,
            // then call DocValues.unwrapSingleton(...) to take a leaner single-valued collector when
            // possible. We therefore serve single-valued numerics through the CACHED single-valued
            // iterator (producer().getNumeric → ParquetNumericDocValues → PageCache hot path), apply
            // the docId→row remapping at the numeric level, and wrap the result with
            // DocValues.singleton(...) so the returned value is a real SingletonSortedNumericDocValues
            // that unwrapSingleton(...) can detect. This wins on two layers: the PageCache (no per-doc
            // FFM call) and the aggregator's single-valued fast path.
            //
            // TODO(multi-value): this intentionally treats every numeric field as single-valued and so
            // breaks true multi-valued (array) numeric fields. Restore the repeated path for genuinely
            // multi-valued columns (e.g. branch on the Parquet column's repetition level) and return
            // RowIdRemappingDocValues.sortedNumeric(producer().getSortedNumeric(asSortedNumeric),
            // newRowIdResolver(), maxDoc()) for those.
            FieldInfo asNumeric = fi.getDocValuesType() == DocValuesType.NUMERIC
                ? fi
                : newDocValuesFieldInfo(field, fi.number, DocValuesType.NUMERIC, fi.docValuesSkipIndexType());
            NumericDocValues numeric = producer().getNumeric(asNumeric);
            RowIdResolver resolver = newRowIdResolver();
            NumericDocValues remapped = resolver == RowIdResolver.IDENTITY
                ? numeric
                : RowIdRemappingDocValues.numeric(numeric, resolver, maxDoc());
            return DocValues.singleton(remapped);
        }
        return in.getSortedNumericDocValues(field);
    }

    @Override
    public BinaryDocValues getBinaryDocValues(String field) throws IOException {
        FieldInfo fi = parquetFieldInfo(field);
        if (fi != null && fi.getDocValuesType() == DocValuesType.BINARY) {
            RowIdResolver resolver = newRowIdResolver();
            BinaryDocValues binary = producer().getBinary(fi);
            return resolver == RowIdResolver.IDENTITY ? binary : RowIdRemappingDocValues.binary(binary, resolver, maxDoc());
        }
        return in.getBinaryDocValues(field);
    }

    @Override
    public SortedDocValues getSortedDocValues(String field) throws IOException {
        FieldInfo fi = parquetFieldInfo(field);
        if (fi != null && fi.getDocValuesType() == DocValuesType.SORTED) {
            RowIdResolver resolver = newRowIdResolver();
            SortedDocValues sorted = producer().getSorted(fi);
            return resolver == RowIdResolver.IDENTITY ? sorted : RowIdRemappingDocValues.sorted(sorted, resolver, maxDoc());
        }
        return in.getSortedDocValues(field);
    }

    @Override
    public SortedSetDocValues getSortedSetDocValues(String field) throws IOException {
        FieldInfo fi = parquetFieldInfo(field);
        if (fi != null) {
            // Mirror getSortedNumericDocValues: keyword value sources request SORTED_SET even for
            // single-valued fields, then call DocValues.unwrapSingleton(...). Serve single-valued
            // keywords through the single-valued ordinal-table iterator (producer().getSorted →
            // ParquetSortedDocValues), remap docId→row, and wrap with DocValues.singleton(...) so the
            // returned value is a real SingletonSortedSetDocValues that unwrapSingleton(...) detects.
            //
            // TODO(multi-value): intentionally treats every keyword field as single-valued and so breaks
            // true multi-valued (array) keyword fields. Restore the multi-valued path for genuinely
            // repeated columns and return RowIdRemappingDocValues.sortedSet(
            // producer().getSortedSet(asSortedSet), newRowIdResolver(), maxDoc()) for those.
            FieldInfo asSorted = fi.getDocValuesType() == DocValuesType.SORTED
                ? fi
                : newDocValuesFieldInfo(field, fi.number, DocValuesType.SORTED, fi.docValuesSkipIndexType());
            SortedDocValues sorted = producer().getSorted(asSorted);
            RowIdResolver resolver = newRowIdResolver();
            SortedDocValues remapped = resolver == RowIdResolver.IDENTITY
                ? sorted
                : RowIdRemappingDocValues.sorted(sorted, resolver, maxDoc());
            return DocValues.singleton(remapped);
        }
        return in.getSortedSetDocValues(field);
    }

    @Override
    public DocValuesSkipper getDocValuesSkipper(String field) throws IOException {
        FieldInfo fi = parquetFieldInfo(field);
        if (fi != null) {
            if (fi.docValuesSkipIndexType() == DocValuesSkipIndexType.NONE) {
                return null;
            }
            // Doc IDs and Parquet rows coincide (IDENTITY resolver — see newRowIdResolver), so
            // page row ranges are directly valid as skipper doc ID intervals.
            return producer().getSkipper(fi);
        }
        return in.getDocValuesSkipper(field);
    }

    @Override
    protected void doClose() throws IOException {
        IOException first = null;
        try {
            if (producer != null) {
                producer.close();
            }
        } catch (IOException e) {
            first = e;
        }
        try {
            super.doClose();
        } catch (IOException e) {
            if (first == null) {
                first = e;
            }
        }
        if (first != null) {
            throw first;
        }
    }

    /**
     * This reader serves no stored fields itself — it only overlays Parquet doc values. The
     * underlying segment reader holds the real stored fields, so return its sequential reader
     * unchanged. {@link SequentialStoredFieldsLeafReader#getSequentialStoredFieldsReader()} already
     * unwrapped {@code in} (a {@code CodecReader}/segment reader) down to {@code reader}; the
     * derived-source layer above wraps the result to synthesize {@code _source}.
     */
    @Override
    protected StoredFieldsReader doGetSequentialStoredFieldsReader(StoredFieldsReader reader) {
        return reader;
    }

    // Cache helpers must delegate to the underlying reader so query/segment caches stay coherent.
    @Override
    public CacheHelper getCoreCacheHelper() {
        return in.getCoreCacheHelper();
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        return in.getReaderCacheHelper();
    }
}
