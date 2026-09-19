/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.docvalues;

import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.IOContext;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.lucene.index.SequentialStoredFieldsLeafReader;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A {@link SequentialStoredFieldsLeafReader} that serves numeric doc values for Parquet-resident
 * fields from a {@link ParquetDocValuesProducer}, delegating everything else to the underlying leaf.
 *
 * <p>A Parquet-only field has no {@link FieldInfo} in the Lucene segment, so Lucene's
 * {@code PerFieldDocValuesFormat} cannot route to it. This reader closes that gap by synthesizing a
 * {@code FieldInfo} (with the DV type from {@link FieldTypeMapping}) for every mapped, codec-supported
 * field that is absent from the delegate, and overriding the numeric DV accessors to serve those fields
 * from a per-segment-core producer. All other fields pass through unchanged.
 *
 * <p>It extends {@link SequentialStoredFieldsLeafReader} (not plain {@code FilterLeafReader}) so the
 * fetch phase can still retrieve stored fields: the derived-source layer above unwraps to this reader,
 * which passes the underlying segment's stored-fields reader straight through.
 *
 * <p>The producer is shared across requests over the same segment core (via
 * {@link ParquetDocValuesProducerRegistry}) and closed by the core's closed-listener. This wrapper is
 * request-scoped: it records the cursors it hands out in a {@link CursorRegistry} and closes only
 * those when the request ends.
 */
public final class ParquetDocValuesLeafReader extends SequentialStoredFieldsLeafReader {

    private final Map<String, FieldInfo> parquetFields;
    private final FieldInfos combinedFieldInfos;
    private final ParquetDocValuesProducer producer;
    private final CursorRegistry cursors = new CursorRegistry();

    /** Memoized result of the assertions-only row-id identity check; see {@link #assertRowIdsAreIdentity}. */
    private boolean rowIdsChecked;
    private boolean rowIdsAreIdentity;

    private ParquetDocValuesLeafReader(
        LeafReader in,
        Map<String, FieldInfo> parquetFields,
        FieldInfos combinedFieldInfos,
        ParquetDocValuesProducer producer
    ) {
        super(in);
        this.parquetFields = parquetFields;
        this.combinedFieldInfos = combinedFieldInfos;
        this.producer = producer;
    }

    /**
     * Wraps {@code in} if a Parquet file resolves for its segment and the mapping declares at least one
     * codec-supported field the Lucene segment does not know about. Otherwise returns {@code in}.
     */
    public static LeafReader wrapIfApplicable(LeafReader in, MapperService mapperService) throws IOException {
        SegmentReader segmentReader;
        try {
            segmentReader = Lucene.segmentReader(in);
        } catch (RuntimeException e) {
            // Not a segment-backed leaf (e.g. an in-memory test reader) - nothing to wrap.
            return in;
        }

        SegmentReadState state = new SegmentReadState(
            segmentReader.directory(),
            segmentReader.getSegmentInfo().info,
            segmentReader.getFieldInfos(),
            IOContext.DEFAULT
        );

        if (ParquetSegmentLayout.resolve(state) == null) {
            return in;
        }

        FieldInfos existing = in.getFieldInfos();
        Map<String, FieldInfo> parquetFields = new LinkedHashMap<>();
        List<FieldInfo> combined = new ArrayList<>();
        int maxNumber = -1;
        for (FieldInfo fi : existing) {
            combined.add(fi);
            maxNumber = Math.max(maxNumber, fi.number);
        }

        // Synthesize a FieldInfo carrying the mapped DV type for each codec-supported field the Lucene
        // segment does not know about at all. A field Lucene does know about is left entirely to the
        // underlying reader: its FieldInfo is the only record of its postings, points and doc values, so
        // replacing it here would hide those from every consumer of getFieldInfos(). In the composite
        // model a Parquet-resident numeric is absent from Lucene, because LuceneDocumentInput skips
        // fields for which the mapping declares no Lucene capability.
        for (MappedFieldType mft : mapperService.fieldTypes()) {
            String name = mft.name();
            if (mapperService.isMetadataField(name)) {
                continue;
            }
            if (FieldTypeMapping.isSupported(mft.typeName()) == false) {
                continue;
            }
            // Fields mapped doc_values:false must stay absent, not fail at read time.
            if (mft.hasDocValues() == false) {
                continue;
            }
            if (existing.fieldInfo(name) != null) {
                continue;
            }
            DocValuesType dvType = FieldTypeMapping.forType(mft.typeName());
            FieldInfo synthetic = newDocValuesFieldInfo(name, ++maxNumber, dvType);
            parquetFields.put(name, synthetic);
            combined.add(synthetic);
        }

        if (parquetFields.isEmpty()) {
            return in;
        }

        // Eager, per segment core: the core cache key is the producer's lifecycle anchor, so a leaf
        // with no core cache helper has no safe scope to attach to and must fail rather than leak.
        IndexReader.CacheHelper coreHelper = in.getCoreCacheHelper();
        if (coreHelper == null) {
            throw new IOException("segment leaf exposes no core cache helper; cannot scope Parquet doc-values producer");
        }
        ParquetDocValuesProducer producer = ParquetDocValuesProducerRegistry.getOrCreate(
            coreHelper,
            () -> new ParquetDocValuesProducer(state, mapperService)
        );

        FieldInfos combinedFieldInfos = new FieldInfos(combined.toArray(new FieldInfo[0]));
        return new ParquetDocValuesLeafReader(in, parquetFields, combinedFieldInfos, producer);
    }

    /** Builds a synthetic doc-values {@link FieldInfo}. Skip index is NONE: the codec serves no skipper. */
    private static FieldInfo newDocValuesFieldInfo(String name, int number, DocValuesType dvType) {
        return new FieldInfo(
            name,
            number,
            false,                       // storeTermVector
            true,                        // omitNorms
            false,                       // storePayloads
            IndexOptions.NONE,           // not indexed via this reader
            dvType,
            DocValuesSkipIndexType.NONE,
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

    private FieldInfo parquetFieldInfo(String field) {
        return parquetFields.get(field);
    }

    /**
     * Confirms the write path's guarantee that docId == Parquet row for this segment. Enabled only
     * with assertions on; a mismatch would mean the identity read is unsafe.
     *
     * <p>Scans the whole segment, so the result is memoized: the property is per-segment, and every
     * doc-values request on this leaf would otherwise repeat the scan.
     */
    private synchronized boolean assertRowIdsAreIdentity() throws IOException {
        if (rowIdsChecked) {
            return rowIdsAreIdentity;
        }
        rowIdsChecked = true;
        rowIdsAreIdentity = computeRowIdsAreIdentity();
        return rowIdsAreIdentity;
    }

    private boolean computeRowIdsAreIdentity() throws IOException {
        SortedNumericDocValues rowId = in.getSortedNumericDocValues(DocumentInput.ROW_ID_FIELD);
        if (rowId == null) {
            return true; // no row-id field => identity by definition
        }
        for (int docId = 0; docId < maxDoc(); docId++) {
            if (rowId.advanceExact(docId) == false || rowId.nextValue() != docId) {
                return false;
            }
        }
        return true;
    }

    @Override
    public FieldInfos getFieldInfos() {
        return combinedFieldInfos;
    }

    @Override
    public NumericDocValues getNumericDocValues(String field) throws IOException {
        if (parquetFieldInfo(field) != null) {
            // Synthesized Parquet fields are SORTED_NUMERIC; like CodecReader, an accessor whose DV
            // type does not match the FieldInfo returns null rather than serving the field.
            return null;
        }
        return in.getNumericDocValues(field);
    }

    @Override
    public SortedNumericDocValues getSortedNumericDocValues(String field) throws IOException {
        FieldInfo fi = parquetFieldInfo(field);
        if (fi != null) {
            // OpenSearch numeric value sources request SORTED_NUMERIC even for single-valued fields,
            // then call DocValues.unwrapSingleton(...). The producer serves this as a singleton over
            // the single-valued numeric iterator (docId == Parquet row, asserted above). The cursor is
            // recorded on this request's registry and closed when the request ends.
            assert assertRowIdsAreIdentity() : "non-identity __row_id__ segment reached the Parquet doc-values read path";
            return producer.getSortedNumeric(fi, cursors);
        }
        return in.getSortedNumericDocValues(field);
    }

    @Override
    protected void doClose() throws IOException {
        closeParquetResources();
        super.doClose();
    }

    /**
     * Closes the cursors this request opened, without touching the shared producer or the underlying
     * Lucene leaf. The request-scoped directory reader calls this explicitly before closing its
     * non-closing delegate. The producer outlives the request and is closed by the segment core.
     */
    void closeParquetResources() throws IOException {
        cursors.close();
    }

    @Override
    protected StoredFieldsReader doGetSequentialStoredFieldsReader(StoredFieldsReader reader) {
        // This reader overlays doc values only; the underlying segment holds the real stored fields.
        return reader;
    }

    @Override
    public CacheHelper getCoreCacheHelper() {
        return in.getCoreCacheHelper();
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        return in.getReaderCacheHelper();
    }
}
