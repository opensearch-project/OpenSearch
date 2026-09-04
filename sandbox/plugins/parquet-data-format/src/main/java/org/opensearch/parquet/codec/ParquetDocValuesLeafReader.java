/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec;

import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexOptions;
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
 * from a per-segment producer. All other fields pass through unchanged.
 *
 * <p>It extends {@link SequentialStoredFieldsLeafReader} (not plain {@code FilterLeafReader}) so the
 * fetch phase can still retrieve stored fields: the derived-source layer above unwraps to this reader,
 * which passes the underlying segment's stored-fields reader straight through.
 *
 * <p>One producer is built lazily per segment and closed when this reader closes; it is not shared
 * across segments or across requests.
 */
public final class ParquetDocValuesLeafReader extends SequentialStoredFieldsLeafReader {

    private final MapperService mapperService;
    private final SegmentReadState segmentReadState;
    private final Map<String, FieldInfo> parquetFields;
    private final FieldInfos combinedFieldInfos;

    private ParquetDocValuesProducer producer;
    private boolean producerInitialized;

    /** Memoized result of the assertions-only row-id identity check; see {@link #assertRowIdsAreIdentity}. */
    private boolean rowIdsChecked;
    private boolean rowIdsAreIdentity;

    private ParquetDocValuesLeafReader(
        LeafReader in,
        MapperService mapperService,
        SegmentReadState segmentReadState,
        Map<String, FieldInfo> parquetFields,
        FieldInfos combinedFieldInfos
    ) {
        super(in);
        this.mapperService = mapperService;
        this.segmentReadState = segmentReadState;
        this.parquetFields = parquetFields;
        this.combinedFieldInfos = combinedFieldInfos;
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
            if (existing.fieldInfo(name) != null) {
                continue;
            }
            DocValuesType dvType = FieldTypeMapping.forType(mft.typeName()).singleValued();
            FieldInfo synthetic = newDocValuesFieldInfo(name, ++maxNumber, dvType);
            parquetFields.put(name, synthetic);
            combined.add(synthetic);
        }

        if (parquetFields.isEmpty()) {
            return in;
        }

        FieldInfos combinedFieldInfos = new FieldInfos(combined.toArray(new FieldInfo[0]));
        return new ParquetDocValuesLeafReader(in, mapperService, state, parquetFields, combinedFieldInfos);
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

    private synchronized ParquetDocValuesProducer producer() throws IOException {
        if (producerInitialized == false) {
            producer = new ParquetDocValuesProducer(segmentReadState, mapperService);
            producerInitialized = true;
        }
        return producer;
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
        FieldInfo fi = parquetFieldInfo(field);
        if (fi != null) {
            // Every synthesized Parquet field is single-valued NUMERIC (FieldTypeMapping.singleValued()).
            assert assertRowIdsAreIdentity() : "non-identity __row_id__ segment reached the Parquet doc-values read path";
            return producer().getNumeric(fi);
        }
        return in.getNumericDocValues(field);
    }

    @Override
    public SortedNumericDocValues getSortedNumericDocValues(String field) throws IOException {
        FieldInfo fi = parquetFieldInfo(field);
        if (fi != null) {
            // OpenSearch numeric value sources request SORTED_NUMERIC even for single-valued fields,
            // then call DocValues.unwrapSingleton(...). The producer serves this as a singleton over
            // the single-valued numeric iterator (docId == Parquet row, asserted above).
            assert assertRowIdsAreIdentity() : "non-identity __row_id__ segment reached the Parquet doc-values read path";
            return producer().getSortedNumeric(fi);
        }
        return in.getSortedNumericDocValues(field);
    }

    @Override
    protected void doClose() throws IOException {
        closeParquetResources();
        super.doClose();
    }

    /**
     * Releases the producer owned by this wrapper without closing the underlying Lucene leaf. The
     * request-scoped directory reader calls this explicitly before closing its non-closing delegate.
     */
    synchronized void closeParquetResources() throws IOException {
        if (producer != null) {
            producer.close();
        }
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
