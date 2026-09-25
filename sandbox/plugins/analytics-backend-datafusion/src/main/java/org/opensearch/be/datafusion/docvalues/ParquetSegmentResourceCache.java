/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.docvalues;

import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.IOContext;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Per-index cache of one {@link ParquetSegmentResources} per segment core, keyed on the leaf's core cache
 * key.
 *
 * <p>The producer, the synthesized {@link FieldInfo}s and the combined {@link FieldInfos} are built
 * once when a segment core is first seen and reused by every request and every {@code openIfChanged}
 * over that core. File resolve, metadata read and format-version gate run once per core, not per
 * request. A core that serves no Parquet doc values caches the {@link ParquetSegmentResources#ABSENT}
 * sentinel, so the negative resolve is not repeated either. The resources are closed and dropped by
 * the core's closed-listener, so their file-level handles die with the segment core.
 *
 * <p>TODO: generalize the core-keyed map + closed-listener eviction when a second per-core value needs
 * it (sorted-set producer, uninverted ordinals); only getOrCreate(coreHelper, factory) and
 * close-on-evict are required.
 */
public final class ParquetSegmentResourceCache {

    private final MapperService mapperService;
    private final Map<IndexReader.CacheKey, ParquetSegmentResources> resourceByCore = new ConcurrentHashMap<>();
    private final Object buildLock = new Object();

    public ParquetSegmentResourceCache(MapperService mapperService) {
        this.mapperService = mapperService;
    }

    /**
     * The resources for {@code in}'s segment core, building them on first use. Returns
     * {@link ParquetSegmentResources#ABSENT} when the core serves no Parquet doc values.
     */
    ParquetSegmentResources resourcesFor(LeafReader in) throws IOException {
        IndexReader.CacheHelper coreHelper = in.getCoreCacheHelper();
        if (coreHelper != null) {
            ParquetSegmentResources cached = resourceByCore.get(coreHelper.getKey());
            if (cached != null) {
                return cached;
            }
        }
        synchronized (buildLock) {
            if (coreHelper != null) {
                ParquetSegmentResources cached = resourceByCore.get(coreHelper.getKey());
                if (cached != null) {
                    return cached;
                }
            }
            return build(in, coreHelper);
        }
    }

    private ParquetSegmentResources build(LeafReader in, IndexReader.CacheHelper coreHelper) throws IOException {
        SegmentReader segmentReader;
        try {
            segmentReader = Lucene.segmentReader(in);
        } catch (RuntimeException e) {
            // Not a segment-backed leaf; it has no stable segment core to key on and serves no Parquet doc values.
            return ParquetSegmentResources.ABSENT;
        }

        // The core cache key is the resources' lifecycle anchor, so a segment leaf with no core cache
        // helper has no safe scope to attach to and must fail rather than leak.
        if (coreHelper == null) {
            throw new IOException("segment leaf exposes no core cache helper; cannot scope Parquet doc-values producer");
        }
        IndexReader.CacheKey key = coreHelper.getKey();

        SegmentReadState state = new SegmentReadState(
            segmentReader.directory(),
            segmentReader.getSegmentInfo().info,
            segmentReader.getFieldInfos(),
            IOContext.DEFAULT
        );

        if (ParquetSegmentLayout.resolve(state) == null) {
            return cacheAbsent(coreHelper, key);
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
            DocValuesType dvType = FieldTypeMapping.forType(mft.typeName());
            FieldInfo synthetic = newDocValuesFieldInfo(name, ++maxNumber, dvType);
            parquetFields.put(name, synthetic);
            combined.add(synthetic);
        }

        if (parquetFields.isEmpty()) {
            return cacheAbsent(coreHelper, key);
        }

        ParquetDocValuesProducer producer = new ParquetDocValuesProducer(state, mapperService);
        FieldInfos combinedFieldInfos = new FieldInfos(combined.toArray(new FieldInfo[0]));
        ParquetSegmentResources built = new ParquetSegmentResources(producer, parquetFields, combinedFieldInfos);
        resourceByCore.put(key, built);
        // Registered once, in the create branch only, so a core carries exactly one listener no matter
        // how many requests wrap its leaf.
        coreHelper.addClosedListener(this::onCoreClosed);
        return built;
    }

    private ParquetSegmentResources cacheAbsent(IndexReader.CacheHelper coreHelper, IndexReader.CacheKey key) {
        resourceByCore.put(key, ParquetSegmentResources.ABSENT);
        coreHelper.addClosedListener(this::onCoreClosed);
        return ParquetSegmentResources.ABSENT;
    }

    /** Drops the resources bound to a segment core when Lucene drops the core, closing their producer. */
    private void onCoreClosed(IndexReader.CacheKey key) throws IOException {
        ParquetSegmentResources removed = resourceByCore.remove(key);
        if (removed != null && removed.producer != null) {
            removed.producer.close();
        }
    }

    /**
     * Test seam: installs {@code resources} for {@code in}'s segment core, or returns the resources
     * already cached for it, registering the core's closed-listener on first install.
     */
    ParquetSegmentResources cacheForTesting(LeafReader in, ParquetSegmentResources resources) {
        IndexReader.CacheHelper coreHelper = in.getCoreCacheHelper();
        IndexReader.CacheKey key = coreHelper.getKey();
        ParquetSegmentResources existing = resourceByCore.get(key);
        if (existing != null) {
            return existing;
        }
        synchronized (buildLock) {
            existing = resourceByCore.get(key);
            if (existing != null) {
                return existing;
            }
            resourceByCore.put(key, resources);
            coreHelper.addClosedListener(this::onCoreClosed);
            return resources;
        }
    }

    /** Cached resource count (tests). */
    int size() {
        return resourceByCore.size();
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
}
