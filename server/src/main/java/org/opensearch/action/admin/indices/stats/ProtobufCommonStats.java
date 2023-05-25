/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.action.admin.indices.stats;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import org.opensearch.common.Nullable;
import org.opensearch.common.io.stream.ProtobufStreamInput;
import org.opensearch.common.io.stream.ProtobufStreamOutput;
import org.opensearch.common.io.stream.ProtobufWriteable;
import org.opensearch.common.unit.ByteSizeValue;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.cache.query.ProtobufQueryCacheStats;
import org.opensearch.index.cache.request.ProtobufRequestCacheStats;
import org.opensearch.index.engine.ProtobufSegmentsStats;
import org.opensearch.index.fielddata.ProtobufFieldDataStats;
import org.opensearch.index.flush.ProtobufFlushStats;
import org.opensearch.index.get.ProtobufGetStats;
import org.opensearch.index.merge.ProtobufMergeStats;
import org.opensearch.index.recovery.ProtobufRecoveryStats;
import org.opensearch.index.refresh.ProtobufRefreshStats;
import org.opensearch.index.search.stats.ProtobufSearchStats;
import org.opensearch.index.shard.ProtobufDocsStats;
import org.opensearch.index.shard.ProtobufIndexingStats;
import org.opensearch.index.store.ProtobufStoreStats;
import org.opensearch.index.translog.ProtobufTranslogStats;
import org.opensearch.index.warmer.ProtobufWarmerStats;
import org.opensearch.search.suggest.completion.ProtobufCompletionStats;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * Common Stats for OpenSearch
*
* @opensearch.internal
*/
public class ProtobufCommonStats implements ProtobufWriteable, ToXContentFragment {

    @Nullable
    public ProtobufDocsStats docs;

    @Nullable
    public ProtobufStoreStats store;

    @Nullable
    public ProtobufIndexingStats indexing;

    @Nullable
    public ProtobufGetStats get;

    @Nullable
    public ProtobufSearchStats search;

    @Nullable
    public ProtobufMergeStats merge;

    @Nullable
    public ProtobufRefreshStats refresh;

    @Nullable
    public ProtobufFlushStats flush;

    @Nullable
    public ProtobufWarmerStats warmer;

    @Nullable
    public ProtobufQueryCacheStats queryCache;

    @Nullable
    public ProtobufFieldDataStats fieldData;

    @Nullable
    public ProtobufCompletionStats completion;

    @Nullable
    public ProtobufSegmentsStats segments;

    @Nullable
    public ProtobufTranslogStats translog;

    @Nullable
    public ProtobufRequestCacheStats requestCache;

    @Nullable
    public ProtobufRecoveryStats recoveryStats;

    public ProtobufCommonStats() {
        this(ProtobufCommonStatsFlags.NONE);
    }

    public ProtobufCommonStats(ProtobufCommonStatsFlags flags) {
        ProtobufCommonStatsFlags.Flag[] setFlags = flags.getFlags();

        for (ProtobufCommonStatsFlags.Flag flag : setFlags) {
            switch (flag) {
                case Docs:
                    docs = new ProtobufDocsStats();
                    break;
                case Store:
                    store = new ProtobufStoreStats();
                    break;
                case Indexing:
                    indexing = new ProtobufIndexingStats();
                    break;
                case Get:
                    get = new ProtobufGetStats();
                    break;
                case Search:
                    search = new ProtobufSearchStats();
                    break;
                case Merge:
                    merge = new ProtobufMergeStats();
                    break;
                case Refresh:
                    refresh = new ProtobufRefreshStats();
                    break;
                case Flush:
                    flush = new ProtobufFlushStats();
                    break;
                case Warmer:
                    warmer = new ProtobufWarmerStats();
                    break;
                case QueryCache:
                    queryCache = new ProtobufQueryCacheStats();
                    break;
                case FieldData:
                    fieldData = new ProtobufFieldDataStats();
                    break;
                case Completion:
                    completion = new ProtobufCompletionStats();
                    break;
                case Segments:
                    segments = new ProtobufSegmentsStats();
                    break;
                case Translog:
                    translog = new ProtobufTranslogStats();
                    break;
                case RequestCache:
                    requestCache = new ProtobufRequestCacheStats();
                    break;
                case Recovery:
                    recoveryStats = new ProtobufRecoveryStats();
                    break;
                default:
                    throw new IllegalStateException("Unknown Flag: " + flag);
            }
        }
    }

    public ProtobufCommonStats(CodedInputStream in) throws IOException {
        ProtobufStreamInput protobufStreamInput = new ProtobufStreamInput(in);
        docs = protobufStreamInput.readOptionalWriteable(ProtobufDocsStats::new);
        store = protobufStreamInput.readOptionalWriteable(ProtobufStoreStats::new);
        indexing = protobufStreamInput.readOptionalWriteable(ProtobufIndexingStats::new);
        get = protobufStreamInput.readOptionalWriteable(ProtobufGetStats::new);
        search = protobufStreamInput.readOptionalWriteable(ProtobufSearchStats::new);
        merge = protobufStreamInput.readOptionalWriteable(ProtobufMergeStats::new);
        refresh = protobufStreamInput.readOptionalWriteable(ProtobufRefreshStats::new);
        flush = protobufStreamInput.readOptionalWriteable(ProtobufFlushStats::new);
        warmer = protobufStreamInput.readOptionalWriteable(ProtobufWarmerStats::new);
        queryCache = protobufStreamInput.readOptionalWriteable(ProtobufQueryCacheStats::new);
        fieldData = protobufStreamInput.readOptionalWriteable(ProtobufFieldDataStats::new);
        completion = protobufStreamInput.readOptionalWriteable(ProtobufCompletionStats::new);
        segments = protobufStreamInput.readOptionalWriteable(ProtobufSegmentsStats::new);
        translog = protobufStreamInput.readOptionalWriteable(ProtobufTranslogStats::new);
        requestCache = protobufStreamInput.readOptionalWriteable(ProtobufRequestCacheStats::new);
        recoveryStats = protobufStreamInput.readOptionalWriteable(ProtobufRecoveryStats::new);
    }

    @Override
    public void writeTo(CodedOutputStream out) throws IOException {
        ProtobufStreamOutput protobufStreamOutput = new ProtobufStreamOutput(out);
        protobufStreamOutput.writeOptionalWriteable(docs);
        protobufStreamOutput.writeOptionalWriteable(store);
        protobufStreamOutput.writeOptionalWriteable(indexing);
        protobufStreamOutput.writeOptionalWriteable(get);
        protobufStreamOutput.writeOptionalWriteable(search);
        protobufStreamOutput.writeOptionalWriteable(merge);
        protobufStreamOutput.writeOptionalWriteable(refresh);
        protobufStreamOutput.writeOptionalWriteable(flush);
        protobufStreamOutput.writeOptionalWriteable(warmer);
        protobufStreamOutput.writeOptionalWriteable(queryCache);
        protobufStreamOutput.writeOptionalWriteable(fieldData);
        protobufStreamOutput.writeOptionalWriteable(completion);
        protobufStreamOutput.writeOptionalWriteable(segments);
        protobufStreamOutput.writeOptionalWriteable(translog);
        protobufStreamOutput.writeOptionalWriteable(requestCache);
        protobufStreamOutput.writeOptionalWriteable(recoveryStats);
    }

    public void add(ProtobufCommonStats stats) {
        if (docs == null) {
            if (stats.getDocs() != null) {
                docs = new ProtobufDocsStats();
                docs.add(stats.getDocs());
            }
        } else {
            docs.add(stats.getDocs());
        }
        if (store == null) {
            if (stats.getStore() != null) {
                store = new ProtobufStoreStats();
                store.add(stats.getStore());
            }
        } else {
            store.add(stats.getStore());
        }
        if (indexing == null) {
            if (stats.getIndexing() != null) {
                indexing = new ProtobufIndexingStats();
                indexing.add(stats.getIndexing());
            }
        } else {
            indexing.add(stats.getIndexing());
        }
        if (get == null) {
            if (stats.getGet() != null) {
                get = new ProtobufGetStats();
                get.add(stats.getGet());
            }
        } else {
            get.add(stats.getGet());
        }
        if (search == null) {
            if (stats.getSearch() != null) {
                search = new ProtobufSearchStats();
                search.add(stats.getSearch());
            }
        } else {
            search.add(stats.getSearch());
        }
        if (merge == null) {
            if (stats.getMerge() != null) {
                merge = new ProtobufMergeStats();
                merge.add(stats.getMerge());
            }
        } else {
            merge.add(stats.getMerge());
        }
        if (refresh == null) {
            if (stats.getRefresh() != null) {
                refresh = new ProtobufRefreshStats();
                refresh.add(stats.getRefresh());
            }
        } else {
            refresh.add(stats.getRefresh());
        }
        if (flush == null) {
            if (stats.getFlush() != null) {
                flush = new ProtobufFlushStats();
                flush.add(stats.getFlush());
            }
        } else {
            flush.add(stats.getFlush());
        }
        if (warmer == null) {
            if (stats.getWarmer() != null) {
                warmer = new ProtobufWarmerStats();
                warmer.add(stats.getWarmer());
            }
        } else {
            warmer.add(stats.getWarmer());
        }
        if (queryCache == null) {
            if (stats.getQueryCache() != null) {
                queryCache = new ProtobufQueryCacheStats();
                queryCache.add(stats.getQueryCache());
            }
        } else {
            queryCache.add(stats.getQueryCache());
        }

        if (fieldData == null) {
            if (stats.getFieldData() != null) {
                fieldData = new ProtobufFieldDataStats();
                fieldData.add(stats.getFieldData());
            }
        } else {
            fieldData.add(stats.getFieldData());
        }
        if (completion == null) {
            if (stats.getCompletion() != null) {
                completion = new ProtobufCompletionStats();
                completion.add(stats.getCompletion());
            }
        } else {
            completion.add(stats.getCompletion());
        }
        if (segments == null) {
            if (stats.getSegments() != null) {
                segments = new ProtobufSegmentsStats();
                segments.add(stats.getSegments());
            }
        } else {
            segments.add(stats.getSegments());
        }
        if (translog == null) {
            if (stats.getTranslog() != null) {
                translog = new ProtobufTranslogStats();
                translog.add(stats.getTranslog());
            }
        } else {
            translog.add(stats.getTranslog());
        }
        if (requestCache == null) {
            if (stats.getRequestCache() != null) {
                requestCache = new ProtobufRequestCacheStats();
                requestCache.add(stats.getRequestCache());
            }
        } else {
            requestCache.add(stats.getRequestCache());
        }
        if (recoveryStats == null) {
            if (stats.getRecoveryStats() != null) {
                recoveryStats = new ProtobufRecoveryStats();
                recoveryStats.add(stats.getRecoveryStats());
            }
        } else {
            recoveryStats.add(stats.getRecoveryStats());
        }
    }

    @Nullable
    public ProtobufDocsStats getDocs() {
        return this.docs;
    }

    @Nullable
    public ProtobufStoreStats getStore() {
        return store;
    }

    @Nullable
    public ProtobufIndexingStats getIndexing() {
        return indexing;
    }

    @Nullable
    public ProtobufGetStats getGet() {
        return get;
    }

    @Nullable
    public ProtobufSearchStats getSearch() {
        return search;
    }

    @Nullable
    public ProtobufMergeStats getMerge() {
        return merge;
    }

    @Nullable
    public ProtobufRefreshStats getRefresh() {
        return refresh;
    }

    @Nullable
    public ProtobufFlushStats getFlush() {
        return flush;
    }

    @Nullable
    public ProtobufWarmerStats getWarmer() {
        return this.warmer;
    }

    @Nullable
    public ProtobufQueryCacheStats getQueryCache() {
        return this.queryCache;
    }

    @Nullable
    public ProtobufFieldDataStats getFieldData() {
        return this.fieldData;
    }

    @Nullable
    public ProtobufCompletionStats getCompletion() {
        return completion;
    }

    @Nullable
    public ProtobufSegmentsStats getSegments() {
        return segments;
    }

    @Nullable
    public ProtobufTranslogStats getTranslog() {
        return translog;
    }

    @Nullable
    public ProtobufRequestCacheStats getRequestCache() {
        return requestCache;
    }

    @Nullable
    public ProtobufRecoveryStats getRecoveryStats() {
        return recoveryStats;
    }

    /**
     * Utility method which computes total memory by adding
    * FieldData, PercolatorCache, Segments (index writer, version map)
    */
    public ByteSizeValue getTotalMemory() {
        long size = 0;
        if (this.getFieldData() != null) {
            size += this.getFieldData().getMemorySizeInBytes();
        }
        if (this.getQueryCache() != null) {
            size += this.getQueryCache().getMemorySizeInBytes();
        }
        if (this.getSegments() != null) {
            size += this.getSegments().getIndexWriterMemoryInBytes() + this.getSegments().getVersionMapMemoryInBytes();
        }

        return new ByteSizeValue(size);
    }

    // note, requires a wrapping object
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        final Stream<ToXContent> stream = Arrays.stream(
            new ToXContent[] {
                docs,
                store,
                indexing,
                get,
                search,
                merge,
                refresh,
                flush,
                warmer,
                queryCache,
                fieldData,
                completion,
                segments,
                translog,
                requestCache,
                recoveryStats }
        ).filter(Objects::nonNull);
        for (ToXContent toXContent : ((Iterable<ToXContent>) stream::iterator)) {
            toXContent.toXContent(builder, params);
        }
        return builder;
    }
}
