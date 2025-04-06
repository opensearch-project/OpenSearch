/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.cache;

import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.Accountable;
import org.opensearch.common.cache.Cache;
import org.opensearch.common.cache.CacheBuilder;
import org.opensearch.common.cache.RemovalListener;
import org.opensearch.common.cache.RemovalNotification;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.AbstractIndexComponent;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.IndexWarmer;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.shard.ShardUtils;
import org.opensearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import org.apache.logging.log4j.message.ParameterizedMessage;


/**
 * Cache for cluster ID document boundaries.
 * This cache stores pre-computed document ranges for each cluster ID to optimize
 * cluster-based document retrieval.
 */
public final class ClusterIdBoundsCache extends AbstractIndexComponent
    implements IndexReader.ClosedListener, RemovalListener<IndexReader.CacheKey, Map<Long, ClusterIdBoundsCache.Value>>, Closeable {


    private static final String CLUSTER_ID_FIELD = "cluster_id";
    private final Cache<IndexReader.CacheKey, Map<Long, Value>> loadedBounds;
    private final Listener listener;

    public ClusterIdBoundsCache(IndexSettings indexSettings, Listener listener) {
        super(indexSettings);
        if (listener == null) {
            throw new IllegalArgumentException("listener must not be null");
        }
        this.loadedBounds = CacheBuilder.<IndexReader.CacheKey, Map<Long, Value>>builder().removalListener(this).build();
        this.listener = listener;
    }


    /**
     * Creates a warmer listener that pre-computes cluster bounds
     */
    public IndexWarmer.Listener createListener(ThreadPool threadPool) {
        return new ClusterBoundsWarmer(threadPool);
    }

    @Override
    public void onClose(IndexReader.CacheKey ownerCoreCacheKey) {
        loadedBounds.invalidate(ownerCoreCacheKey);
    }

    @Override
    public void close() {
        clear("close");
    }

    public void clear(String reason) {
        logger.debug("clearing all cluster bounds because [{}]", reason);
        loadedBounds.invalidateAll();
    }

    private void getClustersBounds(final LeafReaderContext context) throws ExecutionException, IOException {
        final IndexReader.CacheHelper cacheHelper = FilterLeafReader.unwrap(context.reader()).getCoreCacheHelper();
        if (cacheHelper == null) {
            throw new IllegalArgumentException("Reader " + context.reader() + " does not support caching");
        }
        final IndexReader.CacheKey coreCacheReader = cacheHelper.getKey();
        final ShardId shardId = ShardUtils.extractShardId(context.reader());
        if (indexSettings.getIndex().equals(shardId.getIndex()) == false) {
            throw new IllegalStateException(
                "Trying to load cluster bounds for index " + shardId.getIndex() + " with cache of index " + indexSettings.getIndex()
            );
        }

        Map<Long, Value> clusterToBounds = new HashMap<>();
        SortedNumericDocValues docValues = DocValues.getSortedNumeric(context.reader(), CLUSTER_ID_FIELD);
        int doc = docValues.nextDoc();
        while (doc != DocIdSetIterator.NO_MORE_DOCS) {
            long value = docValues.nextValue();
            if (clusterToBounds.containsKey(value)) {
                clusterToBounds.get(value).bounds.upperBound = doc + 1;
            } else {
                clusterToBounds.put(value, new Value(new DocBounds(doc, doc + 1), shardId));
            }
            doc = docValues.nextDoc();
        }
        loadedBounds.put(coreCacheReader, clusterToBounds);
    }

    @Override
    public void onRemoval(RemovalNotification<IndexReader.CacheKey, Map<Long, Value>> notification) {
        if (notification.getKey() == null) {
            return;
        }

        Map<Long, Value> valueCache = notification.getValue();
        if (valueCache == null) {
            return;
        }

        for (Value value : valueCache.values()) {
            if (value != null) {
                listener.onRemoval(value.shardId, value.bounds);
            }
        }
    }

    /**
     * Value for cluster bounds cache
     */
    public static final class Value {
        final DocBounds bounds;
        final ShardId shardId;

        public Value(DocBounds bounds, ShardId shardId) {
            this.bounds = bounds;
            this.shardId = shardId;
        }
    }

    /**
     * Document bounds for a cluster
     */
    public static final class DocBounds implements Accountable {
        int lowerBound; // inclusive
        int upperBound; // exclusive

        public DocBounds(int lowerBound, int upperBound) {
            this.lowerBound = lowerBound;
            this.upperBound = upperBound;
        }

        @Override
        public long ramBytesUsed() {
            return 2 * Integer.BYTES; // Two integers
        }
    }

    final class ClusterBoundsWarmer implements IndexWarmer.Listener {
        private final Executor executor;

        ClusterBoundsWarmer(ThreadPool threadPool) {
            this.executor = threadPool.executor(ThreadPool.Names.WARMER);
        }

        @Override
        public IndexWarmer.TerminationHandle warmReader(final IndexShard indexShard, final OpenSearchDirectoryReader reader) {
            if (indexSettings.getIndex().equals(indexShard.indexSettings().getIndex()) == false) {
                // this is from a different index
                return IndexWarmer.TerminationHandle.NO_WAIT;
            }

            final CountDownLatch latch = new CountDownLatch(reader.leaves().size());
            for (final LeafReaderContext ctx : reader.leaves()) {
                executor.execute(() -> {
                    try {
                        final long start = System.nanoTime();
                        getClustersBounds(ctx);
                        if (indexShard.warmerService().logger().isTraceEnabled()) {
                            indexShard.warmerService()
                                .logger()
                                .trace(
                                    "warmed cluster bounds for [{}], took [{}]",
                                    ctx,
                                    TimeValue.timeValueNanos(System.nanoTime() - start)
                                );
                        }
                    } catch (Exception e) {
                        indexShard.warmerService()
                            .logger()
                            .warn(() -> new ParameterizedMessage("failed to load cluster bounds for [{}]", ctx), e);
                    } finally {
                        latch.countDown();
                    }
                });
            }
            return latch::await;
        }
    }

    Cache<IndexReader.CacheKey, Map<Long, Value>> getLoadedBounds() {
        return loadedBounds;
    }

    /**
     * A listener interface that is executed for each onCache / onRemoval event
     */
    public interface Listener {
        /**
         * Called for each cached bounds on the cache event.
         * @param shardId the shard id the bounds was cached for. This can be <code>null</code>
         * @param accountable the bounds ram representation
         */
        void onCache(ShardId shardId, Accountable accountable);

        /**
         * Called for each cached bounds on the removal event.
         * @param shardId the shard id the bounds was cached for. This can be <code>null</code>
         * @param accountable the bounds ram representation
         */
        void onRemoval(ShardId shardId, Accountable accountable);
    }
}

