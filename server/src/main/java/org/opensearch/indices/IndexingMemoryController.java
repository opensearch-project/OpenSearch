/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.indices;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.store.AlreadyClosedException;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.AbstractRunnable;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardState;
import org.opensearch.index.shard.IndexingOperationListener;
import org.opensearch.threadpool.Scheduler.Cancellable;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.threadpool.ThreadPool.Names;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Memory controller for indexing operations
 *
 * @opensearch.internal
 */
public class IndexingMemoryController implements IndexingOperationListener, Closeable {

    private static final Logger logger = LogManager.getLogger(IndexingMemoryController.class);

    /** How much heap (% or bytes) we will share across all actively indexing shards on this node (default: 10%). */
    public static final Setting<ByteSizeValue> INDEX_BUFFER_SIZE_SETTING = Setting.memorySizeSetting(
        "indices.memory.index_buffer_size",
        "10%",
        Property.NodeScope
    );

    /** Only applies when <code>indices.memory.index_buffer_size</code> is a %,
     * to set a floor on the actual size in bytes (default: 48 MB). */
    public static final Setting<ByteSizeValue> MIN_INDEX_BUFFER_SIZE_SETTING = Setting.byteSizeSetting(
        "indices.memory.min_index_buffer_size",
        new ByteSizeValue(48, ByteSizeUnit.MB),
        new ByteSizeValue(0, ByteSizeUnit.BYTES),
        new ByteSizeValue(Long.MAX_VALUE, ByteSizeUnit.BYTES),
        Property.NodeScope
    );

    /** Only applies when <code>indices.memory.index_buffer_size</code> is a %,
     * to set a ceiling on the actual size in bytes (default: not set). */
    public static final Setting<ByteSizeValue> MAX_INDEX_BUFFER_SIZE_SETTING = Setting.byteSizeSetting(
        "indices.memory.max_index_buffer_size",
        new ByteSizeValue(-1),
        new ByteSizeValue(-1),
        new ByteSizeValue(Long.MAX_VALUE, ByteSizeUnit.BYTES),
        Property.NodeScope
    );

    /** If we see no indexing operations after this much time for a given shard,
     * we consider that shard inactive (default: 5 minutes). */
    public static final Setting<TimeValue> SHARD_INACTIVE_TIME_SETTING = Setting.positiveTimeSetting(
        "indices.memory.shard_inactive_time",
        TimeValue.timeValueMinutes(5),
        Property.NodeScope
    );

    /** How frequently we check indexing memory usage (default: 5 seconds). */
    public static final Setting<TimeValue> SHARD_MEMORY_INTERVAL_TIME_SETTING = Setting.positiveTimeSetting(
        "indices.memory.interval",
        TimeValue.timeValueSeconds(5),
        Property.NodeScope
    );

    private final ThreadPool threadPool;

    private final Iterable<IndexShard> indexShards;

    private final ByteSizeValue indexingBuffer;

    private final TimeValue inactiveTime;
    private final TimeValue interval;

    /** Percentage of (ingest pool + write pool) to use as native memory budget. */
    public static final Setting<Double> NATIVE_INDEX_BUFFER_PERCENT_SETTING = Setting.doubleSetting(
        "indices.memory.native_index_buffer_percent",
        80.0,
        0.0,
        100.0,
        Property.NodeScope
    );

    /** Native allocator for querying pool limits. Set after plugins load. */
    private volatile org.opensearch.arrow.spi.NativeAllocator nativeAllocator;
    private final double nativeBufferPercent;

    /** Contains shards currently being throttled because we can't write segments quickly enough */
    private final Set<IndexShard> throttled = new HashSet<>();

    private final Cancellable scheduler;

    private static final EnumSet<IndexShardState> CAN_WRITE_INDEX_BUFFER_STATES = EnumSet.of(
        IndexShardState.RECOVERING,
        IndexShardState.POST_RECOVERY,
        IndexShardState.STARTED
    );

    private final ShardsIndicesStatusChecker statusChecker;

    IndexingMemoryController(Settings settings, ThreadPool threadPool, Iterable<IndexShard> indexServices) {
        this.indexShards = indexServices;

        ByteSizeValue indexingBuffer = INDEX_BUFFER_SIZE_SETTING.get(settings);

        String indexingBufferSetting = settings.get(INDEX_BUFFER_SIZE_SETTING.getKey());
        // null means we used the default (10%)
        if (indexingBufferSetting == null || indexingBufferSetting.endsWith("%")) {
            // We only apply the min/max when % value was used for the index buffer:
            ByteSizeValue minIndexingBuffer = MIN_INDEX_BUFFER_SIZE_SETTING.get(settings);
            ByteSizeValue maxIndexingBuffer = MAX_INDEX_BUFFER_SIZE_SETTING.get(settings);
            if (indexingBuffer.getBytes() < minIndexingBuffer.getBytes()) {
                indexingBuffer = minIndexingBuffer;
            }
            if (maxIndexingBuffer.getBytes() != -1 && indexingBuffer.getBytes() > maxIndexingBuffer.getBytes()) {
                indexingBuffer = maxIndexingBuffer;
            }
        }
        this.indexingBuffer = indexingBuffer;

        this.inactiveTime = SHARD_INACTIVE_TIME_SETTING.get(settings);
        // we need to have this relatively small to free up heap quickly enough
        this.interval = SHARD_MEMORY_INTERVAL_TIME_SETTING.get(settings);

        this.nativeBufferPercent = NATIVE_INDEX_BUFFER_PERCENT_SETTING.get(settings);

        this.statusChecker = new ShardsIndicesStatusChecker();

        logger.debug(
            "using indexing buffer size [{}] with {} [{}], {} [{}]",
            this.indexingBuffer,
            SHARD_INACTIVE_TIME_SETTING.getKey(),
            this.inactiveTime,
            SHARD_MEMORY_INTERVAL_TIME_SETTING.getKey(),
            this.interval
        );
        this.scheduler = scheduleTask(threadPool);

        // Need to save this so we can later launch async "write indexing buffer to disk" on shards:
        this.threadPool = threadPool;
    }

    /** Sets the native allocator after plugins are loaded. */
    public void setNativeAllocator(org.opensearch.arrow.spi.NativeAllocator allocator) {
        this.nativeAllocator = allocator;
    }

    /** Returns how much native (off-heap) memory this shard is using for its indexing buffer. */
    protected long getNativeBytesUsed(IndexShard shard) {
        return shard.getNativeBytesUsed();
    }

    protected Cancellable scheduleTask(ThreadPool threadPool) {
        // it's fine to run it on the scheduler thread, no busy work
        return threadPool.scheduleWithFixedDelay(statusChecker, interval, Names.SAME);
    }

    @Override
    public void close() {
        scheduler.cancel();
    }

    /**
     * returns the current budget for the total amount of indexing buffers of
     * active shards on this node
     */
    ByteSizeValue indexingBufferSize() {
        return indexingBuffer;
    }

    protected List<IndexShard> availableShards() {
        List<IndexShard> availableShards = new ArrayList<>();
        for (IndexShard shard : indexShards) {
            if (CAN_WRITE_INDEX_BUFFER_STATES.contains(shard.state())) {
                availableShards.add(shard);
            }
        }
        return availableShards;
    }

    /** returns how much heap this shard is using for its indexing buffer */
    protected long getIndexBufferRAMBytesUsed(IndexShard shard) {
        return shard.getIndexBufferRAMBytesUsed();
    }

    /** returns how many bytes this shard is currently writing to disk */
    protected long getShardWritingBytes(IndexShard shard) {
        return shard.getWritingBytes();
    }

    /** ask this shard to refresh, in the background, to free up heap */
    protected void writeIndexingBufferAsync(IndexShard shard) {
        threadPool.executor(ThreadPool.Names.REFRESH).execute(new AbstractRunnable() {
            @Override
            public void doRun() {
                shard.writeIndexingBuffer();
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn(() -> new ParameterizedMessage("failed to write indexing buffer for shard [{}]; ignoring", shard.shardId()), e);
            }
        });
    }

    /** force checker to run now */
    void forceCheck() {
        statusChecker.run();
    }

    /** Asks this shard to throttle indexing to one thread */
    protected void activateThrottling(IndexShard shard) {
        shard.activateThrottling();
    }

    /** Asks this shard to stop throttling indexing to one thread */
    protected void deactivateThrottling(IndexShard shard) {
        shard.deactivateThrottling();
    }

    @Override
    public void postIndex(ShardId shardId, Engine.Index index, Engine.IndexResult result) {
        recordOperationBytes(index, result);
    }

    @Override
    public void postDelete(ShardId shardId, Engine.Delete delete, Engine.DeleteResult result) {
        recordOperationBytes(delete, result);
    }

    /** called by IndexShard to record estimated bytes written to translog for the operation */
    private void recordOperationBytes(Engine.Operation operation, Engine.Result result) {
        if (result.getResultType() == Engine.Result.Type.SUCCESS) {
            statusChecker.bytesWritten(operation.estimatedSizeInBytes());
        }
    }

    /**
     * The bytes used by a shard and a reference to the shard
     *
     * @opensearch.internal
     */
    private static final class ShardAndBytesUsed implements Comparable<ShardAndBytesUsed> {
        final long bytesUsed;
        final IndexShard shard;

        ShardAndBytesUsed(long bytesUsed, IndexShard shard) {
            this.bytesUsed = bytesUsed;
            this.shard = shard;
        }

        @Override
        public int compareTo(ShardAndBytesUsed other) {
            // Sort larger shards first:
            return Long.compare(other.bytesUsed, bytesUsed);
        }
    }

    /** not static because we need access to many fields/methods from our containing class (IMC): */
    final class ShardsIndicesStatusChecker implements Runnable {

        final AtomicLong bytesWrittenSinceCheck = new AtomicLong();
        final ReentrantLock runLock = new ReentrantLock();

        /** Shard calls this on each indexing/delete op */
        public void bytesWritten(int bytes) {
            long totalBytes = bytesWrittenSinceCheck.addAndGet(bytes);
            assert totalBytes >= 0;
            while (totalBytes > indexingBuffer.getBytes() / 30) {

                if (runLock.tryLock()) {
                    try {
                        // Must pull this again because it may have changed since we first checked:
                        totalBytes = bytesWrittenSinceCheck.get();
                        if (totalBytes > indexingBuffer.getBytes() / 30) {
                            bytesWrittenSinceCheck.addAndGet(-totalBytes);
                            // NOTE: this is only an approximate check, because bytes written is to the translog,
                            // vs indexing memory buffer which is typically smaller but can be larger in extreme
                            // cases (many unique terms). This logic is here only as a safety against thread
                            // starvation or too infrequent checking, to ensure we are still checking periodically,
                            // in proportion to bytes processed by indexing:
                            runUnlocked();
                        }
                    } finally {
                        runLock.unlock();
                    }

                    // Must get it again since other threads could have increased it while we were in runUnlocked
                    totalBytes = bytesWrittenSinceCheck.get();
                } else {
                    // Another thread beat us to it: let them do all the work, yay!
                    break;
                }
            }
        }

        @Override
        public void run() {
            runLock.lock();
            try {
                runUnlocked();
            } finally {
                runLock.unlock();
            }
        }

        private void runUnlocked() {
            // NOTE: even if we hit an errant exc here, our ThreadPool.scheduledWithFixedDelay will log the exception and re-invoke us
            // again, on schedule

            // Compute native budget dynamically from allocator pool limits
            long nativeBudget = 0;
            org.opensearch.arrow.spi.NativeAllocator na = nativeAllocator;
            if (na != null) {
                try {
                    org.opensearch.arrow.spi.NativeAllocatorPoolStats stats = na.stats();
                    long ingestLimit = 0, writeLimit = 0;
                    for (var ps : stats.getPools()) {
                        if ("ingest".equals(ps.getName())) ingestLimit = ps.getLimitBytes();
                        if ("write".equals(ps.getName())) writeLimit = ps.getLimitBytes();
                    }
                    nativeBudget = (long) ((ingestLimit + writeLimit) * nativeBufferPercent / 100);
                } catch (Exception e) {
                    logger.debug("Failed to compute native budget from allocator", e);
                }
            }

            // First pass: sum heap and native usage across all shards
            long totalHeapBytesUsed = 0;
            long totalNativeUsed = 0;
            long totalBytesWriting = 0;
            for (IndexShard shard : availableShards()) {

                // Give shard a chance to transition to inactive so we can flush:
                checkIdle(shard, inactiveTime.nanos());

                // How many bytes this shard is currently (async'd) moving from heap to disk:
                long shardWritingBytes = getShardWritingBytes(shard);

                // How many heap bytes this shard is currently using
                long shardBytesUsed = getIndexBufferRAMBytesUsed(shard);

                shardBytesUsed -= shardWritingBytes;
                totalBytesWriting += shardWritingBytes;

                if (shardBytesUsed < 0) {
                    shardBytesUsed = 0;
                }

                totalHeapBytesUsed += shardBytesUsed;
                totalNativeUsed += getNativeBytesUsed(shard);
            }

            if (logger.isTraceEnabled()) {
                logger.trace(
                    "total heap used [{}] vs heap budget [{}], native used [{}] vs native budget [{}], writing [{}]",
                    new ByteSizeValue(totalHeapBytesUsed),
                    indexingBuffer,
                    new ByteSizeValue(totalNativeUsed),
                    new ByteSizeValue(nativeBudget),
                    new ByteSizeValue(totalBytesWriting)
                );
            }

            boolean heapOverBudget = totalHeapBytesUsed > indexingBuffer.getBytes();
            boolean nativeOverBudget = nativeBudget > 0 && totalNativeUsed > nativeBudget;

            // Throttle if significantly over budget
            boolean doThrottle = (totalBytesWriting + totalHeapBytesUsed) > 1.5 * indexingBuffer.getBytes()
                || (nativeBudget > 0 && totalNativeUsed > 1.5 * nativeBudget);

            if (heapOverBudget || nativeOverBudget) {
                if (nativeOverBudget) {
                    logger.info(
                        "IMC native over budget: native_used={}, native_budget={}, triggering refresh",
                        new ByteSizeValue(totalNativeUsed),
                        new ByteSizeValue(nativeBudget)
                    );
                }

                // Build priority queue
                PriorityQueue<ShardAndBytesUsed> queue = new PriorityQueue<>();

                for (IndexShard shard : availableShards()) {
                    long shardWritingBytes = getShardWritingBytes(shard);
                    long shardBytesUsed = getIndexBufferRAMBytesUsed(shard);
                    shardBytesUsed -= shardWritingBytes;
                    if (shardBytesUsed < 0) {
                        shardBytesUsed = 0;
                    }

                    long shardNativeUsed = getNativeBytesUsed(shard);

                    // Sort key depends on which budget is exceeded
                    long sortKey;
                    if (heapOverBudget && nativeOverBudget) {
                        sortKey = shardBytesUsed + shardNativeUsed;
                    } else if (nativeOverBudget) {
                        sortKey = shardNativeUsed;
                    } else {
                        sortKey = shardBytesUsed;
                    }

                    if (sortKey > 0) {
                        queue.add(new ShardAndBytesUsed(sortKey, shard));
                    }
                }

                logger.debug(
                    "now write some indexing buffers: heap used [{}] vs budget [{}], "
                        + "native used [{}] vs budget [{}], writing [{}], [{}] shards queued",
                    new ByteSizeValue(totalHeapBytesUsed),
                    indexingBuffer,
                    new ByteSizeValue(totalNativeUsed),
                    new ByteSizeValue(nativeBudget),
                    new ByteSizeValue(totalBytesWriting),
                    queue.size()
                );

                while ((totalHeapBytesUsed > indexingBuffer.getBytes() || (nativeBudget > 0 && totalNativeUsed > nativeBudget))
                    && queue.isEmpty() == false) {
                    ShardAndBytesUsed largest = queue.poll();
                    logger.debug(
                        "write indexing buffer to disk for shard [{}] to free up [{}] bytes",
                        largest.shard.shardId(),
                        new ByteSizeValue(largest.bytesUsed)
                    );
                    writeIndexingBufferAsync(largest.shard);
                    totalHeapBytesUsed -= getIndexBufferRAMBytesUsed(largest.shard);
                    totalNativeUsed -= getNativeBytesUsed(largest.shard);
                    if (doThrottle && throttled.contains(largest.shard) == false) {
                        logger.info("now throttling indexing for shard [{}]: memory pressure", largest.shard.shardId());
                        throttled.add(largest.shard);
                        activateThrottling(largest.shard);
                    }
                }
            }

            if (doThrottle == false) {
                for (IndexShard shard : throttled) {
                    logger.info("stop throttling indexing for shard [{}]", shard.shardId());
                    deactivateThrottling(shard);
                }
                throttled.clear();
            }
        }
    }

    /**
     * ask this shard to check now whether it is inactive, and reduces its indexing buffer if so.
     */
    protected void checkIdle(IndexShard shard, long inactiveTimeNS) {
        try {
            shard.flushOnIdle(inactiveTimeNS);
        } catch (AlreadyClosedException e) {
            logger.trace(() -> new ParameterizedMessage("ignore exception while checking if shard {} is inactive", shard.shardId()), e);
        }
    }
}
