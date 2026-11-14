/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.search;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.threadpool.ThreadPool;

import java.util.List;

/**
 * Settings of Arrow streaming search, and their current values on this node.
 */
public final class ArrowSearchSettings {

    /**
     * The size batches grow to. Its maximum keeps batches below gRPC's default 4MB client message limit. It also bounds the
     * memory a shard stream holds while the client does not read.
     */
    public static final Setting<ByteSizeValue> BATCH_SIZE = Setting.byteSizeSetting(
        "search.arrow.batch_size",
        new ByteSizeValue(1, ByteSizeUnit.MB),
        new ByteSizeValue(16, ByteSizeUnit.KB),
        new ByteSizeValue(3, ByteSizeUnit.MB),
        Property.NodeScope,
        Property.Dynamic
    );

    /** The size of the first batch of each slice of a shard scan. Later batches double up to the batch size, so rows arrive early. */
    public static final Setting<ByteSizeValue> FIRST_BATCH_SIZE = Setting.byteSizeSetting(
        "search.arrow.first_batch_size",
        new ByteSizeValue(64, ByteSizeUnit.KB),
        new ByteSizeValue(16, ByteSizeUnit.KB),
        new ByteSizeValue(3, ByteSizeUnit.MB),
        Property.NodeScope,
        Property.Dynamic
    );

    /** The maximum number of rows in a batch. */
    public static final Setting<Integer> BATCH_ROWS = Setting.intSetting(
        "search.arrow.batch_rows",
        64 * 1024,
        1,
        1024 * 1024,
        Property.NodeScope,
        Property.Dynamic
    );

    /**
     * The largest row a batch may hold. A larger row fails the search; rows up to the batch size are always accepted, so the
     * limit is the larger of the two. The coordinator sizes each shard stream for batches of this size, so raising it above
     * the batch size lowers how many shard streams a search opens at once.
     */
    public static final Setting<ByteSizeValue> MAX_ROW_SIZE = Setting.byteSizeSetting(
        "search.arrow.max_row_size",
        new ByteSizeValue(1, ByteSizeUnit.MB),
        new ByteSizeValue(16, ByteSizeUnit.KB),
        new ByteSizeValue(64, ByteSizeUnit.MB),
        Property.NodeScope,
        Property.Dynamic
    );

    /**
     * The batch memory one search may hold on its coordinator. It bounds how many shard streams the search opens at once.
     * Each open shard stream counts as the six batches it may have in flight, and the search counts one more batch that gRPC
     * holds for the client. A batch counts as the larger of the batch size and the maximum row size, rounded up as Arrow
     * allocates it.
     */
    public static final Setting<ByteSizeValue> MAX_BUFFERED_BYTES_PER_REQUEST = Setting.byteSizeSetting(
        "search.arrow.max_buffered_bytes_per_request",
        new ByteSizeValue(64, ByteSizeUnit.MB),
        new ByteSizeValue(1, ByteSizeUnit.MB),
        new ByteSizeValue(Long.MAX_VALUE),
        Property.NodeScope,
        Property.Dynamic
    );

    /** The batch memory all searches may hold on a coordinator. A search that does not fit is rejected. */
    public static final Setting<ByteSizeValue> COORDINATOR_MAX_BUFFERED_BYTES = Setting.byteSizeSetting(
        "search.arrow.coordinator.max_buffered_bytes",
        new ByteSizeValue(512, ByteSizeUnit.MB),
        new ByteSizeValue(1, ByteSizeUnit.MB),
        new ByteSizeValue(Long.MAX_VALUE),
        Property.NodeScope,
        Property.Dynamic
    );

    /**
     * How long a client may stop reading. While the search runs, a longer pause fails the search and releases everything it
     * holds. After the search ends, a longer pause resets the call and releases the batches the client has not read. The
     * minimum is one second, since a call is briefly not ready after every batch it queues.
     */
    public static final Setting<TimeValue> CLIENT_IDLE_TIMEOUT = Setting.timeSetting(
        "search.arrow.client_idle_timeout",
        TimeValue.timeValueSeconds(60),
        TimeValue.timeValueSeconds(1),
        Property.NodeScope,
        Property.Dynamic
    );

    /**
     * How long an Arrow search may run before it is cancelled, unless the request sets its own interval. Unlike
     * {@code search.cancel_after_time_interval}, which targets interactive searches, it is off by default, since an export
     * runs as long as its client reads.
     */
    public static final Setting<TimeValue> CANCEL_AFTER_TIME_INTERVAL = Setting.timeSetting(
        "search.arrow.cancel_after_time_interval",
        TimeValue.MINUS_ONE,
        TimeValue.MINUS_ONE,
        Property.NodeScope,
        Property.Dynamic
    );

    /** How many shard scans may be open on a data node at once. */
    public static final Setting<Integer> SHARD_MAX_OPEN_SCANS = new Setting<>(
        "search.arrow.shard.max_open_scans",
        settings -> Integer.toString(4 * OpenSearchExecutors.allocatedProcessors(settings)),
        value -> Setting.parseInt(value, 1, "search.arrow.shard.max_open_scans"),
        Property.NodeScope,
        Property.Dynamic
    );

    /**
     * How many compute slots a data node has, that is, how many slices of shard scans may compute at once. A slice computes
     * on a virtual thread and holds no compute slot while it waits for its consumer.
     */
    public static final Setting<Integer> SHARD_MAX_CONCURRENT_BATCHES = new Setting<>(
        "search.arrow.shard.max_concurrent_batches",
        settings -> Integer.toString(Math.max(1, ThreadPool.searchThreadPoolSize(OpenSearchExecutors.allocatedProcessors(settings)) / 2)),
        value -> Setting.parseInt(value, 1, "search.arrow.shard.max_concurrent_batches"),
        Property.NodeScope,
        Property.Dynamic
    );

    /** How long a new shard scan waits for an open scan slot. A scan that gets none is rejected and retried on another copy. */
    public static final Setting<TimeValue> SHARD_ADMISSION_WAIT = Setting.timeSetting(
        "search.arrow.shard.admission_wait",
        TimeValue.timeValueSeconds(5),
        TimeValue.ZERO,
        Property.NodeScope,
        Property.Dynamic
    );

    /** The batch memory all shard scans may hold on a data node. */
    public static final Setting<ByteSizeValue> SHARD_MAX_BUFFERED_BYTES = Setting.byteSizeSetting(
        "search.arrow.shard.max_buffered_bytes",
        new ByteSizeValue(512, ByteSizeUnit.MB),
        new ByteSizeValue(1, ByteSizeUnit.MB),
        new ByteSizeValue(Long.MAX_VALUE),
        Property.NodeScope
    );

    /**
     * Returns every setting of the plugin.
     */
    public static List<Setting<?>> all() {
        return List.of(
            BATCH_SIZE,
            FIRST_BATCH_SIZE,
            BATCH_ROWS,
            MAX_ROW_SIZE,
            MAX_BUFFERED_BYTES_PER_REQUEST,
            COORDINATOR_MAX_BUFFERED_BYTES,
            CLIENT_IDLE_TIMEOUT,
            CANCEL_AFTER_TIME_INTERVAL,
            SHARD_MAX_OPEN_SCANS,
            SHARD_MAX_CONCURRENT_BATCHES,
            SHARD_ADMISSION_WAIT,
            SHARD_MAX_BUFFERED_BYTES
        );
    }

    private volatile long batchBytes;
    private volatile long firstBatchBytes;
    private volatile int batchRows;
    private volatile long maxRowBytes;
    private volatile long maxBufferedBytesPerRequest;
    private volatile long coordinatorMaxBufferedBytes;
    private volatile TimeValue clientIdleTimeout;
    private volatile TimeValue cancelAfterTimeInterval;
    private volatile TimeValue admissionWait;

    /**
     * Reads the current values and follows updates of the dynamic settings.
     *
     * @param settings        the node settings
     * @param clusterSettings the cluster settings, which update the dynamic values
     */
    public ArrowSearchSettings(Settings settings, ClusterSettings clusterSettings) {
        batchBytes = BATCH_SIZE.get(settings).getBytes();
        firstBatchBytes = FIRST_BATCH_SIZE.get(settings).getBytes();
        batchRows = BATCH_ROWS.get(settings);
        maxRowBytes = MAX_ROW_SIZE.get(settings).getBytes();
        maxBufferedBytesPerRequest = MAX_BUFFERED_BYTES_PER_REQUEST.get(settings).getBytes();
        coordinatorMaxBufferedBytes = COORDINATOR_MAX_BUFFERED_BYTES.get(settings).getBytes();
        clientIdleTimeout = CLIENT_IDLE_TIMEOUT.get(settings);
        cancelAfterTimeInterval = CANCEL_AFTER_TIME_INTERVAL.get(settings);
        admissionWait = SHARD_ADMISSION_WAIT.get(settings);
        clusterSettings.addSettingsUpdateConsumer(BATCH_SIZE, value -> batchBytes = value.getBytes());
        clusterSettings.addSettingsUpdateConsumer(FIRST_BATCH_SIZE, value -> firstBatchBytes = value.getBytes());
        clusterSettings.addSettingsUpdateConsumer(BATCH_ROWS, value -> batchRows = value);
        clusterSettings.addSettingsUpdateConsumer(MAX_ROW_SIZE, value -> maxRowBytes = value.getBytes());
        clusterSettings.addSettingsUpdateConsumer(MAX_BUFFERED_BYTES_PER_REQUEST, value -> maxBufferedBytesPerRequest = value.getBytes());
        clusterSettings.addSettingsUpdateConsumer(COORDINATOR_MAX_BUFFERED_BYTES, value -> coordinatorMaxBufferedBytes = value.getBytes());
        clusterSettings.addSettingsUpdateConsumer(CLIENT_IDLE_TIMEOUT, value -> clientIdleTimeout = value);
        clusterSettings.addSettingsUpdateConsumer(CANCEL_AFTER_TIME_INTERVAL, value -> cancelAfterTimeInterval = value);
        clusterSettings.addSettingsUpdateConsumer(SHARD_ADMISSION_WAIT, value -> admissionWait = value);
    }

    /** Returns the size batches grow to, in bytes. */
    public long batchBytes() {
        return batchBytes;
    }

    /** Returns the size of the first batch of a slice, in bytes, at most the batch size. */
    public long firstBatchBytes() {
        return Math.min(firstBatchBytes, batchBytes);
    }

    /** Returns the maximum number of rows in a batch. */
    public int batchRows() {
        return batchRows;
    }

    /** Returns the largest row a batch may hold, in bytes. */
    public long maxRowBytes() {
        return maxRowBytes;
    }

    /** Returns the batch memory one search may hold on its coordinator, in bytes. */
    public long maxBufferedBytesPerRequest() {
        return maxBufferedBytesPerRequest;
    }

    /** Returns the batch memory all searches may hold on this coordinator, in bytes. */
    public long coordinatorMaxBufferedBytes() {
        return coordinatorMaxBufferedBytes;
    }

    /** Returns how long a client may stop reading. */
    public TimeValue clientIdleTimeout() {
        return clientIdleTimeout;
    }

    /** Returns how long a search may run before it is cancelled, or minus one for no limit. */
    public TimeValue cancelAfterTimeInterval() {
        return cancelAfterTimeInterval;
    }

    /** Returns how long a new shard scan waits for an open scan slot. */
    public TimeValue admissionWait() {
        return admissionWait;
    }

}
