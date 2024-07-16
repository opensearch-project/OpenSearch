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

package org.opensearch.indices.recovery;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.RateLimiter;
import org.apache.lucene.store.RateLimiter.SimpleRateLimiter;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;

import java.util.concurrent.TimeUnit;

/**
 * Settings for the recovery mechanism
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class RecoverySettings {

    private static final Logger logger = LogManager.getLogger(RecoverySettings.class);

    public static final Setting<ByteSizeValue> INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING = Setting.byteSizeSetting(
        "indices.recovery.max_bytes_per_sec",
        new ByteSizeValue(40, ByteSizeUnit.MB),
        Property.Dynamic,
        Property.NodeScope
    );

    /**
     * Individual speed setting for segment replication, default -1B to reuse the setting of recovery.
     */
    public static final Setting<ByteSizeValue> INDICES_REPLICATION_MAX_BYTES_PER_SEC_SETTING = Setting.byteSizeSetting(
        "indices.replication.max_bytes_per_sec",
        new ByteSizeValue(-1),
        Property.Dynamic,
        Property.NodeScope
    );

    /**
     * Controls the maximum number of file chunk requests that can be sent concurrently from the source node to the target node.
     */
    public static final Setting<Integer> INDICES_RECOVERY_MAX_CONCURRENT_FILE_CHUNKS_SETTING = Setting.intSetting(
        "indices.recovery.max_concurrent_file_chunks",
        2,
        1,
        5,
        Property.Dynamic,
        Property.NodeScope
    );

    /**
     * Controls the maximum number of operation chunk requests that can be sent concurrently from the source node to the target node.
     */
    public static final Setting<Integer> INDICES_RECOVERY_MAX_CONCURRENT_OPERATIONS_SETTING = Setting.intSetting(
        "indices.recovery.max_concurrent_operations",
        1,
        1,
        4,
        Property.Dynamic,
        Property.NodeScope
    );

    /**
     * Controls the maximum number of streams that can be started concurrently per recovery when downloading from the remote store.
     */
    public static final Setting<Integer> INDICES_RECOVERY_MAX_CONCURRENT_REMOTE_STORE_STREAMS_SETTING = new Setting<>(
        "indices.recovery.max_concurrent_remote_store_streams",
        (s) -> Integer.toString(Math.max(1, OpenSearchExecutors.allocatedProcessors(s) / 2)),
        (s) -> Setting.parseInt(s, 1, "indices.recovery.max_concurrent_remote_store_streams"),
        Property.Dynamic,
        Property.NodeScope
    );

    /**
     * how long to wait before retrying after issues cause by cluster state syncing between nodes
     * i.e., local node is not yet known on remote node, remote shard not yet started etc.
     */
    public static final Setting<TimeValue> INDICES_RECOVERY_RETRY_DELAY_STATE_SYNC_SETTING = Setting.positiveTimeSetting(
        "indices.recovery.retry_delay_state_sync",
        TimeValue.timeValueMillis(500),
        Property.Dynamic,
        Property.NodeScope
    );

    /** how long to wait before retrying after network related issues */
    public static final Setting<TimeValue> INDICES_RECOVERY_RETRY_DELAY_NETWORK_SETTING = Setting.positiveTimeSetting(
        "indices.recovery.retry_delay_network",
        TimeValue.timeValueSeconds(5),
        Property.Dynamic,
        Property.NodeScope
    );

    /** timeout value to use for requests made as part of the recovery process */
    public static final Setting<TimeValue> INDICES_RECOVERY_INTERNAL_ACTION_TIMEOUT_SETTING = Setting.positiveTimeSetting(
        "indices.recovery.internal_action_timeout",
        TimeValue.timeValueMinutes(15),
        Property.Dynamic,
        Property.NodeScope
    );

    /** timeout value to use for the retrying of requests made as part of the recovery process */
    public static final Setting<TimeValue> INDICES_RECOVERY_INTERNAL_ACTION_RETRY_TIMEOUT_SETTING = Setting.positiveTimeSetting(
        "indices.recovery.internal_action_retry_timeout",
        TimeValue.timeValueMinutes(1),
        Property.Dynamic,
        Property.NodeScope
    );

    /**
     * timeout value to use for requests made as part of the recovery process that are expected to take long time.
     * defaults to twice `indices.recovery.internal_action_timeout`.
     */
    public static final Setting<TimeValue> INDICES_RECOVERY_INTERNAL_LONG_ACTION_TIMEOUT_SETTING = Setting.timeSetting(
        "indices.recovery.internal_action_long_timeout",
        (s) -> TimeValue.timeValueMillis(INDICES_RECOVERY_INTERNAL_ACTION_TIMEOUT_SETTING.get(s).millis() * 2),
        TimeValue.timeValueSeconds(0),
        Property.Dynamic,
        Property.NodeScope
    );

    /**
     * recoveries that don't show any activity for more then this interval will be failed.
     * defaults to `indices.recovery.internal_action_long_timeout`
     */
    public static final Setting<TimeValue> INDICES_RECOVERY_ACTIVITY_TIMEOUT_SETTING = Setting.timeSetting(
        "indices.recovery.recovery_activity_timeout",
        INDICES_RECOVERY_INTERNAL_LONG_ACTION_TIMEOUT_SETTING::get,
        TimeValue.timeValueSeconds(0),
        Property.Dynamic,
        Property.NodeScope
    );

    public static final Setting<TimeValue> INDICES_INTERNAL_REMOTE_UPLOAD_TIMEOUT = Setting.timeSetting(
        "indices.recovery.internal_remote_upload_timeout",
        new TimeValue(1, TimeUnit.HOURS),
        Property.Dynamic,
        Property.NodeScope
    );

    // choose 512KB-16B to ensure that the resulting byte[] is not a humongous allocation in G1.
    public static final ByteSizeValue DEFAULT_CHUNK_SIZE = new ByteSizeValue(512 * 1024 - 16, ByteSizeUnit.BYTES);

    public static final Setting<ByteSizeValue> INDICES_RECOVERY_CHUNK_SIZE_SETTING = Setting.byteSizeSetting(
        "indices.recovery.chunk_size",
        DEFAULT_CHUNK_SIZE,
        new ByteSizeValue(1, ByteSizeUnit.BYTES),
        new ByteSizeValue(100, ByteSizeUnit.MB),
        Property.Dynamic,
        Property.NodeScope
    );

    private volatile ByteSizeValue recoveryMaxBytesPerSec;
    private volatile ByteSizeValue replicationMaxBytesPerSec;
    private volatile int maxConcurrentFileChunks;
    private volatile int maxConcurrentOperations;
    private volatile int maxConcurrentRemoteStoreStreams;
    private volatile SimpleRateLimiter recoveryRateLimiter;
    private volatile SimpleRateLimiter replicationRateLimiter;
    private volatile TimeValue retryDelayStateSync;
    private volatile TimeValue retryDelayNetwork;
    private volatile TimeValue activityTimeout;
    private volatile TimeValue internalActionTimeout;
    private volatile TimeValue internalActionRetryTimeout;
    private volatile TimeValue internalActionLongTimeout;

    private volatile ByteSizeValue chunkSize;
    private volatile TimeValue internalRemoteUploadTimeout;

    public RecoverySettings(Settings settings, ClusterSettings clusterSettings) {
        this.retryDelayStateSync = INDICES_RECOVERY_RETRY_DELAY_STATE_SYNC_SETTING.get(settings);
        this.maxConcurrentFileChunks = INDICES_RECOVERY_MAX_CONCURRENT_FILE_CHUNKS_SETTING.get(settings);
        this.maxConcurrentOperations = INDICES_RECOVERY_MAX_CONCURRENT_OPERATIONS_SETTING.get(settings);
        this.maxConcurrentRemoteStoreStreams = INDICES_RECOVERY_MAX_CONCURRENT_REMOTE_STORE_STREAMS_SETTING.get(settings);
        // doesn't have to be fast as nodes are reconnected every 10s by default (see InternalClusterService.ReconnectToNodes)
        // and we want to give the cluster-manager time to remove a faulty node
        this.retryDelayNetwork = INDICES_RECOVERY_RETRY_DELAY_NETWORK_SETTING.get(settings);

        this.internalActionTimeout = INDICES_RECOVERY_INTERNAL_ACTION_TIMEOUT_SETTING.get(settings);
        this.internalActionRetryTimeout = INDICES_RECOVERY_INTERNAL_ACTION_RETRY_TIMEOUT_SETTING.get(settings);
        this.internalActionLongTimeout = INDICES_RECOVERY_INTERNAL_LONG_ACTION_TIMEOUT_SETTING.get(settings);

        this.activityTimeout = INDICES_RECOVERY_ACTIVITY_TIMEOUT_SETTING.get(settings);
        this.recoveryMaxBytesPerSec = INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.get(settings);
        if (recoveryMaxBytesPerSec.getBytes() <= 0) {
            recoveryRateLimiter = null;
        } else {
            recoveryRateLimiter = new SimpleRateLimiter(recoveryMaxBytesPerSec.getMbFrac());
        }
        this.replicationMaxBytesPerSec = INDICES_REPLICATION_MAX_BYTES_PER_SEC_SETTING.get(settings);
        updateReplicationRateLimiter();

        logger.debug("using recovery max_bytes_per_sec[{}]", recoveryMaxBytesPerSec);
        this.internalRemoteUploadTimeout = INDICES_INTERNAL_REMOTE_UPLOAD_TIMEOUT.get(settings);
        this.chunkSize = INDICES_RECOVERY_CHUNK_SIZE_SETTING.get(settings);

        clusterSettings.addSettingsUpdateConsumer(INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING, this::setRecoveryMaxBytesPerSec);
        clusterSettings.addSettingsUpdateConsumer(INDICES_REPLICATION_MAX_BYTES_PER_SEC_SETTING, this::setReplicationMaxBytesPerSec);
        clusterSettings.addSettingsUpdateConsumer(INDICES_RECOVERY_MAX_CONCURRENT_FILE_CHUNKS_SETTING, this::setMaxConcurrentFileChunks);
        clusterSettings.addSettingsUpdateConsumer(INDICES_RECOVERY_MAX_CONCURRENT_OPERATIONS_SETTING, this::setMaxConcurrentOperations);
        clusterSettings.addSettingsUpdateConsumer(
            INDICES_RECOVERY_MAX_CONCURRENT_REMOTE_STORE_STREAMS_SETTING,
            this::setMaxConcurrentRemoteStoreStreams
        );
        clusterSettings.addSettingsUpdateConsumer(INDICES_RECOVERY_RETRY_DELAY_STATE_SYNC_SETTING, this::setRetryDelayStateSync);
        clusterSettings.addSettingsUpdateConsumer(INDICES_RECOVERY_RETRY_DELAY_NETWORK_SETTING, this::setRetryDelayNetwork);
        clusterSettings.addSettingsUpdateConsumer(INDICES_RECOVERY_INTERNAL_ACTION_TIMEOUT_SETTING, this::setInternalActionTimeout);
        clusterSettings.addSettingsUpdateConsumer(
            INDICES_RECOVERY_INTERNAL_LONG_ACTION_TIMEOUT_SETTING,
            this::setInternalActionLongTimeout
        );
        clusterSettings.addSettingsUpdateConsumer(INDICES_RECOVERY_ACTIVITY_TIMEOUT_SETTING, this::setActivityTimeout);
        clusterSettings.addSettingsUpdateConsumer(INDICES_INTERNAL_REMOTE_UPLOAD_TIMEOUT, this::setInternalRemoteUploadTimeout);
        clusterSettings.addSettingsUpdateConsumer(INDICES_RECOVERY_CHUNK_SIZE_SETTING, this::setChunkSize);
        clusterSettings.addSettingsUpdateConsumer(
            INDICES_RECOVERY_INTERNAL_ACTION_RETRY_TIMEOUT_SETTING,
            this::setInternalActionRetryTimeout
        );
    }

    public RateLimiter recoveryRateLimiter() {
        return recoveryRateLimiter;
    }

    public RateLimiter replicationRateLimiter() {
        return replicationRateLimiter;
    }

    public TimeValue retryDelayNetwork() {
        return retryDelayNetwork;
    }

    public TimeValue retryDelayStateSync() {
        return retryDelayStateSync;
    }

    public TimeValue activityTimeout() {
        return activityTimeout;
    }

    public TimeValue internalActionTimeout() {
        return internalActionTimeout;
    }

    public TimeValue internalActionRetryTimeout() {
        return internalActionRetryTimeout;
    }

    public TimeValue internalActionLongTimeout() {
        return internalActionLongTimeout;
    }

    public TimeValue internalRemoteUploadTimeout() {
        return internalRemoteUploadTimeout;
    }

    public ByteSizeValue getChunkSize() {
        return chunkSize;
    }

    public void setChunkSize(ByteSizeValue chunkSize) {
        this.chunkSize = chunkSize;
    }

    public void setRetryDelayStateSync(TimeValue retryDelayStateSync) {
        this.retryDelayStateSync = retryDelayStateSync;
    }

    public void setRetryDelayNetwork(TimeValue retryDelayNetwork) {
        this.retryDelayNetwork = retryDelayNetwork;
    }

    public void setActivityTimeout(TimeValue activityTimeout) {
        this.activityTimeout = activityTimeout;
    }

    public void setInternalActionTimeout(TimeValue internalActionTimeout) {
        this.internalActionTimeout = internalActionTimeout;
    }

    public void setInternalActionLongTimeout(TimeValue internalActionLongTimeout) {
        this.internalActionLongTimeout = internalActionLongTimeout;
    }

    public void setInternalRemoteUploadTimeout(TimeValue internalRemoteUploadTimeout) {
        this.internalRemoteUploadTimeout = internalRemoteUploadTimeout;
    }

    public void setInternalActionRetryTimeout(TimeValue internalActionRetryTimeout) {
        this.internalActionRetryTimeout = internalActionRetryTimeout;
    }

    private void setRecoveryMaxBytesPerSec(ByteSizeValue recoveryMaxBytesPerSec) {
        this.recoveryMaxBytesPerSec = recoveryMaxBytesPerSec;
        if (recoveryMaxBytesPerSec.getBytes() <= 0) {
            recoveryRateLimiter = null;
        } else if (recoveryRateLimiter != null) {
            recoveryRateLimiter.setMBPerSec(recoveryMaxBytesPerSec.getMbFrac());
        } else {
            recoveryRateLimiter = new SimpleRateLimiter(recoveryMaxBytesPerSec.getMbFrac());
        }
        if (replicationMaxBytesPerSec.getBytes() < 0) updateReplicationRateLimiter();
    }

    private void setReplicationMaxBytesPerSec(ByteSizeValue replicationMaxBytesPerSec) {
        this.replicationMaxBytesPerSec = replicationMaxBytesPerSec;
        updateReplicationRateLimiter();
    }

    private void updateReplicationRateLimiter() {
        if (replicationMaxBytesPerSec.getBytes() >= 0) {
            if (replicationMaxBytesPerSec.getBytes() == 0) {
                replicationRateLimiter = null;
            } else if (replicationRateLimiter != null) {
                replicationRateLimiter.setMBPerSec(replicationMaxBytesPerSec.getMbFrac());
            } else {
                replicationRateLimiter = new SimpleRateLimiter(replicationMaxBytesPerSec.getMbFrac());
            }
        } else { // when replicationMaxBytesPerSec = -1B, use setting of recovery
            if (recoveryMaxBytesPerSec.getBytes() <= 0) {
                replicationRateLimiter = null;
            } else if (replicationRateLimiter != null) {
                replicationRateLimiter.setMBPerSec(recoveryMaxBytesPerSec.getMbFrac());
            } else {
                replicationRateLimiter = new SimpleRateLimiter(recoveryMaxBytesPerSec.getMbFrac());
            }
        }
    }

    public int getMaxConcurrentFileChunks() {
        return maxConcurrentFileChunks;
    }

    private void setMaxConcurrentFileChunks(int maxConcurrentFileChunks) {
        this.maxConcurrentFileChunks = maxConcurrentFileChunks;
    }

    public int getMaxConcurrentOperations() {
        return maxConcurrentOperations;
    }

    private void setMaxConcurrentOperations(int maxConcurrentOperations) {
        this.maxConcurrentOperations = maxConcurrentOperations;
    }

    public int getMaxConcurrentRemoteStoreStreams() {
        return this.maxConcurrentRemoteStoreStreams;
    }

    private void setMaxConcurrentRemoteStoreStreams(int maxConcurrentRemoteStoreStreams) {
        this.maxConcurrentRemoteStoreStreams = maxConcurrentRemoteStoreStreams;
    }

}
