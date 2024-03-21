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

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.IndexSettings;

/**
 * Settings for remote store
 *
 * @opensearch.api
 */
@PublicApi(since = "2.14.0")
public class RemoteStoreSettings {

    /**
     * Used to specify the default translog buffer interval for remote store backed indexes.
     */
    public static final Setting<TimeValue> CLUSTER_REMOTE_TRANSLOG_BUFFER_INTERVAL_SETTING = Setting.timeSetting(
        "cluster.remote_store.translog.buffer_interval",
        IndexSettings.DEFAULT_REMOTE_TRANSLOG_BUFFER_INTERVAL,
        IndexSettings.MINIMUM_REMOTE_TRANSLOG_BUFFER_INTERVAL,
        Property.NodeScope,
        Property.Dynamic
    );

    /**
     * Controls minimum number of metadata files to keep in remote segment store.
     * {@code value < 1} will disable deletion of stale segment metadata files.
     */
    public static final Setting<Integer> CLUSTER_REMOTE_INDEX_SEGMENT_METADATA_RETENTION_MAX_COUNT_SETTING = Setting.intSetting(
        "cluster.remote_store.index.segment_metadata.retention.max_count",
        10,
        -1,
        v -> {
            if (v == 0) {
                throw new IllegalArgumentException(
                    "Value 0 is not allowed for this setting as it would delete all the data from remote segment store"
                );
            }
        },
        Property.NodeScope,
        Property.Dynamic
    );

    private volatile TimeValue clusterRemoteTranslogBufferInterval;
    private volatile int minRemoteSegmentMetadataFiles;

    public RemoteStoreSettings(Settings settings, ClusterSettings clusterSettings) {
        this.clusterRemoteTranslogBufferInterval = CLUSTER_REMOTE_TRANSLOG_BUFFER_INTERVAL_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(
            CLUSTER_REMOTE_TRANSLOG_BUFFER_INTERVAL_SETTING,
            this::setClusterRemoteTranslogBufferInterval
        );

        minRemoteSegmentMetadataFiles = CLUSTER_REMOTE_INDEX_SEGMENT_METADATA_RETENTION_MAX_COUNT_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(
            CLUSTER_REMOTE_INDEX_SEGMENT_METADATA_RETENTION_MAX_COUNT_SETTING,
            this::setMinRemoteSegmentMetadataFiles
        );
    }

    // Exclusively for testing, please do not use it elsewhere.
    public TimeValue getClusterRemoteTranslogBufferInterval() {
        return clusterRemoteTranslogBufferInterval;
    }

    private void setClusterRemoteTranslogBufferInterval(TimeValue clusterRemoteTranslogBufferInterval) {
        this.clusterRemoteTranslogBufferInterval = clusterRemoteTranslogBufferInterval;
    }

    private void setMinRemoteSegmentMetadataFiles(int minRemoteSegmentMetadataFiles) {
        this.minRemoteSegmentMetadataFiles = minRemoteSegmentMetadataFiles;
    }

    public int getMinRemoteSegmentMetadataFiles() {
        return this.minRemoteSegmentMetadataFiles;
    }
}
