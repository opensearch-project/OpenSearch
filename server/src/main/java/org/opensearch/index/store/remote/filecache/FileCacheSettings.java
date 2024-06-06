/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.filecache;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;

/**
 * Settings relate to file cache
 *
 * @opensearch.internal
 */
public class FileCacheSettings {
    /**
     * Defines a limit of how much total remote data can be referenced as a ratio of the size of the disk reserved for
     * the file cache. For example, if 100GB disk space is configured for use as a file cache and the
     * remote_data_ratio of 5 is defined, then a total of 500GB of remote data can be loaded as searchable snapshots.
     * This is designed to be a safeguard to prevent oversubscribing a cluster.
     * Specify a value of zero for no limit, which is the default for compatibility reasons.
     */
    public static final Setting<Double> DATA_TO_FILE_CACHE_SIZE_RATIO_SETTING = Setting.doubleSetting(
        "cluster.filecache.remote_data_ratio",
        0.0,
        0.0,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private volatile double remoteDataRatio;

    public FileCacheSettings(Settings settings, ClusterSettings clusterSettings) {
        setRemoteDataRatio(DATA_TO_FILE_CACHE_SIZE_RATIO_SETTING.get(settings));
        clusterSettings.addSettingsUpdateConsumer(DATA_TO_FILE_CACHE_SIZE_RATIO_SETTING, this::setRemoteDataRatio);
    }

    public void setRemoteDataRatio(double remoteDataRatio) {
        this.remoteDataRatio = remoteDataRatio;
    }

    public double getRemoteDataRatio() {
        return remoteDataRatio;
    }
}
