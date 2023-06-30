/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.monitor.fs;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.SingleObjectCache;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.store.remote.filecache.FileCache;

import java.io.IOException;
import java.util.function.Supplier;

/**
 * FileSystem service
 *
 * @opensearch.internal
 */
public class ProtobufFsService {

    private static final Logger logger = LogManager.getLogger(ProtobufFsService.class);

    private final Supplier<ProtobufFsInfo> fsInfoSupplier;

    public static final Setting<TimeValue> REFRESH_INTERVAL_SETTING = Setting.timeSetting(
        "monitor.fs.refresh_interval",
        TimeValue.timeValueSeconds(1),
        TimeValue.timeValueSeconds(1),
        Property.NodeScope
    );

    // permits tests to bypass the refresh interval on the cache; deliberately unregistered since it is only for use in tests
    public static final Setting<Boolean> ALWAYS_REFRESH_SETTING = Setting.boolSetting(
        "monitor.fs.always_refresh",
        false,
        Property.NodeScope
    );

    public ProtobufFsService(final Settings settings, final NodeEnvironment nodeEnvironment, FileCache fileCache) {
        final FsProbe probe = new FsProbe(nodeEnvironment, fileCache);
        final ProtobufFsInfo initialValue = stats(probe, null);
        if (ALWAYS_REFRESH_SETTING.get(settings)) {
            assert REFRESH_INTERVAL_SETTING.exists(settings) == false;
            logger.debug("bypassing refresh_interval");
            fsInfoSupplier = () -> stats(probe, initialValue);
        } else {
            final TimeValue refreshInterval = REFRESH_INTERVAL_SETTING.get(settings);
            logger.debug("using refresh_interval [{}]", refreshInterval);
            fsInfoSupplier = new FsInfoCache(refreshInterval, initialValue, probe)::getOrRefresh;
        }
    }

    public ProtobufFsInfo stats() {
        return fsInfoSupplier.get();
    }

    private static ProtobufFsInfo stats(FsProbe probe, ProtobufFsInfo initialValue) {
        try {
            return probe.protobufStats(initialValue);
        } catch (IOException e) {
            logger.debug("unexpected exception reading filesystem info", e);
            return null;
        }
    }

    private static class FsInfoCache extends SingleObjectCache<ProtobufFsInfo> {

        private final ProtobufFsInfo initialValue;
        private final FsProbe probe;

        FsInfoCache(TimeValue interval, ProtobufFsInfo initialValue, FsProbe probe) {
            super(interval, initialValue);
            this.initialValue = initialValue;
            this.probe = probe;
        }

        @Override
        protected ProtobufFsInfo refresh() {
            return stats(probe, initialValue);
        }

    }

}
