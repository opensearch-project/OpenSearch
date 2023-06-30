/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.monitor.os;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.SingleObjectCache;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.node.ProtobufReportingService;

import java.io.IOException;

/**
 * Service for the Operating System
 *
 * @opensearch.internal
 */
public class ProtobufOsService implements ProtobufReportingService<ProtobufOsInfo> {

    private static final Logger logger = LogManager.getLogger(ProtobufOsService.class);

    private final OsProbe probe;
    private final ProtobufOsInfo info;
    private final SingleObjectCache<ProtobufOsStats> osStatsCache;

    public static final Setting<TimeValue> REFRESH_INTERVAL_SETTING = Setting.timeSetting(
        "monitor.os.refresh_interval",
        TimeValue.timeValueSeconds(1),
        TimeValue.timeValueSeconds(1),
        Property.NodeScope
    );

    public ProtobufOsService(Settings settings) throws IOException {
        this.probe = OsProbe.getInstance();
        TimeValue refreshInterval = REFRESH_INTERVAL_SETTING.get(settings);
        this.info = probe.protobufOsInfo(refreshInterval.millis(), OpenSearchExecutors.allocatedProcessors(settings));
        this.osStatsCache = new OsStatsCache(refreshInterval, probe.protobufOsStats());
        logger.debug("using refresh_interval [{}]", refreshInterval);
    }

    @Override
    public ProtobufOsInfo protobufInfo() {
        return this.info;
    }

    public synchronized ProtobufOsStats stats() {
        return osStatsCache.getOrRefresh();
    }

    private class OsStatsCache extends SingleObjectCache<ProtobufOsStats> {
        OsStatsCache(TimeValue interval, ProtobufOsStats initValue) {
            super(interval, initValue);
        }

        @Override
        protected ProtobufOsStats refresh() {
            return probe.protobufOsStats();
        }
    }
}
