/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.monitor.process;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.SingleObjectCache;
import org.opensearch.node.ProtobufReportingService;

/**
 * The service for the process
 *
 * @opensearch.internal
 */
public final class ProtobufProcessService implements ProtobufReportingService<ProtobufProcessInfo> {

    private static final Logger logger = LogManager.getLogger(ProtobufProcessService.class);

    private final ProcessProbe probe;
    private final ProtobufProcessInfo info;
    private final SingleObjectCache<ProtobufProcessStats> processStatsCache;

    public static final Setting<TimeValue> REFRESH_INTERVAL_SETTING = Setting.timeSetting(
        "monitor.process.refresh_interval",
        TimeValue.timeValueSeconds(1),
        TimeValue.timeValueSeconds(1),
        Property.NodeScope
    );

    public ProtobufProcessService(Settings settings) {
        this.probe = ProcessProbe.getInstance();
        final TimeValue refreshInterval = REFRESH_INTERVAL_SETTING.get(settings);
        processStatsCache = new ProcessStatsCache(refreshInterval, probe.processProtobufStats());
        this.info = probe.protobufProcessInfo(refreshInterval.millis());
        logger.debug("using refresh_interval [{}]", refreshInterval);
    }

    @Override
    public ProtobufProcessInfo protobufInfo() {
        return this.info;
    }

    public ProtobufProcessStats stats() {
        return processStatsCache.getOrRefresh();
    }

    private class ProcessStatsCache extends SingleObjectCache<ProtobufProcessStats> {
        ProcessStatsCache(TimeValue interval, ProtobufProcessStats initValue) {
            super(interval, initValue);
        }

        @Override
        protected ProtobufProcessStats refresh() {
            return probe.processProtobufStats();
        }
    }
}
