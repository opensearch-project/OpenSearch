/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.monitor.jvm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.node.ProtobufReportingService;

/**
 * Service for monitoring the JVM
 *
 * @opensearch.internal
 */
public class ProtobufJvmService implements ProtobufReportingService<JvmInfo> {

    private static final Logger logger = LogManager.getLogger(ProtobufJvmService.class);

    private final JvmInfo jvmInfo;

    private final TimeValue refreshInterval;

    private ProtobufJvmStats jvmStats;

    public static final Setting<TimeValue> REFRESH_INTERVAL_SETTING = Setting.timeSetting(
        "monitor.jvm.refresh_interval",
        TimeValue.timeValueSeconds(1),
        TimeValue.timeValueSeconds(1),
        Property.NodeScope
    );

    public ProtobufJvmService(Settings settings) {
        this.jvmInfo = JvmInfo.jvmInfo();
        this.jvmStats = ProtobufJvmStats.jvmStats();

        this.refreshInterval = REFRESH_INTERVAL_SETTING.get(settings);

        logger.debug("using refresh_interval [{}]", refreshInterval);
    }

    @Override
    public JvmInfo protobufInfo() {
        return this.jvmInfo;
    }

    public synchronized ProtobufJvmStats stats() {
        if ((System.currentTimeMillis() - jvmStats.getTimestamp()) > refreshInterval.millis()) {
            jvmStats = ProtobufJvmStats.jvmStats();
        }
        return jvmStats;
    }
}
