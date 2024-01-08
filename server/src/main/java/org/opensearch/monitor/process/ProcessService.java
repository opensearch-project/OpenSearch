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

package org.opensearch.monitor.process;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.SingleObjectCache;
import org.opensearch.core.service.ReportingService;

/**
 * The service for the process
 *
 * @opensearch.internal
 */
public final class ProcessService implements ReportingService<ProcessInfo> {

    private static final Logger logger = LogManager.getLogger(ProcessService.class);

    private final ProcessProbe probe;
    private final ProcessInfo info;
    private final SingleObjectCache<ProcessStats> processStatsCache;

    public static final Setting<TimeValue> REFRESH_INTERVAL_SETTING = Setting.timeSetting(
        "monitor.process.refresh_interval",
        TimeValue.timeValueSeconds(1),
        TimeValue.timeValueSeconds(1),
        Property.NodeScope
    );

    public ProcessService(Settings settings) {
        this.probe = ProcessProbe.getInstance();
        final TimeValue refreshInterval = REFRESH_INTERVAL_SETTING.get(settings);
        processStatsCache = new ProcessStatsCache(refreshInterval, probe.processStats());
        this.info = probe.processInfo(refreshInterval.millis());
        logger.debug("using refresh_interval [{}]", refreshInterval);
    }

    @Override
    public ProcessInfo info() {
        return this.info;
    }

    public ProcessStats stats() {
        return processStatsCache.getOrRefresh();
    }

    private class ProcessStatsCache extends SingleObjectCache<ProcessStats> {
        ProcessStatsCache(TimeValue interval, ProcessStats initValue) {
            super(interval, initValue);
        }

        @Override
        protected ProcessStats refresh() {
            return probe.processStats();
        }
    }
}
