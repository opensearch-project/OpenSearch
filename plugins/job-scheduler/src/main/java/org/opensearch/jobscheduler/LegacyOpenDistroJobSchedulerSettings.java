/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.jobscheduler;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.unit.TimeValue;

public class LegacyOpenDistroJobSchedulerSettings {
    public static final Setting<TimeValue> REQUEST_TIMEOUT = Setting.positiveTimeSetting(
            "opendistro.jobscheduler.request_timeout",
            TimeValue.timeValueSeconds(10),
            Setting.Property.NodeScope, Setting.Property.Dynamic, Setting.Property.Deprecated);

    public static final Setting<TimeValue> SWEEP_BACKOFF_MILLIS = Setting.positiveTimeSetting(
            "opendistro.jobscheduler.sweeper.backoff_millis",
            TimeValue.timeValueMillis(50),
            Setting.Property.NodeScope, Setting.Property.Dynamic, Setting.Property.Deprecated);

    public static final Setting<Integer> SWEEP_BACKOFF_RETRY_COUNT = Setting.intSetting(
            "opendistro.jobscheduler.retry_count",
            3,
            Setting.Property.NodeScope, Setting.Property.Dynamic, Setting.Property.Deprecated);

    public static final Setting<TimeValue> SWEEP_PERIOD = Setting.positiveTimeSetting(
            "opendistro.jobscheduler.sweeper.period",
            TimeValue.timeValueMinutes(5),
            Setting.Property.NodeScope, Setting.Property.Dynamic, Setting.Property.Deprecated);

    public static final Setting<Integer> SWEEP_PAGE_SIZE = Setting.intSetting(
            "opendistro.jobscheduler.sweeper.page_size",
            100,
            Setting.Property.NodeScope, Setting.Property.Dynamic, Setting.Property.Deprecated);

    public static final Setting<Double> JITTER_LIMIT = Setting.doubleSetting(
            "opendistro.jobscheduler.jitter_limit",
            0.60, 0, 0.95,
            Setting.Property.NodeScope, Setting.Property.Dynamic, Setting.Property.Deprecated);
}
