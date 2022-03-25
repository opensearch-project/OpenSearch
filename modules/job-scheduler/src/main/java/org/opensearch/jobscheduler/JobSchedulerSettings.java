/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.jobscheduler;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.unit.TimeValue;

public class JobSchedulerSettings {
    public static final Setting<TimeValue> REQUEST_TIMEOUT = Setting.positiveTimeSetting(
            "plugins.jobscheduler.request_timeout",
            LegacyOpenDistroJobSchedulerSettings.REQUEST_TIMEOUT,
            Setting.Property.NodeScope, Setting.Property.Dynamic);

    public static final Setting<TimeValue> SWEEP_BACKOFF_MILLIS = Setting.positiveTimeSetting(
            "plugins.jobscheduler.sweeper.backoff_millis",
            LegacyOpenDistroJobSchedulerSettings.SWEEP_BACKOFF_MILLIS,
            Setting.Property.NodeScope, Setting.Property.Dynamic);

    public static final Setting<Integer> SWEEP_BACKOFF_RETRY_COUNT = Setting.intSetting(
            "plugins.jobscheduler.retry_count",
            LegacyOpenDistroJobSchedulerSettings.SWEEP_BACKOFF_RETRY_COUNT,
            Setting.Property.NodeScope, Setting.Property.Dynamic);

    public static final Setting<TimeValue> SWEEP_PERIOD = Setting.positiveTimeSetting(
            "plugins.jobscheduler.sweeper.period",
            LegacyOpenDistroJobSchedulerSettings.SWEEP_PERIOD,
            Setting.Property.NodeScope, Setting.Property.Dynamic);

    public static final Setting<Integer> SWEEP_PAGE_SIZE = Setting.intSetting(
            "plugins.jobscheduler.sweeper.page_size",
            LegacyOpenDistroJobSchedulerSettings.SWEEP_PAGE_SIZE,
            Setting.Property.NodeScope, Setting.Property.Dynamic);

    public static final Setting<Double> JITTER_LIMIT = Setting.doubleSetting(
            "plugins.jobscheduler.jitter_limit",
            LegacyOpenDistroJobSchedulerSettings.JITTER_LIMIT,
            Setting.Property.NodeScope, Setting.Property.Dynamic);
}
