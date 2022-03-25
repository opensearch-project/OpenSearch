/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.jobscheduler.spi;

import org.opensearch.jobscheduler.spi.schedule.Schedule;
import org.opensearch.common.xcontent.ToXContentObject;

import java.time.Instant;

/**
 * Job parameters that being used by the JobScheduler.
 */
public interface ScheduledJobParameter extends ToXContentObject {
    /**
     * @return job name.
     */
    String getName();

    /**
     * @return job last update time.
     */
    Instant getLastUpdateTime();

    /**
     * @return get job enabled time.
     */
    Instant getEnabledTime();

    /**
     * @return job schedule.
     */
    Schedule getSchedule();

    /**
     * @return true if job is enabled, false otherwise.
     */
    boolean isEnabled();

    /**
     * @return Null if scheduled job doesn't need lock. Seconds of lock duration if the scheduled job needs to be a singleton runner.
     */
    default Long getLockDurationSeconds() {
        return null;
    }

    /**
     * Job will be delayed randomly with range of (0, jitter)*interval for the
     * next execution time. For example, if next run is 10 minutes later, jitter
     * is 0.6, then next job run will be randomly delayed by 0 to 6 minutes.
     *
     * Jitter is percentage, so it should be positive and less than 1.
     * <p>
     * <b>Note:</b> default logic for these cases:
     * 1).If jitter is not set, will regard it as 0.0.
     * 2).If jitter is negative, will reset it as 0.0.
     * 3).If jitter exceeds jitter limit, will cap it as jitter limit. Default
     * jitter limit is 0.95. So if you set jitter as 0.96, will cap it as 0.95.
     *
     * @return job execution jitter
     */
    default Double getJitter() {return null;}
}
