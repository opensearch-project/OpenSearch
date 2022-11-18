/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.jobscheduler.spi;

public interface ScheduledJobRunner {
    void runJob(ScheduledJobParameter job, JobExecutionContext context);
}
