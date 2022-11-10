/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.jobscheduler.scheduler;

import org.opensearch.jobscheduler.spi.ScheduledJobParameter;
import org.opensearch.threadpool.Scheduler;

import java.time.Instant;

class JobSchedulingInfo {

    private String indexName;
    private String jobId;
    private ScheduledJobParameter jobParameter;
    private boolean descheduled = false;
    private Instant actualPreviousExecutionTime;
    private Instant expectedPreviousExecutionTime;
    private Instant expectedExecutionTime;
    private Scheduler.ScheduledCancellable scheduledCancellable;

    JobSchedulingInfo(String indexName, String jobId, ScheduledJobParameter jobParameter) {
        this.indexName = indexName;
        this.jobId = jobId;
        this.jobParameter = jobParameter;
    }

    public String getIndexName() {
        return indexName;
    }

    public String getJobId() {
        return jobId;
    }

    public ScheduledJobParameter getJobParameter() {
        return jobParameter;
    }

    public boolean isDescheduled() {
        return descheduled;
    }

    public Instant getActualPreviousExecutionTime() {
        return actualPreviousExecutionTime;
    }

    public Instant getExpectedPreviousExecutionTime() {
        return expectedPreviousExecutionTime;
    }

    public Instant getExpectedExecutionTime() {
        return this.expectedExecutionTime;
    }

    public Scheduler.ScheduledCancellable getScheduledCancellable() {
        return scheduledCancellable;
    }

    public void setDescheduled(boolean descheduled) {
        this.descheduled = descheduled;
    }

    public void setActualPreviousExecutionTime(Instant actualPreviousExecutionTime) {
        this.actualPreviousExecutionTime = actualPreviousExecutionTime;
    }

    public void setExpectedPreviousExecutionTime(Instant expectedPreviousExecutionTime) {
        this.expectedPreviousExecutionTime = expectedPreviousExecutionTime;
    }

    public void setExpectedExecutionTime(Instant expectedExecutionTime) {
        this.expectedExecutionTime = expectedExecutionTime;
    }

    public void setScheduledCancellable(Scheduler.ScheduledCancellable scheduledCancellable) {
        this.scheduledCancellable = scheduledCancellable;
    }

}
