/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.jobscheduler.scheduler;

import org.opensearch.jobscheduler.JobSchedulerPlugin;
import org.opensearch.jobscheduler.spi.JobExecutionContext;
import org.opensearch.jobscheduler.spi.ScheduledJobParameter;
import org.opensearch.jobscheduler.spi.ScheduledJobRunner;
import org.opensearch.jobscheduler.spi.JobDocVersion;
import org.opensearch.jobscheduler.spi.utils.LockService;
import org.opensearch.jobscheduler.utils.VisibleForTesting;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.Randomness;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Components that handles job scheduling/descheduling.
 */
public class JobScheduler {
    private static final Logger log = LogManager.getLogger(JobScheduler.class);

    private ThreadPool threadPool;
    private ScheduledJobInfo scheduledJobInfo;
    private Clock clock;
    private final LockService lockService;

    public JobScheduler(ThreadPool threadPool, final LockService lockService) {
        this.threadPool = threadPool;
        this.scheduledJobInfo = new ScheduledJobInfo();
        this.clock = Clock.systemDefaultZone();
        this.lockService = lockService;
    }

    @VisibleForTesting
    void setClock(Clock clock) {
        this.clock = clock;
    }

    @VisibleForTesting
    ScheduledJobInfo getScheduledJobInfo() {
        return this.scheduledJobInfo;
    }

    public Set<String> getScheduledJobIds(String indexName) {
        return this.scheduledJobInfo.getJobsByIndex(indexName).keySet();
    }

    public boolean schedule(String indexName, String docId, ScheduledJobParameter scheduledJobParameter,
                            ScheduledJobRunner jobRunner, JobDocVersion version, Double jitterLimit) {
        if (!scheduledJobParameter.isEnabled()) {
            return false;
        }
        log.info("Scheduling job id {} for index {} .", docId, indexName);
        JobSchedulingInfo jobInfo;
        synchronized (this.scheduledJobInfo.getJobsByIndex(indexName)) {
            jobInfo = this.scheduledJobInfo.getJobInfo(indexName, docId);
            if (jobInfo == null) {
                jobInfo = new JobSchedulingInfo(indexName, docId, scheduledJobParameter);
                this.scheduledJobInfo.addJob(indexName, docId, jobInfo);
            }
            if (jobInfo.getScheduledCancellable() != null) {
                return true;
            }

            this.reschedule(scheduledJobParameter, jobInfo, jobRunner, version, jitterLimit);
        }

        return true;
    }

    public List<String> bulkDeschedule(String indexName, Collection<String> ids) {
        if (ids == null) {
            return new ArrayList<>();
        }
        List<String> result = new ArrayList<>();
        for (String id : ids) {
            if (!this.deschedule(indexName, id)) {
                result.add(id);
                log.error("Unable to deschedule job {}", id);
            }
        }
        return result;
    }

    public boolean deschedule(String indexName, String id) {
        JobSchedulingInfo jobInfo = this.scheduledJobInfo.getJobInfo(indexName, id);
        if (jobInfo == null) {
            log.debug("JobId {} doesn't not exist, skip descheduling.", id);
            return true;
        }

        log.info("Descheduling jobId: {}", id);
        jobInfo.setDescheduled(true);
        jobInfo.setActualPreviousExecutionTime(null);
        jobInfo.setExpectedPreviousExecutionTime(null);
        Scheduler.ScheduledCancellable scheduledCancellable = jobInfo.getScheduledCancellable();

        if (scheduledCancellable != null) {
            if (scheduledCancellable.cancel()) {
                this.scheduledJobInfo.removeJob(indexName, id);
            } else {
                return false;
            }
        }

        return true;
    }

    @VisibleForTesting
    boolean reschedule(ScheduledJobParameter jobParameter, JobSchedulingInfo jobInfo, ScheduledJobRunner jobRunner,
                       JobDocVersion version, Double jitterLimit) {
        if (jobParameter.getEnabledTime() == null) {
            log.info("There is no enable time of job {}, this job should never be scheduled.",
                    jobParameter.getName());
            return false;
        }

        Instant nextExecutionTime = jobParameter.getSchedule().getNextExecutionTime(jobInfo.getExpectedExecutionTime());
        if (nextExecutionTime == null) {
            log.info("No next execution time for job {}", jobParameter.getName());
            return true;
        }
        Duration duration = Duration.between(this.clock.instant(), nextExecutionTime);

        // Too many jobs start at the same time point will bring burst. Add random jitter delay to spread out load.
        // Example, if interval is 10 minutes, jitter is 0.6, next job run will be randomly delayed by 0 to 10*0.6 minutes.
        Instant secondExecutionTimeFromNow = jobParameter.getSchedule().getNextExecutionTime(nextExecutionTime);
        if (secondExecutionTimeFromNow != null) {
            Duration interval = Duration.between(nextExecutionTime, secondExecutionTimeFromNow);
            if (interval.toMillis() > 0) {
                double jitter = jobParameter.getJitter() == null ? 0d : jobParameter.getJitter();
                jitter = jitter > jitterLimit ? jitterLimit : jitter;
                jitter = jitter < 0 ? 0 : jitter;
                long randomLong = Randomness.get().nextLong();
                if (randomLong == Long.MIN_VALUE) randomLong += 17; // to ensure the * -1 below doesn't fail to change to positive
                long randomPositiveLong = randomLong < 0 ? randomLong * -1 : randomLong;
                long jitterMillis = Math.round(randomPositiveLong % interval.toMillis() * jitter);
                if (jitter > 0) {
                    log.info("Will delay {} miliseconds for next execution of job {}", jitterMillis, jobParameter.getName());
                }
                duration = duration.plusMillis(jitterMillis);
            }
        }

        jobInfo.setExpectedExecutionTime(nextExecutionTime);

        Runnable runnable = () -> {
            if (jobInfo.isDescheduled()) {
                return;
            }

            jobInfo.setExpectedPreviousExecutionTime(jobInfo.getExpectedExecutionTime());
            jobInfo.setActualPreviousExecutionTime(clock.instant());
            // schedule next execution
            this.reschedule(jobParameter, jobInfo, jobRunner, version, jitterLimit);

            // invoke job runner
            JobExecutionContext context = new JobExecutionContext(jobInfo.getExpectedPreviousExecutionTime(), version, lockService,
                jobInfo.getIndexName(), jobInfo.getJobId());

            jobRunner.runJob(jobParameter, context);
        };

        if (jobInfo.isDescheduled()) {
            return false;
        }

        jobInfo.setScheduledCancellable(this.threadPool.schedule(runnable, new TimeValue(duration.toNanos(),
                        TimeUnit.NANOSECONDS), JobSchedulerPlugin.OPEN_DISTRO_JOB_SCHEDULER_THREAD_POOL_NAME));

        return true;
    }

}
