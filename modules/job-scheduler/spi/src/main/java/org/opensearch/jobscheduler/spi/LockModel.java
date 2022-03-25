/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.jobscheduler.spi;

import org.opensearch.common.Strings;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentParserUtils;
import org.opensearch.index.seqno.SequenceNumbers;

import java.io.IOException;
import java.time.Instant;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public final class LockModel implements ToXContentObject {
    private static final String LOCK_ID_DELIMITR = "-";
    public static final String JOB_INDEX_NAME = "job_index_name";
    public static final String JOB_ID = "job_id";
    public static final String LOCK_TIME = "lock_time";
    public static final String LOCK_DURATION = "lock_duration_seconds";
    public static final String RELEASED = "released";

    private final String lockId;
    private final String jobIndexName;
    private final String jobId;
    private final Instant lockTime;
    private final long lockDurationSeconds;
    private final boolean released;
    private final long seqNo;
    private final long primaryTerm;

    /**
     * Use this constructor to copy existing lock and update the seqNo and primaryTerm.
     *
     * @param copyLock    JobSchedulerLockModel to copy from.
     * @param seqNo       sequence number from OpenSearch document.
     * @param primaryTerm primary term from OpenSearch document.
     */
    public LockModel(final LockModel copyLock, long seqNo, long primaryTerm) {
        this(copyLock.jobIndexName, copyLock.jobId, copyLock.lockTime, copyLock.lockDurationSeconds,
            copyLock.released, seqNo, primaryTerm);
    }

    /**
     * Use this constructor to copy existing lock and change status of the released of the lock.
     *
     * @param copyLock JobSchedulerLockModel to copy from.
     * @param released boolean flag to indicate if the lock is released
     */
    public LockModel(final LockModel copyLock, final boolean released) {
        this(copyLock.jobIndexName, copyLock.jobId, copyLock.lockTime, copyLock.lockDurationSeconds,
            released, copyLock.seqNo, copyLock.primaryTerm);
    }

    /**
     * Use this constructor to copy existing lock and change the duration of the lock.
     *
     * @param copyLock            JobSchedulerLockModel to copy from.
     * @param updateLockTime      new updated lock time to start the lock.
     * @param lockDurationSeconds total lock duration in seconds.
     * @param released            boolean flag to indicate if the lock is released
     */
    public LockModel(final LockModel copyLock,
                     final Instant updateLockTime, final long lockDurationSeconds, final boolean released) {
        this(copyLock.jobIndexName, copyLock.jobId, updateLockTime, lockDurationSeconds, released, copyLock.seqNo, copyLock.primaryTerm);
    }

    public LockModel(String jobIndexName, String jobId, Instant lockTime, long lockDurationSeconds, boolean released) {
        this(jobIndexName, jobId, lockTime, lockDurationSeconds, released,
            SequenceNumbers.UNASSIGNED_SEQ_NO, SequenceNumbers.UNASSIGNED_PRIMARY_TERM);
    }

    public LockModel(String jobIndexName, String jobId, Instant lockTime,
                     long lockDurationSeconds, boolean released, long seqNo, long primaryTerm) {
        this.lockId = jobIndexName + LOCK_ID_DELIMITR + jobId;
        this.jobIndexName = jobIndexName;
        this.jobId = jobId;
        this.lockTime = lockTime;
        this.lockDurationSeconds = lockDurationSeconds;
        this.released = released;
        this.seqNo = seqNo;
        this.primaryTerm = primaryTerm;
    }

    public static String generateLockId(String jobIndexName, String jobId) {
        return jobIndexName + LOCK_ID_DELIMITR + jobId;
    }

    public static LockModel parse(final XContentParser parser, long seqNo, long primaryTerm) throws IOException {
        String jobIndexName = null;
        String jobId = null;
        Instant lockTime = null;
        Long lockDurationSecond = null;
        Boolean released = null;

        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        while (!XContentParser.Token.END_OBJECT.equals(parser.nextToken())) {
            String fieldName = parser.currentName();
            parser.nextToken();
            switch (fieldName) {
                case JOB_INDEX_NAME:
                    jobIndexName = parser.text();
                    break;
                case JOB_ID:
                    jobId = parser.text();
                    break;
                case LOCK_TIME:
                    lockTime = Instant.ofEpochSecond(parser.longValue());
                    break;
                case LOCK_DURATION:
                    lockDurationSecond = parser.longValue();
                    break;
                case RELEASED:
                    released = parser.booleanValue();
                    break;
                default:
                    throw new IllegalArgumentException("Unknown field " + fieldName);
            }
        }

        return new LockModel(
            requireNonNull(jobIndexName, "JobIndexName cannot be null"),
            requireNonNull(jobId, "JobId cannot be null"),
            requireNonNull(lockTime, "lockTime cannot be null"),
            requireNonNull(lockDurationSecond, "lockDurationSeconds cannot be null"),
            requireNonNull(released, "released cannot be null"),
            seqNo,
            primaryTerm
        );
    }

    @Override public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject()
            .field(JOB_INDEX_NAME, this.jobIndexName)
            .field(JOB_ID, this.jobId)
            .field(LOCK_TIME, this.lockTime.getEpochSecond())
            .field(LOCK_DURATION, this.lockDurationSeconds)
            .field(RELEASED, this.released)
            .endObject();
        return builder;
    }

    @Override public String toString() {
        return Strings.toString(this, false, true);
    }

    public String getLockId() {
        return lockId;
    }

    public String getJobIndexName() {
        return jobIndexName;
    }

    public String getJobId() {
        return jobId;
    }

    public Instant getLockTime() {
        return lockTime;
    }

    public long getLockDurationSeconds() {
        return lockDurationSeconds;
    }

    public boolean isReleased() {
        return released;
    }

    public long getSeqNo() {
        return seqNo;
    }

    public long getPrimaryTerm() {
        return primaryTerm;
    }

    public boolean isExpired() {
        return lockTime.getEpochSecond() + lockDurationSeconds < Instant.now().getEpochSecond();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LockModel lockModel = (LockModel) o;
        return lockDurationSeconds == lockModel.lockDurationSeconds &&
                released == lockModel.released &&
                seqNo == lockModel.seqNo &&
                primaryTerm == lockModel.primaryTerm &&
                lockId.equals(lockModel.lockId) &&
                jobIndexName.equals(lockModel.jobIndexName) &&
                jobId.equals(lockModel.jobId) &&
                lockTime.equals(lockModel.lockTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(lockId, jobIndexName, jobId, lockTime, lockDurationSeconds, released, seqNo, primaryTerm);
    }
}
