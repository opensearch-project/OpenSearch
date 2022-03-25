/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.jobscheduler.spi;

import java.util.Locale;

/**
 * Structure to represent scheduled job document version. JobScheduler use this to determine this job
 */
public class JobDocVersion implements Comparable<JobDocVersion> {
    private final long primaryTerm;
    private final long seqNo;
    private final long version;

    public JobDocVersion(long primaryTerm, long seqNo, long version) {
        this.primaryTerm = primaryTerm;
        this.seqNo = seqNo;
        this.version = version;
    }

    public long getPrimaryTerm() {
        return primaryTerm;
    }

    public long getSeqNo() {
        return seqNo;
    }

    public long getVersion() {
        return version;
    }

    /**
     * Compare two doc versions. Refer to https://github.com/elastic/elasticsearch/issues/10708
     *
     * @param v the doc version to compare.
     * @return -1 if this &lt; v, 0 if this == v, otherwise 1;
     */
    @Override
    public int compareTo(JobDocVersion v) {
        if (v == null) {
            return 1;
        }
        if (this.seqNo < v.seqNo) {
            return -1;
        }
        if (this.seqNo > v.seqNo) {
            return 1;
        }
        if(this.primaryTerm < v.primaryTerm) {
            return -1;
        }
        if(this.primaryTerm > v.primaryTerm) {
            return 1;
        }
        return 0;
    }

    @Override
    public String toString() {
        return String.format(Locale.getDefault(), "{_version: %s, _primary_term: %s, _seq_no: %s}", this.version,
                this.primaryTerm, this.seqNo);
    }
}
