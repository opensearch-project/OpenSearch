/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;

import java.io.IOException;

/**
 * The object will contain job details of extension.
 *
 * @opensearch.internal
 */
public class JobDetails implements Writeable {

    private String jobType;

    private String jobIndex;

    public JobDetails(String jobType, String jobIndex) {
        this.jobType = jobType;
        this.jobIndex = jobIndex;
    }

    public JobDetails(StreamInput in) throws IOException {
        if (in.available() > 0) {
            this.jobType = in.readString();
            this.jobIndex = in.readString();
        }
    }

    public String getJobType() {
        return jobType;
    }

    public void setJobType(String jobType) {
        this.jobType = jobType;
    }

    public String getJobIndex() {
        return jobIndex;
    }

    public void setJobIndex(String jobIndex) {
        this.jobIndex = jobIndex;
    }

    @Override
    public String toString() {
        return "JobDetails [jobType=" + jobType + ", jobIndex=" + jobIndex + "]";
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(jobType);
        out.writeString(jobIndex);

    }
}
