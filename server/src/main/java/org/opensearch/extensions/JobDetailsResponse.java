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
import org.opensearch.transport.TransportResponse;

import java.io.IOException;
import java.util.Objects;

/**
 * JobDetails Response from extensions
 *
 * @opensearch.internal
 */
public class JobDetailsResponse extends TransportResponse {

    private JobDetails jobDetails;

    public JobDetailsResponse(JobDetails jobDetails) {
        this.jobDetails = jobDetails;
    }

    public JobDetailsResponse(StreamInput in) throws IOException {
        super(in);
        jobDetails = new JobDetails(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        jobDetails.writeTo(out);
    }

    public JobDetails getJobDetails() {
        return jobDetails;
    }

    public void setJobDetails(JobDetails jobDetails) {
        this.jobDetails = jobDetails;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JobDetailsResponse that = (JobDetailsResponse) o;
        return Objects.equals(jobDetails, that.jobDetails);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobDetails);
    }

    @Override
    public String toString() {
        return "JobDetailsResponse{" + "jobDetails=" + jobDetails + '}';
    }
}
