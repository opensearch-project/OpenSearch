/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.action;

import org.opensearch.analytics.exec.profile.ProfiledResult;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Response carrying materialized query results from the analytics engine.
 * Local-dispatch only — the rows are held in-memory and not wire-serializable.
 */
public class AnalyticsQueryResponse extends ActionResponse {

    private final transient Iterable<Object[]> rows;
    private final transient ProfiledResult profiledResult;

    public AnalyticsQueryResponse(Iterable<Object[]> rows) {
        this.rows = rows;
        this.profiledResult = null;
    }

    public AnalyticsQueryResponse(ProfiledResult profiledResult) {
        this.rows = profiledResult.rows();
        this.profiledResult = profiledResult;
    }

    public AnalyticsQueryResponse(StreamInput in) throws IOException {
        super(in);
        throw new UnsupportedOperationException("AnalyticsQueryResponse is local-dispatch only");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("AnalyticsQueryResponse is local-dispatch only");
    }

    public Iterable<Object[]> getRows() {
        return rows;
    }

    public ProfiledResult getProfiledResult() {
        return profiledResult;
    }
}
