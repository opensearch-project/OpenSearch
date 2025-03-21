/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.shards;

import org.opensearch.Version;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.pagination.PageParams;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeReadRequest;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.rest.action.admin.cluster.ClusterAdminTask;

import java.io.IOException;
import java.util.Map;

/**
 * A request of _cat/shards.
 *
 * @opensearch.api
 */
public class CatShardsRequest extends ClusterManagerNodeReadRequest<CatShardsRequest> {

    private String[] indices;
    private TimeValue cancelAfterTimeInterval;
    private PageParams pageParams = null;
    private boolean requestLimitCheckSupported;

    public CatShardsRequest() {}

    public CatShardsRequest(StreamInput in) throws IOException {
        super(in);
        if (in.getVersion().onOrAfter(Version.V_2_18_0)) {
            indices = in.readStringArray();
            cancelAfterTimeInterval = in.readOptionalTimeValue();
            if (in.readBoolean()) {
                pageParams = new PageParams(in);
            }
            requestLimitCheckSupported = in.readBoolean();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getVersion().onOrAfter(Version.V_2_18_0)) {
            if (indices == null) {
                out.writeVInt(0);
            } else {
                out.writeStringArray(indices);
            }
            out.writeOptionalTimeValue(cancelAfterTimeInterval);
            out.writeBoolean(pageParams != null);
            if (pageParams != null) {
                pageParams.writeTo(out);
            }
            out.writeBoolean(requestLimitCheckSupported);
        }
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public void setIndices(String[] indices) {
        this.indices = indices;
    }

    public String[] getIndices() {
        return this.indices;
    }

    public void setCancelAfterTimeInterval(TimeValue timeout) {
        this.cancelAfterTimeInterval = timeout;
    }

    public TimeValue getCancelAfterTimeInterval() {
        return this.cancelAfterTimeInterval;
    }

    public void setPageParams(PageParams pageParams) {
        this.pageParams = pageParams;
    }

    public PageParams getPageParams() {
        return pageParams;
    }

    public void setRequestLimitCheckSupported(final boolean requestLimitCheckSupported) {
        this.requestLimitCheckSupported = requestLimitCheckSupported;
    }

    public boolean isRequestLimitCheckSupported() {
        return this.requestLimitCheckSupported;
    }

    @Override
    public ClusterAdminTask createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new ClusterAdminTask(id, type, action, parentTaskId, headers, this.cancelAfterTimeInterval);
    }
}
