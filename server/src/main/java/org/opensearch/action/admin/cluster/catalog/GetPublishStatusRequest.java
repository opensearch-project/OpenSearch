/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.catalog;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeReadRequest;
import org.opensearch.common.Nullable;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Exactly one of {@code indexName} or {@code publishId} must be set.
 *
 * @opensearch.experimental
 */
public class GetPublishStatusRequest extends ClusterManagerNodeReadRequest<GetPublishStatusRequest> {

    @Nullable
    private final String indexName;
    @Nullable
    private final String publishId;

    public GetPublishStatusRequest(@Nullable String indexName, @Nullable String publishId) {
        this.indexName = indexName;
        this.publishId = publishId;
    }

    public GetPublishStatusRequest(StreamInput in) throws IOException {
        super(in);
        this.indexName = in.readOptionalString();
        this.publishId = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(indexName);
        out.writeOptionalString(publishId);
    }

    @Nullable
    public String indexName() { return indexName; }

    @Nullable
    public String publishId() { return publishId; }

    @Override
    public ActionRequestValidationException validate() {
        boolean hasIndex = !Strings.isNullOrEmpty(indexName);
        boolean hasId = !Strings.isNullOrEmpty(publishId);
        if (hasIndex == hasId) {
            ActionRequestValidationException ex = new ActionRequestValidationException();
            ex.addValidationError("exactly one of indexName or publishId must be set");
            return ex;
        }
        return null;
    }
}
