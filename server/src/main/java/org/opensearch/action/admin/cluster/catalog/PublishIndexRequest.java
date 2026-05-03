/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.catalog;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeRequest;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * @opensearch.experimental
 */
public class PublishIndexRequest extends ClusterManagerNodeRequest<PublishIndexRequest> {

    private final String indexName;

    public PublishIndexRequest(String indexName) {
        this.indexName = indexName;
    }

    public PublishIndexRequest(StreamInput in) throws IOException {
        super(in);
        this.indexName = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(indexName);
    }

    public String indexName() {
        return indexName;
    }

    @Override
    public ActionRequestValidationException validate() {
        if (Strings.isNullOrEmpty(indexName)) {
            ActionRequestValidationException ex = new ActionRequestValidationException();
            ex.addValidationError("indexName must not be empty");
            return ex;
        }
        return null;
    }
}
