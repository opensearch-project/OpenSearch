/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.catalog;

import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Response to {@link PublishIndexAction}. Carries the generated {@code publishId} for
 * status polling.
 *
 * @opensearch.experimental
 */
public class PublishIndexResponse extends AcknowledgedResponse {

    private final String publishId;

    public PublishIndexResponse(boolean acknowledged, String publishId) {
        super(acknowledged);
        this.publishId = publishId;
    }

    public PublishIndexResponse(StreamInput in) throws IOException {
        super(in);
        this.publishId = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(publishId);
    }

    public String publishId() {
        return publishId;
    }

    @Override
    protected void addCustomFields(XContentBuilder builder, Params params) throws IOException {
        if (publishId != null) {
            builder.field("publish_id", publishId);
        }
    }
}
