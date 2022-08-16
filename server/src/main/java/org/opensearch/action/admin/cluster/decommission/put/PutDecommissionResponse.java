/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.decommission.put;

import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.ToXContentObject;

import java.io.IOException;

/**
 * A response to {@link PutDecommissionRequest} indicating that requested nodes has been decommissioned
 *
 * @opensearch.internal
 */
public class PutDecommissionResponse extends AcknowledgedResponse implements ToXContentObject {

    PutDecommissionResponse(StreamInput in) throws IOException {
        super(in);
    }

    PutDecommissionResponse(boolean acknowledged) {
        super(acknowledged);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
    }

    // TODO - once we have custom fields, we need to override addCustomFields method
}
