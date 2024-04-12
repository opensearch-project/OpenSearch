/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.resource_limit_group;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

public class SampleRequest extends ActionRequest {

    public SampleRequest() {
    }

    public SampleRequest(StreamInput in) throws IOException {
        super(in);
    }

    /**
     * @return
     */
    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    /**
     * @param out
     * @throws IOException
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
    }
}
