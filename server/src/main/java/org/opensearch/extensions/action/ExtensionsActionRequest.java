/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions.action;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class ExtensionsActionRequest extends ActionRequest {
    String firstRequest;

    public ExtensionsActionRequest(String firstRequest) {
        this.firstRequest = firstRequest;
    }

    ExtensionsActionRequest(StreamInput in) throws IOException {
        super(in);
        firstRequest = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(firstRequest);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
