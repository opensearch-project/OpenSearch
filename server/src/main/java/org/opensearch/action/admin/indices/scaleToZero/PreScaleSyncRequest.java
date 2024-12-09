/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.scaleToZero;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

public class PreScaleSyncRequest extends ActionRequest {
    private final String index;

    public PreScaleSyncRequest(String index) {
        this.index = index;
    }

    public PreScaleSyncRequest(StreamInput in) throws IOException {
        super(in);
        this.index = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(index);
    }

    public String getIndex() {
        return index;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
