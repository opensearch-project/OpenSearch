/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.io.stream.StreamInput;

import java.io.IOException;

public class DeletePITRequest extends ActionRequest {

    private final String id;

    public DeletePITRequest(String id) {
        this.id = id;
    }

    public DeletePITRequest(StreamInput in) throws IOException {
        id = in.readString();
    }

    public String getId() {
        return id;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

}
