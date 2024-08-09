/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http.executioncontextplugin;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Request object for GetExecutionContext transport action
 */
public class TestGetExecutionContextRequest extends ActionRequest {

    /**
     * Default constructor
     */
    public TestGetExecutionContextRequest() {}

    /**
     * Constructor with stream input
     * @param in the stream input
     * @throws IOException IOException
     */
    public TestGetExecutionContextRequest(final StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
