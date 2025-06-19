/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sdk.sample.helloworld.transport;

import java.io.IOException;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

/**
 * A sample request class to demonstrate extension actions
 */
public class SampleRequest extends ActionRequest {

    private final String name;

    /**
     * Instantiate this request
     *
     * @param name A name to pass to the action
     */
    public SampleRequest(String name) {
        this.name = name;
    }

    /**
     * Instantiate this request from a byte stream
     *
     * @param in the byte stream
     * @throws java.io.IOException on failure reading the stream
     */
    public SampleRequest(StreamInput in) throws IOException {
        this.name = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public String getName() {
        return name;
    }
}
