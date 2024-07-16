/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.action;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;

/**
 * A request for get QueryGroup
 *
 * @opensearch.experimental
 */
public class GetQueryGroupRequest extends ActionRequest implements Writeable.Reader<GetQueryGroupRequest> {
    String name;

    /**
     * Default constructor for GetQueryGroupRequest
     * @param name - name for the QueryGroup to get
     */
    public GetQueryGroupRequest(String name) {
        this.name = name;
    }

    /**
     * Constructor for GetQueryGroupRequest
     * @param in - A {@link StreamInput} object
     */
    public GetQueryGroupRequest(StreamInput in) throws IOException {
        super(in);
        name = in.readOptionalString();
    }

    @Override
    public GetQueryGroupRequest read(StreamInput in) throws IOException {
        return new GetQueryGroupRequest(in);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    /**
     * Name getter
     */
    public String getName() {
        return name;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(name);
    }
}
