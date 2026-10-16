/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.registerplugins;

import java.io.IOException;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeRequest;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

/**
 * Request for registering plugins
 *
 * @opensearch.internal
 */
public class RegisterPluginsRequest extends ClusterManagerNodeRequest<RegisterPluginsRequest> {
    private boolean registerAnalysis = true;

    public RegisterPluginsRequest() {}

    public RegisterPluginsRequest(StreamInput in) throws IOException {
        super(in);
        registerAnalysis = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(registerAnalysis);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public boolean isRegisterAnalysis() { return registerAnalysis; }
    public void setRegisterAnalysis(boolean registerAnalysis) { this.registerAnalysis = registerAnalysis; }
}
