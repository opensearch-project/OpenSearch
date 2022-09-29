/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.authz;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.transport.TransportRequest;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * Request to evaluate user privileges for actions
 *
 * This object encapsulates all that is needed to perform authorization on a request
 *
 * @opensearch.experimental
 */
public class AuthorizationRequest extends TransportRequest {
    private String permissionId;
    private Map<String, CheckableParameter> params;

    public AuthorizationRequest(
        String permissionId,
        Map<String, CheckableParameter> params
    ) {
        this.permissionId = permissionId;
        this.params = params;
    }

    public AuthorizationRequest(StreamInput in) throws IOException {
        super(in);
        permissionId = in.readString();
        if (in.readBoolean()) {
            params = in.readMap(StreamInput::readString, i -> {
                try {
                    return CheckableParameter.readParameterFromStream(i);
                } catch (ClassNotFoundException e) {
                    // Should not happen, CheckableParameter writes its type to StreamOutput
                    throw new RuntimeException(e);
                }
            });
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(permissionId);
        boolean hasParams = params != null;
        out.writeBoolean(hasParams);
        if (hasParams) {
            out.writeMap(params, StreamOutput::writeString, (o, s) -> CheckableParameter.writeParameterToStream(s, o));
        }
    }

    public String getPermissionId() {
        return permissionId;
    }

    public Map<String, CheckableParameter> getParams() {
        return params;
    }

    @Override
    public String toString() {
        return "AuthorizationRequest{permissionId="
            + permissionId
            + ", params="
            + params
            + "}";
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        AuthorizationRequest that = (AuthorizationRequest) obj;
        return Objects.equals(permissionId, that.permissionId) && Objects.equals(params, that.params);
    }

    @Override
    public int hashCode() {
        return Objects.hash(permissionId, params);
    }
}
