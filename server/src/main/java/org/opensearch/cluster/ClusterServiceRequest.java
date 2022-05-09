/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.transport.TransportRequest;

import java.io.IOException;

public class ClusterServiceRequest extends TransportRequest {
    private enum ClusterStateEnum {
        CLUSTER_STATE("CLUSTER_STATE"),
        CLUSTER_SETTINGS("CLUSTER_SETTINGS");

        public final String label;

        ClusterStateEnum(String label) {
            this.label = label;
        }
    }

    private String state;

    public ClusterServiceRequest(String state) {
        this.state = state;
    }

    public ClusterServiceRequest(StreamInput in) throws IOException {
        super(in);
        in.readEnum(ClusterStateEnum.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeEnum(ClusterStateEnum.CLUSTER_STATE);
        out.writeEnum(ClusterStateEnum.CLUSTER_SETTINGS);
    }

}
