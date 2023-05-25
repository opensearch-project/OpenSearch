/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

/*
* Modifications Copyright OpenSearch Contributors. See
* GitHub history for details.
*/

package org.opensearch.action.admin.cluster.state;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import org.opensearch.action.ProtobufActionResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ProtobufClusterName;
import org.opensearch.cluster.ProtobufClusterState;
import org.opensearch.common.io.stream.ProtobufStreamInput;
import org.opensearch.common.io.stream.ProtobufStreamOutput;

import java.io.IOException;

/**
 * The response for getting the cluster state.
*
* @opensearch.internal
*/
public class ProtobufClusterStateResponse extends ProtobufActionResponse {

    private ProtobufClusterName clusterName;
    private ProtobufClusterState clusterState;
    private boolean waitForTimedOut = false;

    public ProtobufClusterStateResponse(CodedInputStream in) throws IOException {
        super(in);
        ProtobufStreamInput protobufStreamInput = new ProtobufStreamInput(in);
        clusterName = new ProtobufClusterName(in);
        clusterState = protobufStreamInput.readOptionalWriteable(innerIn -> ProtobufClusterState.readFrom(innerIn, null));
        waitForTimedOut = in.readBool();
    }

    public ProtobufClusterStateResponse(ProtobufClusterName clusterName, ProtobufClusterState clusterState, boolean waitForTimedOut) {
        this.clusterName = clusterName;
        this.clusterState = clusterState;
        this.waitForTimedOut = waitForTimedOut;
    }

    /**
     * The requested cluster state.  Only the parts of the cluster state that were
    * requested are included in the returned {@link ClusterState} instance.
    */
    public ProtobufClusterState getState() {
        return this.clusterState;
    }

    /**
     * The name of the cluster.
    */
    public ProtobufClusterName getClusterName() {
        return this.clusterName;
    }

    /**
     * Returns whether the request timed out waiting for a cluster state with a metadata version equal or
    * higher than the specified metadata.
    */
    public boolean isWaitForTimedOut() {
        return waitForTimedOut;
    }

    @Override
    public void writeTo(CodedOutputStream out) throws IOException {
        ProtobufStreamOutput protobufStreamOutput = new ProtobufStreamOutput(out);
        clusterName.writeTo(out);
        protobufStreamOutput.writeOptionalWriteable(clusterState);
        out.writeBoolNoTag(waitForTimedOut);
    }
}
