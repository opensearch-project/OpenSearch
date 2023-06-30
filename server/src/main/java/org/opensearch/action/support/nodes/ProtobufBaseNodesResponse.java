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

package org.opensearch.action.support.nodes;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import org.opensearch.action.ProtobufActionResponse;
import org.opensearch.action.ProtobufFailedNodeException;
import org.opensearch.cluster.ClusterName;
import org.opensearch.common.io.stream.ProtobufStreamInput;
import org.opensearch.common.io.stream.ProtobufStreamOutput;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Transport response for nodes requests
*
* @opensearch.internal
*/
public abstract class ProtobufBaseNodesResponse<TNodeResponse extends ProtobufBaseNodeResponse> extends ProtobufActionResponse {

    private ClusterName clusterName;
    private List<ProtobufFailedNodeException> failures;
    private List<TNodeResponse> nodes;
    private Map<String, TNodeResponse> nodesMap;

    protected ProtobufBaseNodesResponse(CodedInputStream in) throws IOException {
        super(in);
        ProtobufStreamInput protobufStreamInput = new ProtobufStreamInput(in);
        clusterName = new ClusterName(in);
        nodes = readNodesFrom(in);
        failures = protobufStreamInput.readList(ProtobufFailedNodeException::new);
    }

    protected ProtobufBaseNodesResponse(ClusterName clusterName, List<TNodeResponse> nodes, List<ProtobufFailedNodeException> failures) {
        this.clusterName = Objects.requireNonNull(clusterName);
        this.failures = Objects.requireNonNull(failures);
        this.nodes = Objects.requireNonNull(nodes);
    }

    /**
     * Get the {@link ClusterName} associated with all of the nodes.
    *
    * @return Never {@code null}.
    */
    public ClusterName getClusterName() {
        return clusterName;
    }

    /**
     * Get the failed node exceptions.
    *
    * @return Never {@code null}. Can be empty.
    */
    public List<ProtobufFailedNodeException> failures() {
        return failures;
    }

    /**
     * Determine if there are any node failures in {@link #failures}.
    *
    * @return {@code true} if {@link #failures} contains at least 1 {@link ProtobufFailedNodeException}.
    */
    public boolean hasFailures() {
        return failures.isEmpty() == false;
    }

    /**
     * Get the <em>successful</em> node responses.
    *
    * @return Never {@code null}. Can be empty.
    * @see #hasFailures()
    */
    public List<TNodeResponse> getNodes() {
        return nodes;
    }

    /**
     * Lazily build and get a map of Node ID to node response.
    *
    * @return Never {@code null}. Can be empty.
    * @see #getNodes()
    */
    public Map<String, TNodeResponse> getNodesMap() {
        if (nodesMap == null) {
            nodesMap = new HashMap<>();
            for (TNodeResponse nodeResponse : nodes) {
                nodesMap.put(nodeResponse.getNode().getId(), nodeResponse);
            }
        }
        return nodesMap;
    }

    @Override
    public void writeTo(CodedOutputStream out) throws IOException {
        ProtobufStreamOutput protobufStreamOutput = new ProtobufStreamOutput(out);
        clusterName.writeTo(out);
        writeNodesTo(out, nodes);
        protobufStreamOutput.writeCollection(failures, (o, v) -> v.writeTo(o));
    }

    /**
     * Read the {@link #nodes} from the stream.
    *
    * @return Never {@code null}.
    */
    protected abstract List<TNodeResponse> readNodesFrom(CodedInputStream in) throws IOException;

    /**
     * Write the {@link #nodes} to the stream.
    */
    protected abstract void writeNodesTo(CodedOutputStream out, List<TNodeResponse> nodes) throws IOException;

}
