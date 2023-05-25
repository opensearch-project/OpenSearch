/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.cluster.coordination;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import org.opensearch.common.io.stream.ProtobufWriteable;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Class encapsulating stats about the PublishClusterStateAction
*
* @opensearch.internal
*/
public class ProtobufPublishClusterStateStats implements ProtobufWriteable, ToXContentObject {

    private final long fullClusterStateReceivedCount;
    private final long incompatibleClusterStateDiffReceivedCount;
    private final long compatibleClusterStateDiffReceivedCount;

    /**
     * @param fullClusterStateReceivedCount the number of times this node has received a full copy of the cluster state from the cluster-manager.
    * @param incompatibleClusterStateDiffReceivedCount the number of times this node has received a cluster-state diff from the cluster-manager.
    * @param compatibleClusterStateDiffReceivedCount the number of times that received cluster-state diffs were compatible with
    */
    public ProtobufPublishClusterStateStats(
        long fullClusterStateReceivedCount,
        long incompatibleClusterStateDiffReceivedCount,
        long compatibleClusterStateDiffReceivedCount
    ) {
        this.fullClusterStateReceivedCount = fullClusterStateReceivedCount;
        this.incompatibleClusterStateDiffReceivedCount = incompatibleClusterStateDiffReceivedCount;
        this.compatibleClusterStateDiffReceivedCount = compatibleClusterStateDiffReceivedCount;
    }

    public ProtobufPublishClusterStateStats(CodedInputStream in) throws IOException {
        fullClusterStateReceivedCount = in.readInt64();
        incompatibleClusterStateDiffReceivedCount = in.readInt64();
        compatibleClusterStateDiffReceivedCount = in.readInt64();
    }

    @Override
    public void writeTo(CodedOutputStream out) throws IOException {
        out.writeInt64NoTag(fullClusterStateReceivedCount);
        out.writeInt64NoTag(incompatibleClusterStateDiffReceivedCount);
        out.writeInt64NoTag(compatibleClusterStateDiffReceivedCount);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("published_cluster_states");
        {
            builder.field("full_states", fullClusterStateReceivedCount);
            builder.field("incompatible_diffs", incompatibleClusterStateDiffReceivedCount);
            builder.field("compatible_diffs", compatibleClusterStateDiffReceivedCount);
        }
        builder.endObject();
        return builder;
    }

    public long getFullClusterStateReceivedCount() {
        return fullClusterStateReceivedCount;
    }

    public long getIncompatibleClusterStateDiffReceivedCount() {
        return incompatibleClusterStateDiffReceivedCount;
    }

    public long getCompatibleClusterStateDiffReceivedCount() {
        return compatibleClusterStateDiffReceivedCount;
    }

    @Override
    public String toString() {
        return "ProtobufPublishClusterStateStats(full="
            + fullClusterStateReceivedCount
            + ", incompatible="
            + incompatibleClusterStateDiffReceivedCount
            + ", compatible="
            + compatibleClusterStateDiffReceivedCount
            + ")";
    }
}
