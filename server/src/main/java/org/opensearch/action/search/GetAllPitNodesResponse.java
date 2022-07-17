/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.nodes.BaseNodesResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This class transforms active PIT objects from all nodes to unique PIT objects
 */
public class GetAllPitNodesResponse extends BaseNodesResponse<GetAllPitNodeResponse> implements ToXContentObject {
    List<ListPitInfo> pitsInfo = new ArrayList<>();

    @Inject
    public GetAllPitNodesResponse(StreamInput in) throws IOException {
        super(in);
    }

    public GetAllPitNodesResponse(
        ClusterName clusterName,
        List<GetAllPitNodeResponse> getAllPitNodeResponse,
        List<FailedNodeException> failures
    ) {
        super(clusterName, getAllPitNodeResponse, failures);
        Set<String> uniquePitIds = new HashSet<>();
        pitsInfo.addAll(
            getAllPitNodeResponse.stream()
                .flatMap(p -> p.getPitsInfo().stream().filter(t -> uniquePitIds.add(t.getPitId())))
                .collect(Collectors.toList())
        );
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startArray("pitsInfo");
        for (ListPitInfo pit : pitsInfo) {
            pit.toXContent(builder, params);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    @Override
    public List<GetAllPitNodeResponse> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(GetAllPitNodeResponse::new);
    }

    @Override
    public void writeNodesTo(StreamOutput out, List<GetAllPitNodeResponse> nodes) throws IOException {
        out.writeList(nodes);
    }

    public List<ListPitInfo> getPITIDs() {
        return new ArrayList<>(pitsInfo);
    }
}
