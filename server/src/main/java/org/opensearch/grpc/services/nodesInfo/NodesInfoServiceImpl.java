/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.grpc.services.nodesInfo;

import io.grpc.stub.StreamObserver;
import org.opensearch.action.admin.cluster.node.info.proto.NodesInfoProtoService;
import org.opensearch.action.admin.cluster.node.info.proto.NodesInfoServiceGrpc;

public class NodesInfoServiceImpl extends NodesInfoServiceGrpc.NodesInfoServiceImplBase {

    @Override
    public void getNodesInfo(NodesInfoProtoService.NodesInfoRequest request, StreamObserver<NodesInfoProtoService.NodesInfoResponse> responseObserver) {
        // Func stub
    }

    @Override
    public void streamNodesInfo(NodesInfoProtoService.NodesInfoRequest request, StreamObserver<NodesInfoProtoService.NodesInfoResponse> responseObserver) {
        // Func stub
    }
}
