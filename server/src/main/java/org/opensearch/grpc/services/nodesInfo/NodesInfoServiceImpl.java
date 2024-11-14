/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.grpc.services.nodesInfo;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.opensearch.action.admin.cluster.node.info.proto.NodesInfoProtoService;
import org.opensearch.action.admin.cluster.node.info.proto.NodesInfoServiceGrpc;

public class NodesInfoServiceImpl extends NodesInfoServiceGrpc.NodesInfoServiceImplBase {

    @Override
    public void getNodesInfo(NodesInfoProtoService.NodesInfoRequest request, StreamObserver<NodesInfoProtoService.NodesInfoResponse> responseObserver) {
        try {
            System.out.println("getNodesInfo");

            NodesInfoProtoService.NodesInfoResponse response = NodesInfoProtoService.NodesInfoResponse.newBuilder()
                .setClusterName("test-cluster")
                .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void streamNodesInfo(NodesInfoProtoService.NodesInfoRequest request, StreamObserver<NodesInfoProtoService.NodesInfoResponse> responseObserver) {
        responseObserver.onError(
            Status.UNIMPLEMENTED.withDescription("Method StreamNodesInfo is not implemented").asRuntimeException()
        );
    }
}
