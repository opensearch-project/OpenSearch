/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.actions;

import org.opensearch.action.main.MainRequest;
import org.opensearch.action.main.MainResponse;
import org.opensearch.client.Client;
import org.opensearch.core.action.ActionListener;
import org.opensearch.proto.grpc.MainAction;
import org.opensearch.proto.grpc.MainActionServiceGrpc;

import io.grpc.stub.StreamObserver;

/**
 * gRPC version of the MainAction, i.e. `GET /`
 */
public final class MainActionService extends MainActionServiceGrpc.MainActionServiceImplBase {
    private final Client client;

    public MainActionService(Client client) {
        this.client = client;
    }

    @Override
    public void get(MainAction.MainRequest request, StreamObserver<MainAction.MainResponse> responseObserver) {
        client.execute(org.opensearch.action.main.MainAction.INSTANCE, new MainRequest(), new ActionListener<>() {
            @Override
            public void onResponse(MainResponse mainResponse) {
                responseObserver.onNext(
                    MainAction.MainResponse.newBuilder()
                        .setName(mainResponse.getNodeName())
                        .setClusterName(mainResponse.getClusterName().value())
                        .setClusterUuid(mainResponse.getClusterUuid())
                        .setVersion(
                            MainAction.MainResponse.Version.newBuilder()
                                .setDistribution(mainResponse.getBuild().getDistribution())
                                .setNumber(mainResponse.getBuild().getQualifiedVersion())
                                .setBuildType(mainResponse.getBuild().type().displayName())
                                .setBuildHash(mainResponse.getBuild().hash())
                                .setBuildDate(mainResponse.getBuild().date())
                                .setBuildSnapshot(Boolean.toString(mainResponse.getBuild().isSnapshot()))
                                .setLuceneVersion(mainResponse.getVersion().luceneVersion.toString())
                                .setMinimumWireCompatibilityVersion(mainResponse.getVersion().minimumCompatibilityVersion().toString())
                                .setMinimumIndexCompatibilityVersion(
                                    mainResponse.getVersion().minimumIndexCompatibilityVersion().toString()
                                )
                                .build()
                        )
                        .setTagline("The OpenSearch Project: https://opensearch.org/")
                        .build()
                );
                responseObserver.onCompleted();
            }

            @Override
            public void onFailure(Exception e) {
                responseObserver.onError(e);
            }
        });
    }
}
