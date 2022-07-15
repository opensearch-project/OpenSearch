/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.decommission;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.ActionListener;
import org.opensearch.action.admin.cluster.management.decommission.PutDecommissionRequest;
import org.opensearch.cluster.AckedClusterStateUpdateTask;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateApplier;
import org.opensearch.cluster.ack.ClusterStateUpdateResponse;
import org.opensearch.cluster.metadata.DecommissionedAttributeMetadata;
import org.opensearch.cluster.metadata.DecommissionedAttributesMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.component.AbstractLifecycleComponent;
import org.opensearch.common.inject.Inject;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DecommissionService extends AbstractLifecycleComponent implements ClusterStateApplier {

    private static final Logger logger = LogManager.getLogger(DecommissionService.class);

    private final ClusterService clusterService;

    private final ThreadPool threadPool;

    @Inject
    public DecommissionService(
        ClusterService clusterService,
        ThreadPool threadPool
    ) {
        this.clusterService = clusterService;
        this.threadPool = threadPool;
    }

    /**
     * Registers new decommissioned attribute in the cluster
     * <p>
     * This method can be only called on the cluster-manager node. It tries to create a new decommissioned attribute on the master
     * and if it was successful it adds new decommissioned attribute to cluster metadata.
     *
     * @param request  register decommission request
     * @param listener register decommission listener
     */
    public void registerDecommissionAttribute(
        final PutDecommissionRequest request,
        final ActionListener<ClusterStateUpdateResponse> listener
    ) {
//        assert lifecycle.started() : "Trying to decommission an attribute but service is in state [" + lifecycle.state() + "]";

        final DecommissionedAttributeMetadata newDecommissionedAttributeMetadata = new DecommissionedAttributeMetadata(
            request.getName(),
            request.getDecommissionAttribute().key(),
            request.getDecommissionAttribute().values()
        );

        // TODO - add validator on decommission attribute - to check if its a valid attribute
        // validate(request.name)

        final ActionListener<ClusterStateUpdateResponse> registrationListener;
        // TODO - this listener can be used to verify if the response was acknowledged by all the nodes -
        //  can be read from the request if the query param had verify
        // if request.verify() -> perform task

        registrationListener = listener;

        // TODO - Can we do the decommission action here? before we push the new metadata to cluster state

        clusterService.submitStateUpdateTask(
            // TODO - put request.name instead of zone
            "put_decommission [" + request.getName() + "]",
            new AckedClusterStateUpdateTask<ClusterStateUpdateResponse>(request, registrationListener) {
                @Override
                protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
                    return new ClusterStateUpdateResponse(acknowledged);
                }

                @Override
                public ClusterState execute(ClusterState currentState) {
                    //TODO - before updating cluster state ensure attribute is not in use
                    Metadata metadata = currentState.metadata();
                    Metadata.Builder mdBuilder = Metadata.builder(currentState.metadata());
                    DecommissionedAttributesMetadata decommissionedAttributes = metadata.custom(DecommissionedAttributesMetadata.TYPE);
                    if (decommissionedAttributes == null) {
                        // TODO - get attribute details from request
                        logger.info("decommission request for attribute name [{}] and attribute value [{}]", request.getName(), request.getDecommissionAttribute().toString());
                        decommissionedAttributes = new DecommissionedAttributesMetadata(
                            Collections.singletonList(new DecommissionedAttributeMetadata(request.getName(), request.getDecommissionAttribute().key(), request.getDecommissionAttribute().values()))
                        );
                    } else {
                        boolean found = false;
                        List<DecommissionedAttributeMetadata> decommissionedAttributesMetadata = new ArrayList<>(
                            decommissionedAttributes.decommissionedAttributes().size() + 1
                        );
                        for (DecommissionedAttributeMetadata decommissionedAttributeMetadata : decommissionedAttributes.decommissionedAttributes()) {
                            if (decommissionedAttributeMetadata.name().equals(newDecommissionedAttributeMetadata.name())) {
                                if (newDecommissionedAttributeMetadata.equals(decommissionedAttributeMetadata)) {
                                    // No update needed
                                    return currentState;
                                }
                                found = true;
                                decommissionedAttributesMetadata.add(newDecommissionedAttributeMetadata);
                            } else {
                                decommissionedAttributesMetadata.add(decommissionedAttributeMetadata);
                            }
                        }
                        if (!found) {
                            // TODO update name based on request
                            logger.info("put decommissioned attribute [{}]", request.getDecommissionAttribute().toString());
                            decommissionedAttributesMetadata.add(
                                new DecommissionedAttributeMetadata(
                                    request.getName(),
                                    request.getDecommissionAttribute().key(),
                                    request.getDecommissionAttribute().values()
                                )
                            );
                        } else {
                            // TODO - update name based on request
                            logger.info("update decommission attribute [{}]", request.getDecommissionAttribute().toString());
                        }
                        decommissionedAttributes = new DecommissionedAttributesMetadata(decommissionedAttributesMetadata);
                    }
                    mdBuilder.putCustom(DecommissionedAttributesMetadata.TYPE, decommissionedAttributes);
                    return ClusterState.builder(currentState).metadata(mdBuilder).build();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    logger.warn(() -> new ParameterizedMessage("failed to decommission attribute [{}]", request.getName()), e);
                    super.onFailure(source, e);
                }

                @Override
                public boolean mustAck(DiscoveryNode discoveryNode) {
                    // master must acknowledge the metadata change
                    return discoveryNode.isMasterNode();
                }
            }
        );
    }

    @Override
    public void applyClusterState(ClusterChangedEvent event) {

    }

    @Override
    protected void doStart() {

    }

    @Override
    protected void doStop() {

    }

    @Override
    protected void doClose() throws IOException {

    }

    //TODO - Similarly we can create a registerRecommissionAttribute to recommission a zone
//    public void registerRecommissionAttribute(
//        final ClusterManagementRecommissionRequest request,
//        final ActionListener<ClusterStateUpdateResponse> listener
//    )

}
