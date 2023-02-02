/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.rest.configuration;

import java.io.IOException;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.nodes.TransportNodesAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.identity.configuration.CType;
import org.opensearch.identity.configuration.ConfigurationRepository;
import org.opensearch.identity.configuration.DynamicConfigFactory;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportService;

/**
 * Updates and reloads configuration for this node
 */
public class TransportIdentityConfigUpdateAction extends TransportNodesAction<
    IdentityConfigUpdateRequest,
    IdentityConfigUpdateResponse,
    TransportIdentityConfigUpdateAction.NodeConfigUpdateRequest,
    IdentityConfigUpdateNodeResponse> {

    protected Logger logger = LogManager.getLogger(getClass());

    private final ConfigurationRepository configurationRepository;
    private DynamicConfigFactory dynamicConfigFactory;

    @Inject
    public TransportIdentityConfigUpdateAction(
        final Settings settings,
        final ThreadPool threadPool,
        final ClusterService clusterService,
        final TransportService transportService,
        final ConfigurationRepository configurationRepository,
        final ActionFilters actionFilters,
        DynamicConfigFactory dynamicConfigFactory
    ) {
        super(
            IdentityConfigUpdateAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            IdentityConfigUpdateRequest::new,
            TransportIdentityConfigUpdateAction.NodeConfigUpdateRequest::new,
            ThreadPool.Names.MANAGEMENT,
            IdentityConfigUpdateNodeResponse.class
        );

        this.configurationRepository = configurationRepository;
        this.dynamicConfigFactory = dynamicConfigFactory;
    }

    public static class NodeConfigUpdateRequest extends TransportRequest {

        IdentityConfigUpdateRequest request;

        public NodeConfigUpdateRequest(StreamInput in) throws IOException {
            super(in);
            request = new IdentityConfigUpdateRequest(in);
        }

        public NodeConfigUpdateRequest(final IdentityConfigUpdateRequest request) {
            this.request = request;
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            super.writeTo(out);
            request.writeTo(out);
        }
    }

    @Override
    protected IdentityConfigUpdateNodeResponse newNodeResponse(StreamInput in) throws IOException {
        return new IdentityConfigUpdateNodeResponse(in);
    }

    @Override
    protected IdentityConfigUpdateResponse newResponse(
        IdentityConfigUpdateRequest request,
        List<IdentityConfigUpdateNodeResponse> responses,
        List<FailedNodeException> failures
    ) {
        return new IdentityConfigUpdateResponse(this.clusterService.getClusterName(), responses, failures);

    }

    @Override
    protected IdentityConfigUpdateNodeResponse nodeOperation(final NodeConfigUpdateRequest request) {
        configurationRepository.reloadConfiguration(CType.fromStringValues((request.request.getConfigTypes())));
        return new IdentityConfigUpdateNodeResponse(clusterService.localNode(), request.request.getConfigTypes(), null);
    }

    @Override
    protected NodeConfigUpdateRequest newNodeRequest(IdentityConfigUpdateRequest request) {
        return new NodeConfigUpdateRequest(request);
    }

}
