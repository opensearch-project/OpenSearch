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
public class TransportConfigUpdateAction extends TransportNodesAction<
    ConfigUpdateRequest,
    ConfigUpdateResponse,
    TransportConfigUpdateAction.NodeConfigUpdateRequest,
    ConfigUpdateNodeResponse> {

    protected Logger logger = LogManager.getLogger(getClass());

    private final ConfigurationRepository configurationRepository;
    private DynamicConfigFactory dynamicConfigFactory;

    @Inject
    public TransportConfigUpdateAction(
        final Settings settings,
        final ThreadPool threadPool,
        final ClusterService clusterService,
        final TransportService transportService,
        final ConfigurationRepository configurationRepository,
        final ActionFilters actionFilters,
        DynamicConfigFactory dynamicConfigFactory
    ) {
        super(
            ConfigUpdateAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            ConfigUpdateRequest::new,
            TransportConfigUpdateAction.NodeConfigUpdateRequest::new,
            ThreadPool.Names.MANAGEMENT,
            ConfigUpdateNodeResponse.class
        );

        this.configurationRepository = configurationRepository;
        this.dynamicConfigFactory = dynamicConfigFactory;
    }

    public static class NodeConfigUpdateRequest extends TransportRequest {

        ConfigUpdateRequest request;

        public NodeConfigUpdateRequest(StreamInput in) throws IOException {
            super(in);
            request = new ConfigUpdateRequest(in);
        }

        public NodeConfigUpdateRequest(final ConfigUpdateRequest request) {
            this.request = request;
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            super.writeTo(out);
            request.writeTo(out);
        }
    }

    @Override
    protected ConfigUpdateNodeResponse newNodeResponse(StreamInput in) throws IOException {
        return new ConfigUpdateNodeResponse(in);
    }

    @Override
    protected ConfigUpdateResponse newResponse(
        ConfigUpdateRequest request,
        List<ConfigUpdateNodeResponse> responses,
        List<FailedNodeException> failures
    ) {
        return new ConfigUpdateResponse(this.clusterService.getClusterName(), responses, failures);

    }

    @Override
    protected ConfigUpdateNodeResponse nodeOperation(final NodeConfigUpdateRequest request) {
        configurationRepository.reloadConfiguration(CType.fromStringValues((request.request.getConfigTypes())));
        return new ConfigUpdateNodeResponse(clusterService.localNode(), request.request.getConfigTypes(), null);
    }

    @Override
    protected NodeConfigUpdateRequest newNodeRequest(ConfigUpdateRequest request) {
        return new NodeConfigUpdateRequest(request);
    }

}
