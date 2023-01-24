/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.rest.service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.search.SearchTransportService;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.identity.rest.response.CreateUserResponse;
import org.opensearch.transport.TransportService;

/**
 * Service class for User related functions
 */
public class UserService {

    private static final Logger logger = LogManager.getLogger(UserService.class);

    private final ClusterService clusterService;
    private final SearchTransportService searchTransportService;
    private final TransportService transportService;
    private final NodeClient nodeClient;

    @Inject
    public UserService(
        ClusterService clusterService,
        SearchTransportService searchTransportService,
        TransportService transportService,
        NodeClient nodeClient
    ) {
        this.clusterService = clusterService;
        this.searchTransportService = searchTransportService;
        this.transportService = transportService;
        this.nodeClient = nodeClient;
    }

    public void createUser(String username, String password, ActionListener<CreateUserResponse> listener) {
        // TODO: Implement this
    }
}
