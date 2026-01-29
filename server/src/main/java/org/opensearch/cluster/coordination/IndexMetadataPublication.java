/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.cluster.coordination;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.OpenSearchException;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.coordination.ClusterStatePublisher.AckListener;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.transport.TransportException;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

/**
 * Publication task
 *
 * @opensearch.internal
 */
public abstract class IndexMetadataPublication {

    protected final Logger logger = LogManager.getLogger(getClass());

    private final List<PublicationTarget> publicationTargets;
    private final ClusterState targetPublishState;

    public IndexMetadataPublication(ClusterState clusterState) {
        publicationTargets = new ArrayList<>(clusterState.getNodes().getNodes().size());
        clusterState.getNodes()
            .clusterManagersFirstStream()
            .forEach(n -> publicationTargets.add(new PublicationTarget(n)));
        targetPublishState = clusterState;
    }

    public void start() {
        logger.debug("publishing latest IndexMetadata Versions to {}", publicationTargets);
        publicationTargets.forEach(PublicationTarget::sendPublishRequest);
    }


    protected abstract void sendPublishRequest(
        DiscoveryNode destination,
        ActionListener<IndexMetadataPublishResponse> responseActionListener
    );

    /**
     * A publication target.
     *
     * @opensearch.internal
     */
    class PublicationTarget {
        private final DiscoveryNode discoveryNode;
        private boolean ackIsPending = true;

        PublicationTarget(DiscoveryNode discoveryNode) {
            this.discoveryNode = discoveryNode;
        }


        @Override
        public String toString() {
            return "PublicationTarget{" + "discoveryNode=" + discoveryNode + ", ackIsPending=" + ackIsPending + '}';
        }

        void sendPublishRequest() {
            IndexMetadataPublication.this.sendPublishRequest(discoveryNode, new PublishResponseHandler());
        }

        /**
         * A handler for a publish response.
         *
         * @opensearch.internal
         */
        private class PublishResponseHandler implements ActionListener<IndexMetadataPublishResponse> {

            @Override
            public void onResponse(IndexMetadataPublishResponse response) {
            }

            @Override
            public void onFailure(Exception e) {
            }

        }
    }
}
