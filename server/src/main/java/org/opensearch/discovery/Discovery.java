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

package org.opensearch.discovery;

import org.opensearch.cluster.NodeConnectionsService;
import org.opensearch.cluster.coordination.ClusterStatePublisher;
import org.opensearch.common.lifecycle.LifecycleComponent;

/**
 * A pluggable module allowing to implement discovery of other nodes, publishing of the cluster
 * state to all nodes, electing a cluster-manager of the cluster that raises cluster state change
 * events.
 *
 * @opensearch.internal
 */
public interface Discovery extends LifecycleComponent, ClusterStatePublisher {

    /**
     * @return stats about the discovery
     */
    DiscoveryStats stats();

    /**
     * Triggers the first join cycle
     */
    void startInitialJoin();

    /**
     * Sets the NodeConnectionsService which is an abstraction used for connection management
     */
    void setNodeConnectionsService(NodeConnectionsService nodeConnectionsService);
}
