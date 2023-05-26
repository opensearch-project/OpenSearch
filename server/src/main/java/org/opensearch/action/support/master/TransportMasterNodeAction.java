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

package org.opensearch.action.support.master;

import org.opensearch.action.ActionResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

/**
 * A base class for operations that needs to be performed on the cluster-manager node.
 *
 * @opensearch.internal
 * @deprecated As of 2.1, because supporting inclusive language, replaced by {@link TransportClusterManagerNodeAction}
 */
@Deprecated
public abstract class TransportMasterNodeAction<Request extends MasterNodeRequest<Request>, Response extends ActionResponse> extends
    TransportClusterManagerNodeAction<Request, Response> {

    protected TransportMasterNodeAction(
        String actionName,
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        Writeable.Reader<Request> request,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(actionName, true, transportService, clusterService, threadPool, actionFilters, request, indexNameExpressionResolver);
    }

    protected TransportMasterNodeAction(
        String actionName,
        boolean canTripCircuitBreaker,
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        Writeable.Reader<Request> request,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            actionName,
            canTripCircuitBreaker,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            request,
            indexNameExpressionResolver
        );
    }

}
