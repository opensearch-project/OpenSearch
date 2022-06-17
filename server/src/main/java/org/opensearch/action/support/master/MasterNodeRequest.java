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

import org.opensearch.action.support.clustermanager.ClusterManagerNodeRequest;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.unit.TimeValue;

import java.io.IOException;

/**
 * A based request for cluster-manager based operation.
 *
 * @opensearch.internal
 * @deprecated As of 2.1, because supporting inclusive language, replaced by {@link ClusterManagerNodeRequest}
 */
@Deprecated
public abstract class MasterNodeRequest<Request extends MasterNodeRequest<Request>> extends ClusterManagerNodeRequest<Request> {

    @Deprecated
    public static final TimeValue DEFAULT_MASTER_NODE_TIMEOUT = DEFAULT_CLUSTER_MANAGER_NODE_TIMEOUT;

    @Deprecated
    protected TimeValue masterNodeTimeout = clusterManagerNodeTimeout;

    protected MasterNodeRequest(StreamInput in) throws IOException {
        super(in);
    }

    /**
     * A timeout value in case the cluster-manager has not been discovered yet or disconnected.
     */
    @SuppressWarnings("unchecked")
    @Deprecated
    public final Request masterNodeTimeout(TimeValue timeout) {
        return clusterManagerNodeTimeout(timeout);
    }

    /**
     * A timeout value in case the cluster-manager has not been discovered yet or disconnected.
     */
    @Deprecated
    public final Request masterNodeTimeout(String timeout) {
        return clusterManagerNodeTimeout(timeout);
    }
}
