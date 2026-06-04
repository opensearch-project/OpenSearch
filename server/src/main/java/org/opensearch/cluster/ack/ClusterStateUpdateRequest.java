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

package org.opensearch.cluster.ack;

import org.opensearch.common.unit.TimeValue;

/**
 * Base class to be used when needing to update the cluster state
 * Contains the basic fields that are always needed
 *
 * @opensearch.internal
 */
public abstract class ClusterStateUpdateRequest<T extends ClusterStateUpdateRequest<T>> implements AckedRequest {

    private TimeValue ackTimeout;
    private TimeValue clusterManagerNodeTimeout;

    /**
     * Returns the maximum time interval to wait for acknowledgements
     */
    @Override
    public TimeValue ackTimeout() {
        return ackTimeout;
    }

    /**
     * Sets the acknowledgement timeout
     */
    @SuppressWarnings("unchecked")
    public T ackTimeout(TimeValue ackTimeout) {
        this.ackTimeout = ackTimeout;
        return (T) this;
    }

    /**
     * Returns the maximum time interval to wait for the request to
     * be completed on the cluster-manager node
     */
    @Override
    public TimeValue clusterManagerNodeTimeout() {
        return clusterManagerNodeTimeout;
    }

    /**
     * Sets the cluster-manager node timeout
     */
    @SuppressWarnings("unchecked")
    public T clusterManagerNodeTimeout(TimeValue clusterManagerNodeTimeout) {
        this.clusterManagerNodeTimeout = clusterManagerNodeTimeout;
        return (T) this;
    }
}
