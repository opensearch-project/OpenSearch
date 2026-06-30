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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.client;

import org.opensearch.common.unit.TimeValue;

import static org.opensearch.common.unit.TimeValue.timeValueSeconds;

/**
 * A base request for any requests that supply timeouts.
 * <p>
 * Please note, any requests that use a ackTimeout should set timeout as they
 * represent the same backing field on the server.
 */
public abstract class TimedRequest implements Validatable {

    public static final TimeValue DEFAULT_ACK_TIMEOUT = timeValueSeconds(30);
    public static final TimeValue DEFAULT_CLUSTER_MANAGER_NODE_TIMEOUT = TimeValue.timeValueSeconds(30);

    private TimeValue timeout = DEFAULT_ACK_TIMEOUT;
    private TimeValue clusterManagerTimeout = DEFAULT_CLUSTER_MANAGER_NODE_TIMEOUT;

    /**
     * Sets the timeout to wait for the all the nodes to acknowledge
     * @param timeout timeout as a {@link TimeValue}
     */
    public void setTimeout(TimeValue timeout) {
        this.timeout = timeout;
    }

    /**
     * Sets the timeout to connect to the cluster-manager node
     * @param clusterManagerTimeout timeout as a {@link TimeValue}
     */
    public void setClusterManagerTimeout(TimeValue clusterManagerTimeout) {
        this.clusterManagerTimeout = clusterManagerTimeout;
    }

    /**
     * Returns the request timeout
     */
    public TimeValue timeout() {
        return timeout;
    }

    /**
     * Returns the timeout for the request to be completed on the cluster-manager node
     */
    public TimeValue clusterManagerNodeTimeout() {
        return clusterManagerTimeout;
    }
}
