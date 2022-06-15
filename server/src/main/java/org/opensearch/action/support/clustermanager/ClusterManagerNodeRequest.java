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

package org.opensearch.action.support.clustermanager;

import org.opensearch.action.ActionRequest;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.unit.TimeValue;

import java.io.IOException;

/**
 * A based request for cluster-manager based operation.
 *
 * @opensearch.internal
 */
public abstract class ClusterManagerNodeRequest<Request extends ClusterManagerNodeRequest<Request>> extends ActionRequest {

    public static final TimeValue DEFAULT_MASTER_NODE_TIMEOUT = TimeValue.timeValueSeconds(30);

    protected TimeValue masterNodeTimeout = DEFAULT_MASTER_NODE_TIMEOUT;

    protected ClusterManagerNodeRequest() {}

    protected ClusterManagerNodeRequest(StreamInput in) throws IOException {
        super(in);
        masterNodeTimeout = in.readTimeValue();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeTimeValue(masterNodeTimeout);
    }

    /**
     * A timeout value in case the cluster-manager has not been discovered yet or disconnected.
     */
    @SuppressWarnings("unchecked")
    public final Request masterNodeTimeout(TimeValue timeout) {
        this.masterNodeTimeout = timeout;
        return (Request) this;
    }

    /**
     * A timeout value in case the cluster-manager has not been discovered yet or disconnected.
     */
    public final Request masterNodeTimeout(String timeout) {
        return masterNodeTimeout(TimeValue.parseTimeValue(timeout, null, getClass().getSimpleName() + ".masterNodeTimeout"));
    }

    public final TimeValue masterNodeTimeout() {
        return this.masterNodeTimeout;
    }
}
