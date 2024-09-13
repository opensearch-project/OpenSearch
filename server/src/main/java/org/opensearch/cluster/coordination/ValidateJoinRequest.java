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

import org.opensearch.cluster.ClusterState;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.transport.TransportRequest;

import java.io.IOException;

/**
 * Transport request to validate node join
 *
 * @opensearch.internal
 */
public class ValidateJoinRequest extends TransportRequest {
    private ClusterState state;

     boolean isRemoteStateEnabled;

    public ValidateJoinRequest(StreamInput in) throws IOException {
        super(in);
        this.state = ClusterState.readFrom(in, null);
        this.isRemoteStateEnabled = in.readBoolean();
    }

    public ValidateJoinRequest(ClusterState state) {
        this.state = state;
    }

    public ValidateJoinRequest(ClusterState state, boolean isRemoteStateEnabled) {
        this.state = state;
        this.isRemoteStateEnabled = isRemoteStateEnabled;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        this.state.writeTo(out);
        out.writeBoolean(this.isRemoteStateEnabled);
    }

    public ClusterState getState() {
        return state;
    }
}
