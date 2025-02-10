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

package org.opensearch.action.admin.cluster.reroute;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.clustermanager.AcknowledgedRequest;
import org.opensearch.cluster.routing.allocation.command.AllocationCommand;
import org.opensearch.cluster.routing.allocation.command.AllocationCommands;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Objects;

/**
 * Request to submit cluster reroute allocation commands
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class ClusterRerouteRequest extends AcknowledgedRequest<ClusterRerouteRequest> {
    private AllocationCommands commands = new AllocationCommands();
    private boolean dryRun;
    private boolean explain;
    private boolean retryFailed;

    public ClusterRerouteRequest(StreamInput in) throws IOException {
        super(in);
        commands = AllocationCommands.readFrom(in);
        dryRun = in.readBoolean();
        explain = in.readBoolean();
        retryFailed = in.readBoolean();
    }

    public ClusterRerouteRequest() {}

    /**
     * Adds allocation commands to be applied to the cluster. Note, can be empty, in which case
     * will simply run a simple "reroute".
     */
    public ClusterRerouteRequest add(AllocationCommand... commands) {
        this.commands.add(commands);
        return this;
    }

    /**
     * Sets a dry run flag (defaults to {@code false}) allowing to run the commands without
     * actually applying them to the cluster state, and getting the resulting cluster state back.
     */
    public ClusterRerouteRequest dryRun(boolean dryRun) {
        this.dryRun = dryRun;
        return this;
    }

    /**
     * Returns the current dry run flag which allows to run the commands without actually applying them,
     * just to get back the resulting cluster state back.
     */
    public boolean dryRun() {
        return this.dryRun;
    }

    /**
     * Sets the explain flag, which will collect information about the reroute
     * request without executing the actions. Similar to dryRun,
     * but human-readable.
     */
    public ClusterRerouteRequest explain(boolean explain) {
        this.explain = explain;
        return this;
    }

    /**
     * Sets the retry failed flag (defaults to {@code false}). If true, the
     * request will retry allocating shards that can't currently be allocated due to too many allocation failures.
     */
    public ClusterRerouteRequest setRetryFailed(boolean retryFailed) {
        this.retryFailed = retryFailed;
        return this;
    }

    /**
     * Returns the current explain flag
     */
    public boolean explain() {
        return this.explain;
    }

    /**
     * Returns the current retry failed flag
     */
    public boolean isRetryFailed() {
        return this.retryFailed;
    }

    /**
     * Set the allocation commands to execute.
     */
    public ClusterRerouteRequest commands(AllocationCommands commands) {
        this.commands = commands;
        return this;
    }

    /**
     * Returns the allocation commands to execute
     */
    public AllocationCommands getCommands() {
        return commands;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        AllocationCommands.writeTo(commands, out);
        out.writeBoolean(dryRun);
        out.writeBoolean(explain);
        out.writeBoolean(retryFailed);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        ClusterRerouteRequest other = (ClusterRerouteRequest) obj;
        // Override equals and hashCode for testing
        return Objects.equals(commands, other.commands)
            && Objects.equals(dryRun, other.dryRun)
            && Objects.equals(explain, other.explain)
            && Objects.equals(timeout, other.timeout)
            && Objects.equals(retryFailed, other.retryFailed)
            && Objects.equals(clusterManagerNodeTimeout, other.clusterManagerNodeTimeout);
    }

    @Override
    public int hashCode() {
        // Override equals and hashCode for testing
        return Objects.hash(commands, dryRun, explain, timeout, retryFailed, clusterManagerNodeTimeout);
    }
}
