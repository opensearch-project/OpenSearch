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

package org.opensearch.action.admin.cluster.health;

import org.opensearch.Version;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.IndicesRequest;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeReadRequest;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.common.Priority;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.unit.TimeValue;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static org.opensearch.action.ValidateActions.addValidationError;

/**
 * Transport request for requesting cluster health
 *
 * @opensearch.internal
 */
public class ClusterHealthRequest extends ClusterManagerNodeReadRequest<ClusterHealthRequest> implements IndicesRequest.Replaceable {

    private String[] indices;
    private String awarenessAttribute;
    private IndicesOptions indicesOptions = IndicesOptions.lenientExpandHidden();
    private TimeValue timeout = new TimeValue(30, TimeUnit.SECONDS);
    private ClusterHealthStatus waitForStatus;
    private boolean waitForNoRelocatingShards = false;
    private boolean waitForNoInitializingShards = false;
    private ActiveShardCount waitForActiveShards = ActiveShardCount.NONE;
    private String waitForNodes = "";
    private Priority waitForEvents = null;
    private boolean ensureNodeWeighedIn = false;
    /**
     * Only used by the high-level REST Client. Controls the details level of the health information returned.
     * The default value is 'cluster'.
     */
    private Level level = Level.CLUSTER;

    public ClusterHealthRequest() {}

    public ClusterHealthRequest(String... indices) {
        this.indices = indices;
    }

    public ClusterHealthRequest(StreamInput in) throws IOException {
        super(in);
        indices = in.readStringArray();
        timeout = in.readTimeValue();
        if (in.readBoolean()) {
            waitForStatus = ClusterHealthStatus.fromValue(in.readByte());
        }
        waitForNoRelocatingShards = in.readBoolean();
        waitForActiveShards = ActiveShardCount.readFrom(in);
        waitForNodes = in.readString();
        if (in.readBoolean()) {
            waitForEvents = Priority.readFrom(in);
        }
        waitForNoInitializingShards = in.readBoolean();
        indicesOptions = IndicesOptions.readIndicesOptions(in);
        if (in.getVersion().onOrAfter(Version.V_2_5_0)) {
            awarenessAttribute = in.readOptionalString();
            level = in.readEnum(Level.class);
        }
        if (in.getVersion().onOrAfter(Version.V_2_6_0)) {
            ensureNodeWeighedIn = in.readBoolean();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (indices == null) {
            out.writeVInt(0);
        } else {
            out.writeStringArray(indices);
        }
        out.writeTimeValue(timeout);
        if (waitForStatus == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeByte(waitForStatus.value());
        }
        out.writeBoolean(waitForNoRelocatingShards);
        waitForActiveShards.writeTo(out);
        out.writeString(waitForNodes);
        if (waitForEvents == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            Priority.writeTo(waitForEvents, out);
        }
        out.writeBoolean(waitForNoInitializingShards);
        indicesOptions.writeIndicesOptions(out);
        if (out.getVersion().onOrAfter(Version.V_2_5_0)) {
            out.writeOptionalString(awarenessAttribute);
            out.writeEnum(level);
        }
        if (out.getVersion().onOrAfter(Version.V_2_6_0)) {
            out.writeBoolean(ensureNodeWeighedIn);
        }
    }

    @Override
    public String[] indices() {
        return indices;
    }

    @Override
    public ClusterHealthRequest indices(String... indices) {
        this.indices = indices;
        return this;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    public ClusterHealthRequest indicesOptions(final IndicesOptions indicesOptions) {
        this.indicesOptions = indicesOptions;
        return this;
    }

    @Override
    public boolean includeDataStreams() {
        return true;
    }

    public TimeValue timeout() {
        return timeout;
    }

    public ClusterHealthRequest timeout(TimeValue timeout) {
        this.timeout = timeout;
        if (clusterManagerNodeTimeout == DEFAULT_CLUSTER_MANAGER_NODE_TIMEOUT) {
            clusterManagerNodeTimeout = timeout;
        }
        return this;
    }

    public ClusterHealthRequest timeout(String timeout) {
        return this.timeout(TimeValue.parseTimeValue(timeout, null, getClass().getSimpleName() + ".timeout"));
    }

    public ClusterHealthStatus waitForStatus() {
        return waitForStatus;
    }

    public ClusterHealthRequest waitForStatus(ClusterHealthStatus waitForStatus) {
        this.waitForStatus = waitForStatus;
        return this;
    }

    public ClusterHealthRequest waitForGreenStatus() {
        return waitForStatus(ClusterHealthStatus.GREEN);
    }

    public ClusterHealthRequest waitForYellowStatus() {
        return waitForStatus(ClusterHealthStatus.YELLOW);
    }

    public boolean waitForNoRelocatingShards() {
        return waitForNoRelocatingShards;
    }

    /**
     * Sets whether the request should wait for there to be no relocating shards before
     * retrieving the cluster health status.  Defaults to {@code false}, meaning the
     * operation does not wait on there being no more relocating shards.  Set to <code>true</code>
     * to wait until the number of relocating shards in the cluster is 0.
     */
    public ClusterHealthRequest waitForNoRelocatingShards(boolean waitForNoRelocatingShards) {
        this.waitForNoRelocatingShards = waitForNoRelocatingShards;
        return this;
    }

    public boolean waitForNoInitializingShards() {
        return waitForNoInitializingShards;
    }

    /**
     * Sets whether the request should wait for there to be no initializing shards before
     * retrieving the cluster health status.  Defaults to {@code false}, meaning the
     * operation does not wait on there being no more initializing shards.  Set to <code>true</code>
     * to wait until the number of initializing shards in the cluster is 0.
     */
    public ClusterHealthRequest waitForNoInitializingShards(boolean waitForNoInitializingShards) {
        this.waitForNoInitializingShards = waitForNoInitializingShards;
        return this;
    }

    public ActiveShardCount waitForActiveShards() {
        return waitForActiveShards;
    }

    /**
     * Sets the number of shard copies that must be active across all indices before getting the
     * health status. Defaults to {@link ActiveShardCount#NONE}, meaning we don't wait on any active shards.
     * Set this value to {@link ActiveShardCount#ALL} to wait for all shards (primary and
     * all replicas) to be active across all indices in the cluster. Otherwise, use
     * {@link ActiveShardCount#from(int)} to set this value to any non-negative integer, up to the
     * total number of shard copies to wait for.
     */
    public ClusterHealthRequest waitForActiveShards(ActiveShardCount waitForActiveShards) {
        if (waitForActiveShards.equals(ActiveShardCount.DEFAULT)) {
            // the default for cluster health request is 0, not 1
            this.waitForActiveShards = ActiveShardCount.NONE;
        } else {
            this.waitForActiveShards = waitForActiveShards;
        }
        return this;
    }

    /**
     * A shortcut for {@link #waitForActiveShards(ActiveShardCount)} where the numerical
     * shard count is passed in, instead of having to first call {@link ActiveShardCount#from(int)}
     * to get the ActiveShardCount.
     */
    public ClusterHealthRequest waitForActiveShards(final int waitForActiveShards) {
        return waitForActiveShards(ActiveShardCount.from(waitForActiveShards));
    }

    public String waitForNodes() {
        return waitForNodes;
    }

    /**
     * Waits for N number of nodes. Use "12" for exact mapping, "&gt;12" and "&lt;12" for range.
     */
    public ClusterHealthRequest waitForNodes(String waitForNodes) {
        this.waitForNodes = waitForNodes;
        return this;
    }

    public ClusterHealthRequest waitForEvents(Priority waitForEvents) {
        this.waitForEvents = waitForEvents;
        return this;
    }

    public Priority waitForEvents() {
        return this.waitForEvents;
    }

    /**
     * Set the level of detail for the health information to be returned.
     * Only used by the high-level REST Client.
     */
    public void level(Level level) {
        this.level = Objects.requireNonNull(level, "level must not be null");
    }

    public void setLevel(String level) {
        switch (level) {
            case "indices":
                level(ClusterHealthRequest.Level.INDICES);
                break;
            case "shards":
                level(ClusterHealthRequest.Level.SHARDS);
                break;
            case "awareness_attributes":
                level(ClusterHealthRequest.Level.AWARENESS_ATTRIBUTES);
                break;
            default:
                level(ClusterHealthRequest.Level.CLUSTER);
        }
    }

    /**
     * Get the level of detail for the health information to be returned.
     * Only used by the high-level REST Client.
     */
    public Level level() {
        return level;
    }

    public ClusterHealthRequest setAwarenessAttribute(String awarenessAttribute) {
        this.awarenessAttribute = awarenessAttribute;
        return this;
    }

    public String getAwarenessAttribute() {
        return awarenessAttribute;
    }

    public final ClusterHealthRequest ensureNodeWeighedIn(boolean ensureNodeWeighedIn) {
        this.ensureNodeWeighedIn = ensureNodeWeighedIn;
        return this;
    }

    /**
     * For a given local request, checks if the local node is commissioned or not (default: false).
     * @return <code>true</code> if local information is to be returned only when local node is also commissioned
     * <code>false</code> to not check local node if commissioned or not for a local request
     */
    public final boolean ensureNodeWeighedIn() {
        return ensureNodeWeighedIn;
    }

    @Override
    public ActionRequestValidationException validate() {
        if (level.equals(Level.AWARENESS_ATTRIBUTES) && indices.length > 0) {
            return addValidationError("awareness_attribute is not a supported parameter with index health", null);
        } else if (!level.equals(Level.AWARENESS_ATTRIBUTES) && awarenessAttribute != null) {
            return addValidationError("level=awareness_attributes is required with awareness_attribute parameter", null);
        }
        if (ensureNodeWeighedIn && local == false) {
            return addValidationError("not a local request to ensure local node commissioned or weighed in", null);
        }
        return null;
    }

    /**
     * The level of the health request.
     *
     * @opensearch.internal
     */
    public enum Level {
        CLUSTER,
        INDICES,
        SHARDS,
        AWARENESS_ATTRIBUTES
    }
}
