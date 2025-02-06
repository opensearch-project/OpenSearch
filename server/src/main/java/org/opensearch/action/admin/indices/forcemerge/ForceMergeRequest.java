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

package org.opensearch.action.admin.indices.forcemerge;

import org.opensearch.Version;
import org.opensearch.action.support.broadcast.BroadcastRequest;
import org.opensearch.common.UUIDs;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.index.engine.Engine;
import org.opensearch.transport.client.IndicesAdminClient;
import org.opensearch.transport.client.Requests;

import java.io.IOException;
import java.util.Arrays;

/**
 * A request to force merging the segments of one or more indices. In order to
 * run a merge on all the indices, pass an empty array or {@code null} for the
 * indices.
 * {@link #maxNumSegments(int)} allows to control the number of segments
 * to force merge down to. Defaults to simply checking if a merge needs
 * to execute, and if so, executes it
 *
 * @see Requests#forceMergeRequest(String...)
 * @see IndicesAdminClient#forceMerge(ForceMergeRequest)
 * @see ForceMergeResponse
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class ForceMergeRequest extends BroadcastRequest<ForceMergeRequest> {

    /**
     * Defaults for the Force Merge Request
     *
     * @opensearch.internal
     */
    public static final class Defaults {
        public static final int MAX_NUM_SEGMENTS = -1;
        public static final boolean ONLY_EXPUNGE_DELETES = false;
        public static final boolean FLUSH = true;
        public static final boolean PRIMARY_ONLY = false;
    }

    private int maxNumSegments = Defaults.MAX_NUM_SEGMENTS;
    private boolean onlyExpungeDeletes = Defaults.ONLY_EXPUNGE_DELETES;
    private boolean flush = Defaults.FLUSH;
    private boolean primaryOnly = Defaults.PRIMARY_ONLY;

    private static final Version FORCE_MERGE_UUID_VERSION = Version.V_3_0_0;

    /**
     * Force merge UUID to store in the live commit data of a shard under
     * {@link org.opensearch.index.engine.Engine#FORCE_MERGE_UUID_KEY} after force merging it.
     */
    private final String forceMergeUUID;

    private boolean shouldStoreResult;

    /**
     * Constructs a merge request over one or more indices.
     *
     * @param indices The indices to merge, no indices passed means all indices will be merged.
     */
    public ForceMergeRequest(String... indices) {
        super(indices);
        forceMergeUUID = UUIDs.randomBase64UUID();
    }

    public ForceMergeRequest(StreamInput in) throws IOException {
        super(in);
        maxNumSegments = in.readInt();
        onlyExpungeDeletes = in.readBoolean();
        flush = in.readBoolean();
        if (in.getVersion().onOrAfter(Version.V_2_13_0)) {
            primaryOnly = in.readBoolean();
        }
        if (in.getVersion().onOrAfter(FORCE_MERGE_UUID_VERSION)) {
            forceMergeUUID = in.readString();
        } else if ((forceMergeUUID = in.readOptionalString()) == null) {
            throw new IllegalStateException(
                "As of legacy version 7.7 [" + Engine.FORCE_MERGE_UUID_KEY + "] is no longer optional in force merge requests."
            );
        }
    }

    /**
     * Will merge the index down to &lt;= maxNumSegments. By default, will cause the merge
     * process to merge down to half the configured number of segments.
     */
    public int maxNumSegments() {
        return maxNumSegments;
    }

    /**
     * Will merge the index down to &lt;= maxNumSegments. By default, will cause the merge
     * process to merge down to half the configured number of segments.
     */
    public ForceMergeRequest maxNumSegments(int maxNumSegments) {
        this.maxNumSegments = maxNumSegments;
        return this;
    }

    /**
     * Should the merge only expunge deletes from the index, without full merging.
     * Defaults to full merging ({@code false}).
     */
    public boolean onlyExpungeDeletes() {
        return onlyExpungeDeletes;
    }

    /**
     * Should the merge only expunge deletes from the index, without full merge.
     * Defaults to full merging ({@code false}).
     */
    public ForceMergeRequest onlyExpungeDeletes(boolean onlyExpungeDeletes) {
        this.onlyExpungeDeletes = onlyExpungeDeletes;
        return this;
    }

    /**
     * Force merge UUID to use when force merging or {@code null} if not using one in a mixed version cluster containing nodes older than
     * {@link #FORCE_MERGE_UUID_VERSION}.
     */
    public String forceMergeUUID() {
        return forceMergeUUID;
    }

    /**
     * Should flush be performed after the merge. Defaults to {@code true}.
     */
    public boolean flush() {
        return flush;
    }

    /**
     * Should flush be performed after the merge. Defaults to {@code true}.
     */
    public ForceMergeRequest flush(boolean flush) {
        this.flush = flush;
        return this;
    }

    /**
     * Should force merge only performed on primary shards. Defaults to {@code false}.
     */
    public boolean primaryOnly() {
        return primaryOnly;
    }

    /**
     * Should force merge only performed on primary shards. Defaults to {@code false}.
     */
    public ForceMergeRequest primaryOnly(boolean primaryOnly) {
        this.primaryOnly = primaryOnly;
        return this;
    }

    /**
     * Should this task store its result after it has finished?
     */
    public void setShouldStoreResult(boolean shouldStoreResult) {
        this.shouldStoreResult = shouldStoreResult;
    }

    @Override
    public boolean getShouldStoreResult() {
        return shouldStoreResult;
    }

    @Override
    public String getDescription() {
        return "Force-merge indices "
            + Arrays.toString(indices())
            + ", maxSegments["
            + maxNumSegments
            + "], onlyExpungeDeletes["
            + onlyExpungeDeletes
            + "], flush["
            + flush
            + "], primaryOnly["
            + primaryOnly
            + "]";
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeInt(maxNumSegments);
        out.writeBoolean(onlyExpungeDeletes);
        out.writeBoolean(flush);
        if (out.getVersion().onOrAfter(Version.V_2_13_0)) {
            out.writeBoolean(primaryOnly);
        }
        if (out.getVersion().onOrAfter(FORCE_MERGE_UUID_VERSION)) {
            out.writeString(forceMergeUUID);
        } else {
            out.writeOptionalString(forceMergeUUID);
        }
    }

    @Override
    public String toString() {
        return "ForceMergeRequest{"
            + "maxNumSegments="
            + maxNumSegments
            + ", onlyExpungeDeletes="
            + onlyExpungeDeletes
            + ", flush="
            + flush
            + ", primaryOnly="
            + primaryOnly
            + '}';
    }
}
