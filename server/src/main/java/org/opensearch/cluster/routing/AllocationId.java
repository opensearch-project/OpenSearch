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

package org.opensearch.cluster.routing;

import org.opensearch.Version;
import org.opensearch.common.Nullable;
import org.opensearch.common.UUIDs;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Uniquely identifies an allocation. An allocation is a shard moving from unassigned to initializing,
 * or relocation.
 * <p>
 * Relocation is a special case, where the origin shard is relocating with a relocationId and same id, and
 * the target shard (only materialized in RoutingNodes) is initializing with the id set to the origin shard
 * relocationId. Once relocation is done, the new allocation id is set to the relocationId. This is similar
 * behavior to how ShardRouting#currentNodeId is used.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class AllocationId implements ToXContentObject, Writeable {
    private static final String ID_KEY = "id";
    private static final String RELOCATION_ID_KEY = "relocation_id";
    private static final String SPLIT_CHILD_ALLOCATION_IDS = "split_child_allocation_ids";
    private static final String PARENT_ALLOCATION_ID = "parent_allocation_id";

    private static final ObjectParser<AllocationId.Builder, Void> ALLOCATION_ID_PARSER = new ObjectParser<>("allocationId");

    static {
        ALLOCATION_ID_PARSER.declareString(AllocationId.Builder::setId, new ParseField(ID_KEY));
        ALLOCATION_ID_PARSER.declareString(AllocationId.Builder::setRelocationId, new ParseField(RELOCATION_ID_KEY));
        ALLOCATION_ID_PARSER.declareStringArray(
            AllocationId.Builder::setSplitChildAllocationIds,
            new ParseField(SPLIT_CHILD_ALLOCATION_IDS)
        );
        ALLOCATION_ID_PARSER.declareString(AllocationId.Builder::setParentAllocationId, new ParseField(PARENT_ALLOCATION_ID));
    }

    private static class Builder {
        private String id;
        private String relocationId;
        private List<String> splitChildAllocationIds;
        private String parentAllocationId;

        public void setId(String id) {
            this.id = id;
        }

        public void setRelocationId(String relocationId) {
            this.relocationId = relocationId;
        }

        public void setSplitChildAllocationIds(List<String> splitChildAllocationIds) {
            this.splitChildAllocationIds = splitChildAllocationIds;
        }

        public void setParentAllocationId(String parentAllocationId) {
            this.parentAllocationId = parentAllocationId;
        }

        public AllocationId build() {
            return new AllocationId(id, relocationId, splitChildAllocationIds, parentAllocationId);
        }
    }

    private final String id;
    @Nullable
    private final String relocationId;
    @Nullable
    private final List<String> splitChildAllocationIds;
    @Nullable
    private final String parentAllocationId;

    AllocationId(StreamInput in) throws IOException {
        this.id = in.readString();
        this.relocationId = in.readOptionalString();
        if (in.getVersion().onOrAfter(Version.V_3_7_0)) {
            List<String> childIds = in.readOptionalStringList();
            splitChildAllocationIds = childIds == null ? null : Collections.unmodifiableList(childIds);
            parentAllocationId = in.readOptionalString();
        } else {
            splitChildAllocationIds = null;
            parentAllocationId = null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(this.id);
        out.writeOptionalString(this.relocationId);
        if (out.getVersion().onOrAfter(Version.V_3_7_0)) {
            out.writeOptionalStringCollection(splitChildAllocationIds);
            out.writeOptionalString(parentAllocationId);
        }
    }

    private AllocationId(String id, String relocationId, List<String> splitChildAllocationIds, String parentAllocationId) {
        Objects.requireNonNull(id, "Argument [id] must be non-null");
        this.id = id;
        this.relocationId = relocationId;
        this.splitChildAllocationIds = splitChildAllocationIds == null ? null : Collections.unmodifiableList(splitChildAllocationIds);
        this.parentAllocationId = parentAllocationId;
    }

    /**
     * Creates a new allocation id for initializing allocation.
     */
    public static AllocationId newInitializing() {
        return new AllocationId(UUIDs.randomBase64UUID(), null, null, null);
    }

    /**
     * Creates a new allocation id for initializing allocation based on an existing id.
     */
    public static AllocationId newInitializing(String existingAllocationId) {
        return new AllocationId(existingAllocationId, null, null, null);
    }

    /**
     * Creates a new allocation id for the target initializing shard that is the result
     * of a relocation.
     */
    public static AllocationId newTargetRelocation(AllocationId allocationId) {
        assert allocationId.getRelocationId() != null;
        return new AllocationId(allocationId.getRelocationId(), allocationId.getId(), null, null);
    }

    /**
     * Creates a new allocation id for a shard that moves to be relocated, populating
     * the transient holder for relocationId.
     */
    public static AllocationId newRelocation(AllocationId allocationId) {
        assert allocationId.getRelocationId() == null;
        return new AllocationId(allocationId.getId(), UUIDs.randomBase64UUID(), null, null);
    }

    /**
     * Creates a new allocation id representing a cancelled relocation.
     * <p>
     * Note that this is expected to be called on the allocation id
     * of the *source* shard.
     */
    public static AllocationId cancelRelocation(AllocationId allocationId) {
        assert allocationId.getRelocationId() != null;
        return new AllocationId(allocationId.getId(), null, null, null);
    }

    /**
     * Creates a new allocation id finalizing a relocation.
     * <p>
     * Note that this is expected to be called on the allocation id
     * of the *target* shard and thus it only needs to clear the relocating id.
     */
    public static AllocationId finishRelocation(AllocationId allocationId) {
        assert allocationId.getRelocationId() != null;
        return new AllocationId(allocationId.getId(), null, null, null);
    }

    /**
     * Creates a new allocation id for a shard that is undergoing split, populating
     * the transient holder for splitChildAllocationIds.
     */
    public static AllocationId newSplit(AllocationId allocationId, int numberOfChildShards) {
        assert allocationId.getSplitChildAllocationIds() == null && allocationId.getParentAllocationId() == null;
        List<String> childIds = new ArrayList<>();
        for (int c = 0; c < numberOfChildShards; c++) {
            childIds.add(UUIDs.randomBase64UUID());
        }
        return new AllocationId(allocationId.getId(), null, childIds, null);
    }

    /**
     * Creates a new allocation id for a child shard that is the result of a split.
     */
    public static AllocationId newTargetSplit(AllocationId allocationId, String childAllocId) {
        assert allocationId.getSplitChildAllocationIds() != null;
        return new AllocationId(childAllocId, null, null, allocationId.getId());
    }

    /**
     * Creates a new allocation id representing a cancelled split.
     */
    public static AllocationId cancelSplit(AllocationId allocationId) {
        assert allocationId.getSplitChildAllocationIds() != null;
        return new AllocationId(allocationId.getId(), null, null, null);
    }

    /**
     * Creates a new allocation id finalizing a split on the target child shard.
     */
    public static AllocationId finishSplit(AllocationId allocationId) {
        assert allocationId.getParentAllocationId() != null;
        return new AllocationId(allocationId.getId(), null, null, null);
    }

    /**
     * The allocation id uniquely identifying an allocation, note, if it is relocation
     * the {@link #getRelocationId()} need to be taken into account as well.
     */
    public String getId() {
        return id;
    }

    /**
     * The transient relocation id holding the unique id that is used for relocation.
     */
    public String getRelocationId() {
        return relocationId;
    }

    /**
     * The transient split child allocation ids holding the unique ids which are used for split.
     */
    public List<String> getSplitChildAllocationIds() {
        return splitChildAllocationIds;
    }

    /**
     * The transient split parent allocation id holding the unique id that is used for split.
     */
    public String getParentAllocationId() {
        return parentAllocationId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AllocationId)) return false;
        AllocationId that = (AllocationId) o;
        return Objects.equals(id, that.id)
            && Objects.equals(relocationId, that.relocationId)
            && Objects.equals(splitChildAllocationIds, that.splitChildAllocationIds)
            && Objects.equals(parentAllocationId, that.parentAllocationId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, relocationId, splitChildAllocationIds, parentAllocationId);
    }

    @Override
    public String toString() {
        return "[id="
            + id
            + (relocationId == null ? "" : ", rId=" + relocationId)
            + (splitChildAllocationIds == null ? "" : ", splitChildIds=" + splitChildAllocationIds)
            + (parentAllocationId == null ? "" : ", parentId=" + parentAllocationId)
            + "]";
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ID_KEY, id);
        if (relocationId != null) {
            builder.field(RELOCATION_ID_KEY, relocationId);
        }
        if (splitChildAllocationIds != null) {
            builder.startArray(SPLIT_CHILD_ALLOCATION_IDS);
            for (String childId : splitChildAllocationIds) {
                builder.value(childId);
            }
            builder.endArray();
        }
        if (parentAllocationId != null) {
            builder.field(PARENT_ALLOCATION_ID, parentAllocationId);
        }
        builder.endObject();
        return builder;
    }

    public static AllocationId fromXContent(XContentParser parser) throws IOException {
        return ALLOCATION_ID_PARSER.parse(parser, new AllocationId.Builder(), null).build();
    }
}
