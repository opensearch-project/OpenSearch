/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.cluster.AbstractDiffable;
import org.opensearch.cluster.Diff;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.collect.Tuple;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/**
 * Metadata for tracking shard split operations on an index.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class SplitShardsMetadata extends AbstractDiffable<SplitShardsMetadata> implements ToXContentFragment {
    private static final int MINIMUM_RANGE_LENGTH_THRESHOLD = 1000;

    private static final String KEY_ROOT_SHARDS_TO_ALL_CHILDREN = "root_shards_to_all_children";
    private static final String KEY_NUMBER_OF_ROOT_SHARDS = "num_of_root_shards";
    private static final String KEY_PARENT_TO_CHILD_SHARDS = "parent_to_child_shards";
    private static final String KEY_MAX_SHARD_ID = "max_shard_id";
    private static final String KEY_IN_PROGRESS_SPLIT_SHARD_IDS = "in_progress_split_shard_id";
    private static final String KEY_ACTIVE_SHARD_IDS = "active_shard_ids";

    // Following fields are upadated only after split completion and are used to service active shards request.
    // Root shard id to flat list of all child shards under root.
    private final ShardRange[][] rootShardsToAllChildren;
    private final int maxShardId;
    private final Set<Integer> activeShardIds;

    // Following fields can store temporary information about in progress child shards along with info about
    // split completed shards.
    // Mapping of a parent shard ID to children.
    private final Map<Integer, ShardRange[]> parentToChildShards;
    private final Set<Integer> inProgressSplitShardIds;

    SplitShardsMetadata(
        ShardRange[][] rootShardsToAllChildren,
        Map<Integer, ShardRange[]> parentToChildShards,
        Set<Integer> inProgressSplitShardIds,
        Set<Integer> activeShardIds,
        int maxShardId
    ) {

        this.rootShardsToAllChildren = rootShardsToAllChildren;
        this.parentToChildShards = Collections.unmodifiableMap(parentToChildShards);
        this.maxShardId = maxShardId;
        this.inProgressSplitShardIds = Collections.unmodifiableSet(inProgressSplitShardIds);
        this.activeShardIds = activeShardIds;
    }

    public SplitShardsMetadata(StreamInput in) throws IOException {
        int numberOfRootShards = in.readVInt();
        this.rootShardsToAllChildren = new ShardRange[numberOfRootShards][];
        for (int i = 0; i < numberOfRootShards; i++) {
            this.rootShardsToAllChildren[i] = in.readOptionalArray(ShardRange::new, ShardRange[]::new);
        }
        this.maxShardId = in.readVInt();
        this.inProgressSplitShardIds = Collections.unmodifiableSet(in.readSet(StreamInput::readInt));
        this.activeShardIds = Collections.unmodifiableSet(in.readSet(StreamInput::readInt));
        this.parentToChildShards = Collections.unmodifiableMap(
            in.readMap(StreamInput::readInt, i -> i.readArray(ShardRange::new, ShardRange[]::new))
        );
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(rootShardsToAllChildren.length);
        for (ShardRange[] rootShardsToAllChild : rootShardsToAllChildren) {
            out.writeOptionalArray(rootShardsToAllChild);
        }
        out.writeVInt(this.maxShardId);
        out.writeCollection(this.inProgressSplitShardIds, StreamOutput::writeInt);
        out.writeCollection(this.activeShardIds, StreamOutput::writeInt);
        out.writeMap(this.parentToChildShards, StreamOutput::writeInt, StreamOutput::writeArray);
    }

    public int getShardIdOfHash(int rootShardId, int hash, boolean includeInProgressChildren) {
        // First check if we have child shards against this root shard.
        if (rootShardsToAllChildren[rootShardId] == null) {
            if (includeInProgressChildren && parentToChildShards.containsKey(rootShardId)) {
                ShardRange shardRange = binarySearchShards(parentToChildShards.get(rootShardId), hash);
                assert shardRange != null;
                return shardRange.shardId();
            }
            return rootShardId;
        }

        ShardRange[] existingChildShards = rootShardsToAllChildren[rootShardId];
        ShardRange shardRange = binarySearchShards(existingChildShards, hash);
        assert shardRange != null;

        if (includeInProgressChildren && parentToChildShards.containsKey(shardRange.shardId())) {
            shardRange = binarySearchShards(parentToChildShards.get(shardRange.shardId()), hash);
        }
        assert shardRange != null;

        return shardRange.shardId();
    }

    private ShardRange binarySearchShards(ShardRange[] childShards, int hash) {
        int low = 0, high = childShards.length - 1;
        while (low <= high) {
            int mid = low + (high - low) / 2;
            ShardRange midShard = childShards[mid];
            if (midShard.contains(hash)) {
                return midShard;
            } else if (hash < midShard.start()) {
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }
        return null;
    }

    @Override
    public String toString() {
        StringBuilder parentToChildMap = new StringBuilder();
        for (Map.Entry<Integer, ShardRange[]> entry : parentToChildShards.entrySet()) {
            parentToChildMap.append("[");
            parentToChildMap.append(entry.getKey()).append("=").append(Arrays.toString(entry.getValue()));
            parentToChildMap.append("]");
        }
        return "SplitShardsMetadata{"
            + "rootShardsToAllChildren="
            + Arrays.toString(rootShardsToAllChildren)
            + ", maxShardId="
            + maxShardId
            + ", activeShardIds="
            + activeShardIds
            + ", parentToChildShards="
            + parentToChildMap
            + ", inProgressSplitShardIds="
            + inProgressSplitShardIds
            + '}';
    }

    public int getNumberOfRootShards() {
        return rootShardsToAllChildren.length;
    }

    public int getNumberOfShards() {
        return activeShardIds.size();
    }

    public List<Integer> getRootShards() {
        List<Integer> rootShardList = new ArrayList<>();
        for (int i = 0; i < rootShardsToAllChildren.length; i++) {
            rootShardList.add(i);
        }
        return rootShardList;
    }

    public ShardRange[] getChildShardsOfParent(int shardId) {
        if (parentToChildShards.containsKey(shardId) == false) {
            return null;
        }

        ShardRange[] childShards = new ShardRange[parentToChildShards.get(shardId).length];
        int childShardIdx = 0;
        for (ShardRange childShard : parentToChildShards.get(shardId)) {
            childShards[childShardIdx++] = childShard;
        }
        return childShards;
    }

    public Set<Integer> getChildShardIdsOfParent(int shardId) {
        Set<Integer> childShardIds = new HashSet<>();
        if (parentToChildShards.containsKey(shardId) == false) {
            return childShardIds;
        }

        for (ShardRange childShard : parentToChildShards.get(shardId)) {
            childShardIds.add(childShard.shardId());
        }
        return childShardIds;
    }

    public int inProgressChildShardsCount() {
        int total = 0;
        for (Integer parent : inProgressSplitShardIds) {
            total += parentToChildShards.get(parent).length;
        }
        return total;
    }

    public Iterator<Integer> getActiveShardIterator() {
        return new HashSet<>(activeShardIds).iterator();
    }

    // Visible for testing
    static void validateShardRanges(int shardId, ShardRange[] shardRanges) {
        Integer start = null;
        int lowerBound = Integer.MIN_VALUE;
        int upperBound = Integer.MAX_VALUE;
        for (ShardRange shardRange : shardRanges) {
            validateBounds(shardRange, start, lowerBound);
            long rangeEnd = shardRange.end();
            long rangeLength = rangeEnd - shardRange.start() + 1;
            if (rangeLength < MINIMUM_RANGE_LENGTH_THRESHOLD) {
                throw new IllegalArgumentException(
                    "Shard range from "
                        + shardRange.start()
                        + " to "
                        + shardRange.end()
                        + " is below shard range threshold of "
                        + MINIMUM_RANGE_LENGTH_THRESHOLD
                );
            }

            start = shardRange.end();
        }

        if (start == null) {
            throw new IllegalArgumentException("No shard range defined for child shards of shard " + shardId);
        }

        if (start != upperBound) {
            throw new IllegalArgumentException(
                "Shard range from " + (start + 1) + " to " + upperBound + " is missing from the list of shard ranges"
            );
        }
    }

    private static void validateBounds(ShardRange shardRange, Integer start, long parentStart) {
        if (start == null) {
            if (shardRange.start() != parentStart) {
                throw new IllegalArgumentException(
                    "Shard range from " + parentStart + " to " + (shardRange.start() - 1) + " is missing from the list of shard ranges"
                );
            }
        } else if (shardRange.start() != start + 1) {
            String errorMessage;
            if (shardRange.start() < start + 1) {
                errorMessage = "Shard range overlaps from " + shardRange.start() + " to " + start;
            } else {
                errorMessage = "Shard range from "
                    + (start + 1)
                    + " to "
                    + (shardRange.start() - 1)
                    + " is missing from the list of shard ranges";
            }
            throw new IllegalArgumentException(errorMessage);
        }
    }

    /**
     * Builder for {@link SplitShardsMetadata}.
     *
     * @opensearch.experimental
     */
    public static class Builder {
        private final ShardRange[][] rootShardsToAllChildren;
        private final Map<Integer, ShardRange[]> parentToChildShards;
        private int maxShardId;
        private final Set<Integer> inProgressSplitShardIds;
        private final Set<Integer> activeShardIds;

        public Builder(int numberOfShards) {
            maxShardId = numberOfShards - 1;
            rootShardsToAllChildren = new ShardRange[numberOfShards][];
            parentToChildShards = new HashMap<>();
            inProgressSplitShardIds = new HashSet<>();
            activeShardIds = new HashSet<>();
            for (int i = 0; i < numberOfShards; i++) {
                activeShardIds.add(i);
            }
        }

        public Builder(SplitShardsMetadata splitShardsMetadata) {
            this.maxShardId = splitShardsMetadata.maxShardId;

            this.rootShardsToAllChildren = new ShardRange[splitShardsMetadata.rootShardsToAllChildren.length][];
            Set<Integer> activeShardIds = new HashSet<>();
            for (int i = 0; i < splitShardsMetadata.rootShardsToAllChildren.length; i++) {
                if (splitShardsMetadata.rootShardsToAllChildren[i] != null) {
                    this.rootShardsToAllChildren[i] = new ShardRange[splitShardsMetadata.rootShardsToAllChildren[i].length];
                    int j = 0;
                    for (ShardRange childShard : splitShardsMetadata.rootShardsToAllChildren[i]) {
                        this.rootShardsToAllChildren[i][j++] = childShard;
                        activeShardIds.add(childShard.shardId());
                    }
                } else {
                    activeShardIds.add(i);
                }
            }

            this.parentToChildShards = new HashMap<>();
            for (Integer parentShardId : splitShardsMetadata.parentToChildShards.keySet()) {
                // Getting a copy of child shards for this parent.
                ShardRange[] childShards = splitShardsMetadata.getChildShardsOfParent(parentShardId);
                this.parentToChildShards.put(parentShardId, childShards);
            }

            inProgressSplitShardIds = new HashSet<>(splitShardsMetadata.inProgressSplitShardIds);
            this.activeShardIds = activeShardIds;
        }

        /**
         * Create metadata of new child shards for the provided shard id.
         * @param splitShardId Shard id to split
         * @param numberOfChildren Number of child shards this shard is going to have.
         */
        public List<ShardRange> splitShard(int splitShardId, int numberOfChildren) {
            if (inProgressSplitShardIds.contains(splitShardId) || parentToChildShards.containsKey(splitShardId)) {
                throw new IllegalArgumentException("Split of shard [" + splitShardId + "] is already in progress or completed.");
            }

            Tuple<Integer, ShardRange> shardTuple = findRootAndShard(splitShardId, rootShardsToAllChildren);
            if (shardTuple == null) {
                throw new IllegalArgumentException("Invalid shard id provided for splitting");
            }
            ShardRange parentShard = shardTuple.v2();

            long rangeSize = ((long) parentShard.end() - parentShard.start() + 1) / numberOfChildren;
            if (rangeSize <= MINIMUM_RANGE_LENGTH_THRESHOLD) {
                throw new IllegalArgumentException("Cannot split shard [" + splitShardId + "] further.");
            }

            Set<Integer> inProgressChildShardIds = getInProgressChildShardIds();
            inProgressSplitShardIds.add(splitShardId);

            List<Integer> allConsumedShardIds = new ArrayList<>();
            for (int i = 0; i < rootShardsToAllChildren.length; i++) {
                if (rootShardsToAllChildren[i] == null) {
                    allConsumedShardIds.add(i);
                }
            }
            for (Integer splitId : parentToChildShards.keySet()) {
                allConsumedShardIds.add(splitId);
                for (ShardRange shard : parentToChildShards.get(splitId)) {
                    allConsumedShardIds.add(shard.shardId());
                }
            }

            Set<Integer> shardIdHoles = findHoles(allConsumedShardIds);
            List<ShardRange> newChildShardsList = new ArrayList<>();
            long start = parentShard.start();

            int nextChildShardId = maxShardId, childShardId;
            for (int i = 0; i < numberOfChildren; ++i) {
                if (shardIdHoles.isEmpty()) {
                    nextChildShardId++;
                    while (inProgressChildShardIds.contains(nextChildShardId)) {
                        assert activeShardIds.contains(nextChildShardId) == false;
                        nextChildShardId++;
                    }
                    childShardId = nextChildShardId;
                } else {
                    childShardId = shardIdHoles.iterator().next();
                    shardIdHoles.remove(childShardId);
                    nextChildShardId = Math.max(nextChildShardId, childShardId);
                }
                assert !inProgressChildShardIds.contains(childShardId);
                assert !activeShardIds.contains(childShardId);
                inProgressChildShardIds.add(childShardId);

                long end = i == numberOfChildren - 1 ? parentShard.end() : start + rangeSize - 1;
                ShardRange childShard = new ShardRange(childShardId, (int) start, (int) end);
                newChildShardsList.add(childShard);
                start = end + 1;
            }

            ShardRange[] newShardRanges = newChildShardsList.toArray(new ShardRange[0]);

            // Get existing childShardRanges under rootShard
            List<ShardRange> shardsUnderRoot = rootShardsToAllChildren[shardTuple.v1()] == null
                ? new ArrayList<>()
                : new ArrayList<>(Arrays.asList(rootShardsToAllChildren[shardTuple.v1()]));
            shardsUnderRoot.remove(shardTuple.v2()); // Remove parent shard range
            shardsUnderRoot.addAll(List.of(newShardRanges)); // Add child shard range
            ShardRange[] newShardsUnderRoot = shardsUnderRoot.toArray(new ShardRange[0]);
            Arrays.sort(newShardsUnderRoot);

            validateShardRanges(splitShardId, newShardsUnderRoot);

            parentToChildShards.put(splitShardId, newShardRanges);
            return Collections.unmodifiableList(newChildShardsList);
        }

        private Set<Integer> findHoles(List<Integer> allConsumedShardIds) {
            Set<Integer> gaps = new TreeSet<>();

            if (allConsumedShardIds == null || allConsumedShardIds.size() <= 1) {
                return gaps;
            }

            Collections.sort(allConsumedShardIds);
            for (int i = 1; i < allConsumedShardIds.size(); i++) {
                int current = allConsumedShardIds.get(i);
                int previous = allConsumedShardIds.get(i - 1);

                if (current - previous > 1) {
                    for (int j = previous + 1; j < current; j++) {
                        gaps.add(j);
                    }
                }
            }

            return gaps;
        }

        public void updateSplitMetadataForChildShards(int sourceShardId, Set<Integer> newChildShardIds) {
            Tuple<Integer, ShardRange> shardRangeTuple = findRootAndShard(sourceShardId, rootShardsToAllChildren);

            assert inProgressSplitShardIds.contains(sourceShardId);
            assert newChildShardIds.size() == parentToChildShards.get(sourceShardId).length;
            for (ShardRange childShard : parentToChildShards.get(sourceShardId)) {
                assert newChildShardIds.contains(childShard.shardId());
            }

            List<ShardRange> shardsUnderRoot = rootShardsToAllChildren[shardRangeTuple.v1()] == null
                ? new ArrayList<>()
                : new ArrayList<>(Arrays.asList(rootShardsToAllChildren[shardRangeTuple.v1()]));
            shardsUnderRoot.remove(shardRangeTuple.v2());
            shardsUnderRoot.addAll(Arrays.asList(parentToChildShards.get(sourceShardId)));
            ShardRange[] newShardsUnderRoot = shardsUnderRoot.toArray(new ShardRange[0]);
            Arrays.sort(newShardsUnderRoot);
            validateShardRanges(shardRangeTuple.v1(), newShardsUnderRoot);

            int currentMaxShardId = maxShardId;
            for (Integer newChildId : newChildShardIds) {
                assert activeShardIds.contains(newChildId) == false;
                activeShardIds.add(newChildId);
                currentMaxShardId = Math.max(currentMaxShardId, newChildId);
            }

            activeShardIds.remove(sourceShardId);
            maxShardId = currentMaxShardId;
            rootShardsToAllChildren[shardRangeTuple.v1()] = newShardsUnderRoot;
            inProgressSplitShardIds.remove(sourceShardId);
        }

        private Set<Integer> getInProgressChildShardIds() {
            Set<Integer> inProgressChildShardIds = new HashSet<>();
            for (Integer inProgressParent : inProgressSplitShardIds) {
                assert parentToChildShards.containsKey(inProgressParent);
                ShardRange[] childShards = parentToChildShards.get(inProgressParent);
                for (ShardRange childShard : childShards) {
                    inProgressChildShardIds.add(childShard.shardId());
                }
            }
            return inProgressChildShardIds;
        }

        public void cancelSplit(int sourceShardId) {
            assert inProgressSplitShardIds.contains(sourceShardId);
            inProgressSplitShardIds.remove(sourceShardId);
            parentToChildShards.remove(sourceShardId);
        }

        public SplitShardsMetadata build() {
            return new SplitShardsMetadata(
                this.rootShardsToAllChildren,
                this.parentToChildShards,
                this.inProgressSplitShardIds,
                this.activeShardIds,
                this.maxShardId
            );
        }
    }

    private static Tuple<Integer, ShardRange> findRootAndShard(int shardId, ShardRange[][] rootShardsToAllChildren) {
        ShardRange[] allChildren;
        for (int rootShardId = 0; rootShardId < rootShardsToAllChildren.length; rootShardId++) {
            allChildren = rootShardsToAllChildren[rootShardId];
            if (allChildren != null) {
                for (ShardRange shardUnderRoot : allChildren) {
                    if (shardUnderRoot.shardId() == shardId) {
                        return new Tuple<>(rootShardId, shardUnderRoot);
                    }
                }
            }
        }

        if (shardId < rootShardsToAllChildren.length && rootShardsToAllChildren[shardId] == null) {
            // We are splitting a root shard in this case.
            return new Tuple<>(shardId, new ShardRange(shardId, Integer.MIN_VALUE, Integer.MAX_VALUE));
        }

        throw new IllegalArgumentException("Shard ID doesn't exist in the current list of shard ranges");
    }

    public Set<Integer> getInProgressSplitShardIds() {
        return inProgressSplitShardIds;
    }

    public boolean isSplitOfShardInProgress(int shardId) {
        return inProgressSplitShardIds.contains(shardId);
    }

    public boolean isSplitParent(int shardId) {
        return activeShardIds.contains(shardId) == false && parentToChildShards.containsKey(shardId);
    }

    public boolean isRecoveringChild(int shardId, int parentShardId) {
        if (!inProgressSplitShardIds.contains(parentShardId)) {
            return false;
        }

        for (ShardRange childShard : parentToChildShards.get(parentShardId)) {
            if (childShard.shardId() == shardId) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SplitShardsMetadata)) return false;

        SplitShardsMetadata that = (SplitShardsMetadata) o;

        if (maxShardId != that.maxShardId) return false;
        if (!inProgressSplitShardIds.equals(that.inProgressSplitShardIds)) return false;
        if (!Arrays.deepEquals(rootShardsToAllChildren, that.rootShardsToAllChildren)) return false;
        if (!activeShardIds.equals(that.activeShardIds)) return false;
        if (parentToChildShards.size() != that.parentToChildShards.size()) return false;
        for (Integer key : parentToChildShards.keySet()) {
            if (!Arrays.deepEquals(parentToChildShards.get(key), that.parentToChildShards.get(key))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = Arrays.deepHashCode(rootShardsToAllChildren);
        for (Map.Entry<Integer, ShardRange[]> entry : parentToChildShards.entrySet()) {
            result = 31 * result + Objects.hash(entry.getKey(), Arrays.deepHashCode(entry.getValue()));
        }
        result = 31 * result + maxShardId;
        result = 31 * result + inProgressSplitShardIds.hashCode();
        result = 31 * result + activeShardIds.hashCode();
        return result;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(KEY_NUMBER_OF_ROOT_SHARDS, rootShardsToAllChildren.length);
        builder.field(KEY_MAX_SHARD_ID, maxShardId);
        if (!inProgressSplitShardIds.isEmpty()) {
            builder.field(KEY_IN_PROGRESS_SPLIT_SHARD_IDS, new ArrayList<>(inProgressSplitShardIds));
        }
        builder.field(KEY_ACTIVE_SHARD_IDS, new ArrayList<>(activeShardIds));
        builder.startObject(KEY_ROOT_SHARDS_TO_ALL_CHILDREN);
        for (int rootShardId = 0; rootShardId < rootShardsToAllChildren.length; rootShardId++) {
            ShardRange[] childShards = rootShardsToAllChildren[rootShardId];
            if (childShards != null) {
                builder.startArray(String.valueOf(rootShardId));
                for (ShardRange childShard : childShards) {
                    childShard.toXContent(builder, params);
                }
                builder.endArray();
            }
        }
        builder.endObject();

        builder.startObject(KEY_PARENT_TO_CHILD_SHARDS);
        for (Integer parentShardId : parentToChildShards.keySet()) {
            builder.startArray(String.valueOf(parentShardId));
            for (ShardRange childShard : parentToChildShards.get(parentShardId)) {
                childShard.toXContent(builder, params);
            }
            builder.endArray();
        }
        builder.endObject();

        return builder;
    }

    public static SplitShardsMetadata parse(XContentParser parser) throws IOException {
        XContentParser.Token token;
        String currentFieldName = null;
        int maxShardId = -1;
        Set<Integer> inProgressSplitShardIds = new HashSet<>();
        Set<Integer> activeShardIds = new HashSet<>();
        ShardRange[][] rootShardsToAllChildren = null;
        Map<Integer, ShardRange[]> tempShardIdToChildShards = new HashMap<>();
        int numberOfRootShards = -1;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if (KEY_MAX_SHARD_ID.equals(currentFieldName)) {
                    maxShardId = parser.intValue();
                } else if (KEY_NUMBER_OF_ROOT_SHARDS.equals(currentFieldName)) {
                    numberOfRootShards = parser.intValue();
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (KEY_ROOT_SHARDS_TO_ALL_CHILDREN.equals(currentFieldName)) {
                    Map<Integer, ShardRange[]> rootShards = parseShardsMap(parser);
                    rootShardsToAllChildren = new ShardRange[numberOfRootShards][];
                    for (Map.Entry<Integer, ShardRange[]> entry : rootShards.entrySet()) {
                        rootShardsToAllChildren[entry.getKey()] = entry.getValue();
                    }
                } else if (KEY_PARENT_TO_CHILD_SHARDS.equals(currentFieldName)) {
                    tempShardIdToChildShards = parseShardsMap(parser);
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (KEY_IN_PROGRESS_SPLIT_SHARD_IDS.equals(currentFieldName)) {
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        inProgressSplitShardIds.add(parser.intValue());
                    }
                } else if (KEY_ACTIVE_SHARD_IDS.equals(currentFieldName)) {
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        activeShardIds.add(parser.intValue());
                    }
                }
            }
        }

        return new SplitShardsMetadata(
            rootShardsToAllChildren,
            tempShardIdToChildShards,
            inProgressSplitShardIds,
            activeShardIds,
            maxShardId
        );
    }

    private static Map<Integer, ShardRange[]> parseShardsMap(XContentParser parser) throws IOException {
        XContentParser.Token token;
        String currentFieldName = null;
        Map<Integer, ShardRange[]> shardsMap = new HashMap<>();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_ARRAY) {
                List<ShardRange> childShardRanges = new ArrayList<>();
                while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                    ShardRange shardRange = ShardRange.parse(parser);
                    childShardRanges.add(shardRange);
                }
                assert currentFieldName != null;
                Integer parentShard = Integer.parseInt(currentFieldName);
                shardsMap.put(parentShard, childShardRanges.toArray(new ShardRange[0]));
            }
        }

        return shardsMap;
    }

    public static Diff<SplitShardsMetadata> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(SplitShardsMetadata::new, in);
    }

}
