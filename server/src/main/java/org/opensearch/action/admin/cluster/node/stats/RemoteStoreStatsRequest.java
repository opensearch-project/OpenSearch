/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.node.stats;

import org.opensearch.action.admin.indices.stats.CommonStatsFlags;
import org.opensearch.action.support.broadcast.BroadcastRequest;
import org.opensearch.action.support.nodes.BaseNodesRequest;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class RemoteStoreStatsRequest extends BroadcastRequest<RemoteStoreStatsRequest> {

    private CommonStatsFlags flags = new CommonStatsFlags();

    public RemoteStoreStatsRequest() {
        super((String[]) null);
    }

    public RemoteStoreStatsRequest(StreamInput in) throws IOException {
        super(in);
        flags = new CommonStatsFlags(in);
    }

    /**
     * Sets all flags to return all stats.
     */
    public RemoteStoreStatsRequest all() {
        flags.all();
        return this;
    }

    /**
     * Clears all stats.
     */
    public RemoteStoreStatsRequest clear() {
        flags.clear();
        return this;
    }

    /**
     * Returns the underlying stats flags.
     */
    public CommonStatsFlags flags() {
        return flags;
    }

    /**
     * Sets the underlying stats flags.
     */
    public RemoteStoreStatsRequest flags(CommonStatsFlags flags) {
        this.flags = flags;
        return this;
    }

    /**
     * Sets specific search group stats to retrieve the stats for. Mainly affects search
     * when enabled.
     */
    public RemoteStoreStatsRequest groups(String... groups) {
        flags.groups(groups);
        return this;
    }

    public String[] groups() {
        return this.flags.groups();
    }

    public RemoteStoreStatsRequest docs(boolean docs) {
        flags.set(CommonStatsFlags.Flag.Docs, docs);
        return this;
    }

    public boolean docs() {
        return flags.isSet(CommonStatsFlags.Flag.Docs);
    }

    public RemoteStoreStatsRequest store(boolean store) {
        flags.set(CommonStatsFlags.Flag.Store, store);
        return this;
    }

    public boolean store() {
        return flags.isSet(CommonStatsFlags.Flag.Store);
    }

    public RemoteStoreStatsRequest indexing(boolean indexing) {
        flags.set(CommonStatsFlags.Flag.Indexing, indexing);

        return this;
    }

    public boolean indexing() {
        return flags.isSet(CommonStatsFlags.Flag.Indexing);
    }

    public RemoteStoreStatsRequest get(boolean get) {
        flags.set(CommonStatsFlags.Flag.Get, get);
        return this;
    }

    public boolean get() {
        return flags.isSet(CommonStatsFlags.Flag.Get);
    }

    public RemoteStoreStatsRequest search(boolean search) {
        flags.set(CommonStatsFlags.Flag.Search, search);
        return this;
    }

    public boolean search() {
        return flags.isSet(CommonStatsFlags.Flag.Search);
    }

    public RemoteStoreStatsRequest merge(boolean merge) {
        flags.set(CommonStatsFlags.Flag.Merge, merge);
        return this;
    }

    public boolean merge() {
        return flags.isSet(CommonStatsFlags.Flag.Merge);
    }

    public RemoteStoreStatsRequest refresh(boolean refresh) {
        flags.set(CommonStatsFlags.Flag.Refresh, refresh);
        return this;
    }

    public boolean refresh() {
        return flags.isSet(CommonStatsFlags.Flag.Refresh);
    }

    public RemoteStoreStatsRequest flush(boolean flush) {
        flags.set(CommonStatsFlags.Flag.Flush, flush);
        return this;
    }

    public boolean flush() {
        return flags.isSet(CommonStatsFlags.Flag.Flush);
    }

    public RemoteStoreStatsRequest warmer(boolean warmer) {
        flags.set(CommonStatsFlags.Flag.Warmer, warmer);
        return this;
    }

    public boolean warmer() {
        return flags.isSet(CommonStatsFlags.Flag.Warmer);
    }

    public RemoteStoreStatsRequest queryCache(boolean queryCache) {
        flags.set(CommonStatsFlags.Flag.QueryCache, queryCache);
        return this;
    }

    public boolean queryCache() {
        return flags.isSet(CommonStatsFlags.Flag.QueryCache);
    }

    public RemoteStoreStatsRequest fieldData(boolean fieldData) {
        flags.set(CommonStatsFlags.Flag.FieldData, fieldData);
        return this;
    }

    public boolean fieldData() {
        return flags.isSet(CommonStatsFlags.Flag.FieldData);
    }

    public RemoteStoreStatsRequest segments(boolean segments) {
        flags.set(CommonStatsFlags.Flag.Segments, segments);
        return this;
    }

    public boolean segments() {
        return flags.isSet(CommonStatsFlags.Flag.Segments);
    }

    public RemoteStoreStatsRequest fieldDataFields(String... fieldDataFields) {
        flags.fieldDataFields(fieldDataFields);
        return this;
    }

    public String[] fieldDataFields() {
        return flags.fieldDataFields();
    }

    public RemoteStoreStatsRequest completion(boolean completion) {
        flags.set(CommonStatsFlags.Flag.Completion, completion);
        return this;
    }

    public boolean completion() {
        return flags.isSet(CommonStatsFlags.Flag.Completion);
    }

    public RemoteStoreStatsRequest completionFields(String... completionDataFields) {
        flags.completionDataFields(completionDataFields);
        return this;
    }

    public String[] completionFields() {
        return flags.completionDataFields();
    }

    public RemoteStoreStatsRequest translog(boolean translog) {
        flags.set(CommonStatsFlags.Flag.Translog, translog);
        return this;
    }

    public boolean translog() {
        return flags.isSet(CommonStatsFlags.Flag.Translog);
    }

    public RemoteStoreStatsRequest requestCache(boolean requestCache) {
        flags.set(CommonStatsFlags.Flag.RequestCache, requestCache);
        return this;
    }

    public boolean requestCache() {
        return flags.isSet(CommonStatsFlags.Flag.RequestCache);
    }

    public RemoteStoreStatsRequest recovery(boolean recovery) {
        flags.set(CommonStatsFlags.Flag.Recovery, recovery);
        return this;
    }

    public boolean recovery() {
        return flags.isSet(CommonStatsFlags.Flag.Recovery);
    }

    public boolean includeSegmentFileSizes() {
        return flags.includeSegmentFileSizes();
    }

    public RemoteStoreStatsRequest includeSegmentFileSizes(boolean includeSegmentFileSizes) {
        flags.includeSegmentFileSizes(includeSegmentFileSizes);
        return this;
    }

    public RemoteStoreStatsRequest includeUnloadedSegments(boolean includeUnloadedSegments) {
        flags.includeUnloadedSegments(includeUnloadedSegments);
        return this;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        flags.writeTo(out);
    }

    @Override
    public boolean includeDataStreams() {
        return true;
    }
}
