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

package org.opensearch.action.admin.indices.stats;

import org.opensearch.LegacyESVersion;
import org.opensearch.Version;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.cache.CacheType;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

/**
 * Common Stats Flags for OpenSearch
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class CommonStatsFlags implements Writeable, Cloneable {

    public static final CommonStatsFlags ALL = new CommonStatsFlags().all();
    public static final CommonStatsFlags NONE = new CommonStatsFlags().clear();

    private EnumSet<Flag> flags = EnumSet.allOf(Flag.class);
    private String[] groups = null;
    private String[] fieldDataFields = null;
    private String[] completionDataFields = null;
    private boolean includeSegmentFileSizes = false;
    private boolean includeUnloadedSegments = false;
    private boolean includeAllShardIndexingPressureTrackers = false;
    private boolean includeOnlyTopIndexingPressureMetrics = false;
    // Used for metric CACHE_STATS, to determine which caches to report stats for
    private EnumSet<CacheType> includeCaches = EnumSet.noneOf(CacheType.class);
    private String[] levels = new String[0];

    /**
     * @param flags flags to set. If no flags are supplied, default flags will be set.
     */
    public CommonStatsFlags(Flag... flags) {
        if (flags.length > 0) {
            clear();
            Collections.addAll(this.flags, flags);
        }
    }

    public CommonStatsFlags(StreamInput in) throws IOException {
        final long longFlags = in.readLong();
        flags.clear();
        for (Flag flag : Flag.values()) {
            if ((longFlags & (1 << flag.getIndex())) != 0) {
                flags.add(flag);
            }
        }
        if (in.getVersion().before(Version.V_2_0_0)) {
            in.readStringArray();
        }
        groups = in.readStringArray();
        fieldDataFields = in.readStringArray();
        completionDataFields = in.readStringArray();
        includeSegmentFileSizes = in.readBoolean();
        if (in.getVersion().onOrAfter(LegacyESVersion.V_7_2_0)) {
            includeUnloadedSegments = in.readBoolean();
        }
        if (in.getVersion().onOrAfter(Version.V_1_2_0)) {
            includeAllShardIndexingPressureTrackers = in.readBoolean();
            includeOnlyTopIndexingPressureMetrics = in.readBoolean();
        }
        // TODO: change from V_3_0_0 to V_2_14_0 on main after backport to 2.x
        if (in.getVersion().onOrAfter(Version.V_2_14_0)) {
            includeCaches = in.readEnumSet(CacheType.class);
            levels = in.readStringArray();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        long longFlags = 0;
        for (Flag flag : flags) {
            longFlags |= (1 << flag.getIndex());
        }
        out.writeLong(longFlags);

        if (out.getVersion().before(Version.V_2_0_0)) {
            out.writeStringArrayNullable(Strings.EMPTY_ARRAY);
        }
        out.writeStringArrayNullable(groups);
        out.writeStringArrayNullable(fieldDataFields);
        out.writeStringArrayNullable(completionDataFields);
        out.writeBoolean(includeSegmentFileSizes);
        if (out.getVersion().onOrAfter(LegacyESVersion.V_7_2_0)) {
            out.writeBoolean(includeUnloadedSegments);
        }
        if (out.getVersion().onOrAfter(Version.V_1_2_0)) {
            out.writeBoolean(includeAllShardIndexingPressureTrackers);
            out.writeBoolean(includeOnlyTopIndexingPressureMetrics);
        }
        // TODO: change from V_3_0_0 to V_2_14_0 on main after backport to 2.x
        if (out.getVersion().onOrAfter(Version.V_2_14_0)) {
            out.writeEnumSet(includeCaches);
            out.writeStringArrayNullable(levels);
        }
    }

    /**
     * Sets all flags to return all stats.
     */
    public CommonStatsFlags all() {
        flags = EnumSet.allOf(Flag.class);
        groups = null;
        fieldDataFields = null;
        completionDataFields = null;
        includeSegmentFileSizes = false;
        includeUnloadedSegments = false;
        includeAllShardIndexingPressureTrackers = false;
        includeOnlyTopIndexingPressureMetrics = false;
        includeCaches = EnumSet.allOf(CacheType.class);
        levels = new String[0];
        return this;
    }

    /**
     * Clears all stats.
     */
    public CommonStatsFlags clear() {
        flags = EnumSet.noneOf(Flag.class);
        groups = null;
        fieldDataFields = null;
        completionDataFields = null;
        includeSegmentFileSizes = false;
        includeUnloadedSegments = false;
        includeAllShardIndexingPressureTrackers = false;
        includeOnlyTopIndexingPressureMetrics = false;
        includeCaches = EnumSet.noneOf(CacheType.class);
        levels = new String[0];
        return this;
    }

    public boolean anySet() {
        return !flags.isEmpty();
    }

    public Flag[] getFlags() {
        return flags.toArray(new Flag[flags.size()]);
    }

    public Set<CacheType> getIncludeCaches() {
        return includeCaches;
    }

    public String[] getLevels() {
        return levels;
    }

    /**
     * Sets specific search group stats to retrieve the stats for. Mainly affects search
     * when enabled.
     */
    public CommonStatsFlags groups(String... groups) {
        this.groups = groups;
        return this;
    }

    public String[] groups() {
        return this.groups;
    }

    /**
     * Sets specific search group stats to retrieve the stats for. Mainly affects search
     * when enabled.
     */
    public CommonStatsFlags fieldDataFields(String... fieldDataFields) {
        this.fieldDataFields = fieldDataFields;
        return this;
    }

    public String[] fieldDataFields() {
        return this.fieldDataFields;
    }

    public CommonStatsFlags completionDataFields(String... completionDataFields) {
        this.completionDataFields = completionDataFields;
        return this;
    }

    public String[] completionDataFields() {
        return this.completionDataFields;
    }

    public CommonStatsFlags includeSegmentFileSizes(boolean includeSegmentFileSizes) {
        this.includeSegmentFileSizes = includeSegmentFileSizes;
        return this;
    }

    public CommonStatsFlags includeUnloadedSegments(boolean includeUnloadedSegments) {
        this.includeUnloadedSegments = includeUnloadedSegments;
        return this;
    }

    public CommonStatsFlags includeAllShardIndexingPressureTrackers(boolean includeAllShardPressureTrackers) {
        this.includeAllShardIndexingPressureTrackers = includeAllShardPressureTrackers;
        return this;
    }

    public CommonStatsFlags includeOnlyTopIndexingPressureMetrics(boolean includeOnlyTopIndexingPressureMetrics) {
        this.includeOnlyTopIndexingPressureMetrics = includeOnlyTopIndexingPressureMetrics;
        return this;
    }

    public CommonStatsFlags includeCacheType(CacheType cacheType) {
        includeCaches.add(cacheType);
        return this;
    }

    public CommonStatsFlags includeAllCacheTypes() {
        includeCaches = EnumSet.allOf(CacheType.class);
        return this;
    }

    public CommonStatsFlags setLevels(String[] inputLevels) {
        levels = inputLevels;
        return this;
    }

    public boolean includeUnloadedSegments() {
        return this.includeUnloadedSegments;
    }

    public boolean includeAllShardIndexingPressureTrackers() {
        return this.includeAllShardIndexingPressureTrackers;
    }

    public boolean includeOnlyTopIndexingPressureMetrics() {
        return this.includeOnlyTopIndexingPressureMetrics;
    }

    public boolean includeSegmentFileSizes() {
        return this.includeSegmentFileSizes;
    }

    public boolean isSet(Flag flag) {
        return flags.contains(flag);
    }

    boolean unSet(Flag flag) {
        return flags.remove(flag);
    }

    void set(Flag flag) {
        flags.add(flag);
    }

    public CommonStatsFlags set(Flag flag, boolean add) {
        if (add) {
            set(flag);
        } else {
            unSet(flag);
        }
        return this;
    }

    @Override
    public CommonStatsFlags clone() {
        try {
            CommonStatsFlags cloned = (CommonStatsFlags) super.clone();
            cloned.flags = flags.clone();
            return cloned;
        } catch (CloneNotSupportedException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * The flags.
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public enum Flag {
        Store("store", 0),
        Indexing("indexing", 1),
        Get("get", 2),
        Search("search", 3),
        Merge("merge", 4),
        Flush("flush", 5),
        Refresh("refresh", 6),
        QueryCache("query_cache", 7),
        FieldData("fielddata", 8),
        Docs("docs", 9),
        Warmer("warmer", 10),
        Completion("completion", 11),
        Segments("segments", 12),
        Translog("translog", 13),
        // 14 was previously used for Suggest
        RequestCache("request_cache", 15),
        Recovery("recovery", 16);

        private final String restName;
        private final int index;

        Flag(final String restName, final int index) {
            this.restName = restName;
            this.index = index;
        }

        public String getRestName() {
            return restName;
        }

        private int getIndex() {
            return index;
        }

    }
}
