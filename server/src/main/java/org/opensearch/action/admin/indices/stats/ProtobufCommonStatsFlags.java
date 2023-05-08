/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.action.admin.indices.stats;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import org.opensearch.Version;
import org.opensearch.common.Strings;
import org.opensearch.common.io.stream.ProtobufStreamInput;
import org.opensearch.common.io.stream.ProtobufStreamOutput;
import org.opensearch.common.io.stream.ProtobufWriteable;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;

/**
 * Common Stats Flags for OpenSearch
*
* @opensearch.internal
*/
public class ProtobufCommonStatsFlags implements ProtobufWriteable, Cloneable {

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

    /**
     * @param flags flags to set. If no flags are supplied, default flags will be set.
    */
    public ProtobufCommonStatsFlags(Flag... flags) {
        if (flags.length > 0) {
            clear();
            Collections.addAll(this.flags, flags);
        }
    }

    public ProtobufCommonStatsFlags(CodedInputStream in) throws IOException {
        ProtobufStreamInput protobufStreamInput = new ProtobufStreamInput();
        final long longFlags = in.readLong();
        flags.clear();
        for (Flag flag : Flag.values()) {
            if ((longFlags & (1 << flag.getIndex())) != 0) {
                flags.add(flag);
            }
        }
        if (protobufStreamInput.getVersion().before(Version.V_2_0_0)) {
            protobufStreamInput.readStringArray(in);
        }
        groups = protobufStreamInput.readStringArray(in);
        fieldDataFields = protobufStreamInput.readStringArray(in);
        completionDataFields = protobufStreamInput.readStringArray(in);
        includeSegmentFileSizes = in.readBool();
        includeUnloadedSegments = in.readBool();
        includeAllShardIndexingPressureTrackers = in.readBool();
        includeOnlyTopIndexingPressureMetrics = in.readBool();
    }

    @Override
    public void writeTo(CodedOutputStream out) throws IOException {
        ProtobufStreamOutput protobufStreamOutput = new ProtobufStreamOutput();
        long longFlags = 0;
        for (Flag flag : flags) {
            longFlags |= (1 << flag.getIndex());
        }
        out.writeInt64NoTag(longFlags);

        if (protobufStreamOutput.getVersion().before(Version.V_2_0_0)) {
            protobufStreamOutput.writeStringArrayNullable(Strings.EMPTY_ARRAY, out);
        }
        protobufStreamOutput.writeStringArrayNullable(groups, out);
        protobufStreamOutput.writeStringArrayNullable(fieldDataFields, out);
        protobufStreamOutput.writeStringArrayNullable(completionDataFields, out);
        out.writeBoolNoTag(includeSegmentFileSizes);
        out.writeBoolNoTag(includeUnloadedSegments);
        out.writeBoolNoTag(includeAllShardIndexingPressureTrackers);
        out.writeBoolNoTag(includeOnlyTopIndexingPressureMetrics);
    }

    /**
     * Sets all flags to return all stats.
    */
    public ProtobufCommonStatsFlags all() {
        flags = EnumSet.allOf(Flag.class);
        groups = null;
        fieldDataFields = null;
        completionDataFields = null;
        includeSegmentFileSizes = false;
        includeUnloadedSegments = false;
        includeAllShardIndexingPressureTrackers = false;
        includeOnlyTopIndexingPressureMetrics = false;
        return this;
    }

    /**
     * Clears all stats.
    */
    public ProtobufCommonStatsFlags clear() {
        flags = EnumSet.noneOf(Flag.class);
        groups = null;
        fieldDataFields = null;
        completionDataFields = null;
        includeSegmentFileSizes = false;
        includeUnloadedSegments = false;
        includeAllShardIndexingPressureTrackers = false;
        includeOnlyTopIndexingPressureMetrics = false;
        return this;
    }

    public boolean anySet() {
        return !flags.isEmpty();
    }

    public Flag[] getFlags() {
        return flags.toArray(new Flag[0]);
    }

    /**
     * Sets specific search group stats to retrieve the stats for. Mainly affects search
    * when enabled.
    */
    public ProtobufCommonStatsFlags groups(String... groups) {
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
    public ProtobufCommonStatsFlags fieldDataFields(String... fieldDataFields) {
        this.fieldDataFields = fieldDataFields;
        return this;
    }

    public String[] fieldDataFields() {
        return this.fieldDataFields;
    }

    public ProtobufCommonStatsFlags completionDataFields(String... completionDataFields) {
        this.completionDataFields = completionDataFields;
        return this;
    }

    public String[] completionDataFields() {
        return this.completionDataFields;
    }

    public ProtobufCommonStatsFlags includeSegmentFileSizes(boolean includeSegmentFileSizes) {
        this.includeSegmentFileSizes = includeSegmentFileSizes;
        return this;
    }

    public ProtobufCommonStatsFlags includeUnloadedSegments(boolean includeUnloadedSegments) {
        this.includeUnloadedSegments = includeUnloadedSegments;
        return this;
    }

    public ProtobufCommonStatsFlags includeAllShardIndexingPressureTrackers(boolean includeAllShardPressureTrackers) {
        this.includeAllShardIndexingPressureTrackers = includeAllShardPressureTrackers;
        return this;
    }

    public ProtobufCommonStatsFlags includeOnlyTopIndexingPressureMetrics(boolean includeOnlyTopIndexingPressureMetrics) {
        this.includeOnlyTopIndexingPressureMetrics = includeOnlyTopIndexingPressureMetrics;
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

    public ProtobufCommonStatsFlags set(Flag flag, boolean add) {
        if (add) {
            set(flag);
        } else {
            unSet(flag);
        }
        return this;
    }

    @Override
    public ProtobufCommonStatsFlags clone() {
        try {
            ProtobufCommonStatsFlags cloned = (ProtobufCommonStatsFlags) super.clone();
            cloned.flags = flags.clone();
            return cloned;
        } catch (CloneNotSupportedException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * The flags.
    *
    * @opensearch.internal
    */
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
