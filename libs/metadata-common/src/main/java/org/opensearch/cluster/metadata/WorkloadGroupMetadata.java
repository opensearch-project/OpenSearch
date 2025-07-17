/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.Version;
import org.opensearch.cluster.Diff;
import org.opensearch.cluster.DiffableUtils;
import org.opensearch.cluster.NamedDiff;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.opensearch.cluster.metadata.Metadata.ALL_CONTEXTS;

/**
 * This class holds the WorkloadGroupMetadata
 * sample schema
 * {
 *     "queryGroups": {
 *         "_id": {
 *             {@link WorkloadGroup}
 *         },
 *        ...
 *     }
 * }
 */
public class WorkloadGroupMetadata implements Metadata.Custom {
    // We are not changing this name to ensure the cluster state restore works when a OS version < 3.0 writes it to
    // either a remote store or on local disk and OS version >= 3.0 reads it
    public static final String TYPE = "queryGroups";
    private static final ParseField WORKLOAD_GROUP_FIELD = new ParseField("queryGroups");

    private final Map<String, WorkloadGroup> workloadGroups;

    public WorkloadGroupMetadata(Map<String, WorkloadGroup> workloadGroups) {
        this.workloadGroups = workloadGroups;
    }

    public WorkloadGroupMetadata(StreamInput in) throws IOException {
        this.workloadGroups = in.readMap(StreamInput::readString, WorkloadGroup::new);
    }

    public Map<String, WorkloadGroup> workloadGroups() {
        return this.workloadGroups;
    }

    /**
     * Returns the name of the writeable object
     */
    @Override
    public String getWriteableName() {
        return TYPE;
    }

    /**
     * The minimal version of the recipient this object can be sent to
     */
    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_3_0_0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(workloadGroups, StreamOutput::writeString, (stream, val) -> val.writeTo(stream));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        for (Map.Entry<String, WorkloadGroup> entry : workloadGroups.entrySet()) {
            builder.field(entry.getKey(), entry.getValue());
        }
        return builder;
    }

    public static WorkloadGroupMetadata fromXContent(XContentParser parser) throws IOException {
        Map<String, WorkloadGroup> workloadGroupMap = new HashMap<>();

        if (parser.currentToken() == null) {
            parser.nextToken();
        }

        if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
            throw new IllegalArgumentException(
                "WorkloadGroupMetadata.fromXContent was expecting a { token but found : " + parser.currentToken()
            );
        }
        XContentParser.Token token = parser.currentToken();
        String fieldName = parser.currentName();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else {
                WorkloadGroup workloadGroup = WorkloadGroup.fromXContent(parser);
                workloadGroupMap.put(fieldName, workloadGroup);
            }
        }

        return new WorkloadGroupMetadata(workloadGroupMap);
    }

    @Override
    public Diff<Metadata.Custom> diff(final Metadata.Custom previousState) {
        return new WorkloadGroupMetadataDiff((WorkloadGroupMetadata) previousState, this);
    }

    public static NamedDiff<Metadata.Custom> readDiffFrom(StreamInput in) throws IOException {
        return new WorkloadGroupMetadataDiff(in);
    }

    @Override
    public EnumSet<Metadata.XContentContext> context() {
        return ALL_CONTEXTS;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WorkloadGroupMetadata that = (WorkloadGroupMetadata) o;
        return Objects.equals(workloadGroups, that.workloadGroups);
    }

    @Override
    public int hashCode() {
        return Objects.hash(workloadGroups);
    }

    @Override
    public String toString() {
        return Strings.toString(MediaTypeRegistry.JSON, this);
    }

    /**
     * WorkloadGroupMetadataDiff
     */
    static class WorkloadGroupMetadataDiff implements NamedDiff<Metadata.Custom> {
        final Diff<Map<String, WorkloadGroup>> dataStreamDiff;

        WorkloadGroupMetadataDiff(final WorkloadGroupMetadata before, final WorkloadGroupMetadata after) {
            dataStreamDiff = DiffableUtils.diff(before.workloadGroups, after.workloadGroups, DiffableUtils.getStringKeySerializer());
        }

        WorkloadGroupMetadataDiff(final StreamInput in) throws IOException {
            this.dataStreamDiff = DiffableUtils.readJdkMapDiff(
                in,
                DiffableUtils.getStringKeySerializer(),
                WorkloadGroup::new,
                WorkloadGroup::readDiff
            );
        }

        /**
         * Returns the name of the writeable object
         */
        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            dataStreamDiff.writeTo(out);
        }

        @Override
        public Metadata.Custom apply(Metadata.Custom part) {
            return new WorkloadGroupMetadata(new HashMap<>(dataStreamDiff.apply(((WorkloadGroupMetadata) part).workloadGroups)));
        }
    }
}
