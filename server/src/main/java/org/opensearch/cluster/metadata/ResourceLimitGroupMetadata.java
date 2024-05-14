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
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ConstructingObjectParser;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.opensearch.cluster.metadata.Metadata.ALL_CONTEXTS;

/**
 * This class holds the resourceLimitGroupMetadata
 * sample schema
 * {
 *     "resourceLimitGroups": {
 *         "name": {
 *             {@link ResourceLimitGroup}
 *         },
 *        ...
 *     }
 * }
 */
@ExperimentalApi
public class ResourceLimitGroupMetadata implements Metadata.Custom {
    public static final String TYPE = "resourceLimitGroup";
    private static final ParseField RESOURCE_LIMIT_GROUP_FIELD = new ParseField("resourceLimitGroups");

    @SuppressWarnings("unchecked")
    static final ConstructingObjectParser<ResourceLimitGroupMetadata, Void> PARSER = new ConstructingObjectParser<>(
        "resourceLimitGroupParser",
        args -> new ResourceLimitGroupMetadata((Set<ResourceLimitGroup>) args[0])
    );

    static {
        PARSER.declareObjectArray(
            ConstructingObjectParser.constructorArg(),
            (p, c) -> ResourceLimitGroup.fromXContent(p),
            RESOURCE_LIMIT_GROUP_FIELD
        );
    }

    private final Set<ResourceLimitGroup> resourceLimitGroups;

    public ResourceLimitGroupMetadata(Set<ResourceLimitGroup> resourceLimitGroups) {
        this.resourceLimitGroups = resourceLimitGroups;
    }

    public ResourceLimitGroupMetadata(StreamInput in) throws IOException {
        this.resourceLimitGroups = in.readSet(ResourceLimitGroup::new);
    }

    public Set<ResourceLimitGroup> resourceLimitGroups() {
        return this.resourceLimitGroups;
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

    /**
     * Write this into the {@linkplain StreamOutput}.
     *
     * @param out
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(resourceLimitGroups);
    }

    /**
     * @param builder
     * @param params
     * @return
     * @throws IOException
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(RESOURCE_LIMIT_GROUP_FIELD.getPreferredName(), resourceLimitGroups);
        builder.endObject();
        return builder;
    }

    public static ResourceLimitGroupMetadata fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    /**
     * Returns serializable object representing differences between this and previousState
     *
     * @param previousState
     */
    @Override
    public Diff<Metadata.Custom> diff(final Metadata.Custom previousState) {
        return new ResourceLimitGroupMetadataDiff((ResourceLimitGroupMetadata) previousState, this);
    }

    @Override
    public EnumSet<Metadata.XContentContext> context() {
        return ALL_CONTEXTS;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ResourceLimitGroupMetadata that = (ResourceLimitGroupMetadata) o;
        return Objects.equals(resourceLimitGroups, that.resourceLimitGroups);
    }

    @Override
    public int hashCode() {
        return Objects.hash(resourceLimitGroups);
    }

    @Override
    public String toString() {
        return Strings.toString(MediaTypeRegistry.JSON, this);
    }

    /**
     * ResourceLimitGroupMetadataDiff
     */
    static class ResourceLimitGroupMetadataDiff implements NamedDiff<Metadata.Custom> {
        final Diff<Map<String, ResourceLimitGroup>> dataStreamDiff;

        ResourceLimitGroupMetadataDiff(final ResourceLimitGroupMetadata before, final ResourceLimitGroupMetadata after) {
            dataStreamDiff = DiffableUtils.diff(
                toMap(before.resourceLimitGroups),
                toMap(after.resourceLimitGroups),
                DiffableUtils.getStringKeySerializer()
            );
        }

        ResourceLimitGroupMetadataDiff(final StreamInput in) throws IOException {
            this.dataStreamDiff = DiffableUtils.readJdkMapDiff(
                in,
                DiffableUtils.getStringKeySerializer(),
                ResourceLimitGroup::new,
                ResourceLimitGroup::readDiff
            );
        }

        private Map<String, ResourceLimitGroup> toMap(Set<ResourceLimitGroup> resourceLimitGroups) {
            final Map<String, ResourceLimitGroup> resourceLimitGroupMap = new HashMap<>();

            resourceLimitGroups.forEach(resourceLimitGroup -> resourceLimitGroupMap.put(resourceLimitGroup.getName(), resourceLimitGroup));
            return resourceLimitGroupMap;
        }

        /**
         * Returns the name of the writeable object
         */
        @Override
        public String getWriteableName() {
            return TYPE;
        }

        /**
         * Write this into the {@linkplain StreamOutput}.
         *
         * @param out
         */
        @Override
        public void writeTo(StreamOutput out) throws IOException {
            dataStreamDiff.writeTo(out);
        }

        /**
         * Applies difference to the specified part and returns the resulted part
         *
         * @param part
         */
        @Override
        public Metadata.Custom apply(Metadata.Custom part) {
            return new ResourceLimitGroupMetadata(
                new HashSet<>(dataStreamDiff.apply(toMap(((ResourceLimitGroupMetadata) part).resourceLimitGroups)).values())
            );
        }
    }
}
