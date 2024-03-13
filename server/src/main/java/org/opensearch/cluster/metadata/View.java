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
import org.opensearch.core.ParseField;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ConstructingObjectParser;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

/** View of data in OpenSearch indices */
@ExperimentalApi
public class View extends AbstractDiffable<View> implements ToXContentObject {

    private final String name;
    private final String description;
    private final long createdAt;
    private final long modifiedAt;
    private final SortedSet<Target> targets;

    public View(final String name, final String description, final Long createdAt, final Long modifiedAt, final Set<Target> targets) {
        this.name = Objects.requireNonNull(name, "Name must be provided");
        this.description = description;
        this.createdAt = createdAt != null ? createdAt : -1;
        this.modifiedAt = modifiedAt != null ? modifiedAt : -1;
        this.targets = new TreeSet<>(Objects.requireNonNull(targets, "Targets are required on a view"));
    }

    public View(final StreamInput in) throws IOException {
        this(in.readString(), in.readOptionalString(), in.readZLong(), in.readZLong(), new TreeSet<>(in.readList(Target::new)));
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public long getCreatedAt() {
        return createdAt;
    }

    public long getModifiedAt() {
        return modifiedAt;
    }

    public SortedSet<Target> getTargets() {
        return new TreeSet<>(targets);
    }

    public static Diff<View> readDiffFrom(final StreamInput in) throws IOException {
        return readDiffFrom(View::new, in);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        View that = (View) o;
        return name.equals(that.name)
            && description.equals(that.description)
            && createdAt == that.createdAt
            && modifiedAt == that.modifiedAt
            && targets.equals(that.targets);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, description, createdAt, modifiedAt, targets);
    }

    /** The source of data used to project the view */
    @ExperimentalApi
    public static class Target implements Writeable, ToXContentObject, Comparable<Target> {

        private final String indexPattern;

        public Target(final String indexPattern) {
            this.indexPattern = Objects.requireNonNull(indexPattern, "IndexPattern is required");
        }

        public Target(final StreamInput in) throws IOException {
            this(in.readString());
        }

        public String getIndexPattern() {
            return indexPattern;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Target that = (Target) o;
            return indexPattern.equals(that.indexPattern);
        }

        @Override
        public int hashCode() {
            return Objects.hash(indexPattern);
        }

        public static final ParseField INDEX_PATTERN_FIELD = new ParseField("indexPattern");

        @Override
        public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
            builder.startObject();
            builder.field(INDEX_PATTERN_FIELD.getPreferredName(), indexPattern);
            builder.endObject();
            return builder;
        }

        private static final ConstructingObjectParser<Target, Void> PARSER = new ConstructingObjectParser<>(
            "target",
            args -> new Target((String) args[0])
        );
        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), INDEX_PATTERN_FIELD);
        }

        public static Target fromXContent(final XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            out.writeString(indexPattern);
        }

        @Override
        public int compareTo(final Target o) {
            if (this == o) return 0;

            final Target other = (Target) o;
            return this.indexPattern.compareTo(other.indexPattern);
        }
    }

    public static final ParseField NAME_FIELD = new ParseField("name");
    public static final ParseField DESCRIPTION_FIELD = new ParseField("description");
    public static final ParseField CREATED_AT_FIELD = new ParseField("createdAt");
    public static final ParseField MODIFIED_AT_FIELD = new ParseField("modifiedAt");
    public static final ParseField TARGETS_FIELD = new ParseField("targets");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<View, Void> PARSER = new ConstructingObjectParser<>(
        "view",
        args -> new View((String) args[0], (String) args[1], (Long) args[2], (Long) args[3], new TreeSet<>((List<Target>) args[4]))
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), NAME_FIELD);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), DESCRIPTION_FIELD);
        PARSER.declareLongOrNull(ConstructingObjectParser.optionalConstructorArg(), -1L, CREATED_AT_FIELD);
        PARSER.declareLongOrNull(ConstructingObjectParser.optionalConstructorArg(), -1L, MODIFIED_AT_FIELD);
        PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), (p, c) -> Target.fromXContent(p), TARGETS_FIELD);
    }

    public static View fromXContent(final XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        builder.field(NAME_FIELD.getPreferredName(), name);
        builder.field(DESCRIPTION_FIELD.getPreferredName(), description);
        builder.field(CREATED_AT_FIELD.getPreferredName(), createdAt);
        builder.field(MODIFIED_AT_FIELD.getPreferredName(), modifiedAt);
        builder.field(TARGETS_FIELD.getPreferredName(), targets);
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeOptionalString(description);
        out.writeZLong(createdAt);
        out.writeZLong(modifiedAt);
        out.writeList(targets.stream().collect(Collectors.toList()));
    }
}
