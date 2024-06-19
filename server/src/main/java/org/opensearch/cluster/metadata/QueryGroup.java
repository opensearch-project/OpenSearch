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
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.joda.time.Instant;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * Class to define the QueryGroup schema
 * {
 *     "fafjafjkaf9ag8a9ga9g7ag0aagaga" : {
 *              "jvm": 0.4,
 *              "mode": "enforced",
 *              "name": "analytics",
 *              "updatedAt": 4513232415
 *      }
 * }
 */
@ExperimentalApi
public class QueryGroup extends AbstractDiffable<QueryGroup> implements ToXContentObject {

    public static final int MAX_CHARS_ALLOWED_IN_NAME = 50;
    private final String name;
    private final String _id;
    private final QueryGroupMode mode;
    // It is an epoch in millis
    private final long updatedAtInMillis;
    private final Map<String, Object> resourceLimits;

    // list of resources that are allowed to be present in the QueryGroup schema
    public static final List<String> ALLOWED_RESOURCES = List.of("jvm");

    public QueryGroup(String name, String _id, QueryGroupMode mode, Map<String, Object> resourceLimits, long updatedAt) {
        Objects.requireNonNull(name, "QueryGroup.name can't be null");
        Objects.requireNonNull(resourceLimits, "QueryGroup.resourceLimits can't be null");
        Objects.requireNonNull(mode, "QueryGroup.mode can't be null");
        Objects.requireNonNull(_id, "QueryGroup._id can't be null");

        if (name.length() > MAX_CHARS_ALLOWED_IN_NAME) {
            throw new IllegalArgumentException("QueryGroup.name shouldn't be more than 50 chars long");
        }

        if (resourceLimits.isEmpty()) {
            throw new IllegalArgumentException("QueryGroup.resourceLimits should at least have 1 resource limit");
        }
        validateResourceLimits(resourceLimits);
        if (!isValid(updatedAt)) {
            throw new IllegalArgumentException("QueryGroup.updatedAtInMillis is not a valid epoch");
        }

        this.name = name;
        this._id = _id;
        this.mode = mode;
        this.resourceLimits = resourceLimits;
        this.updatedAtInMillis = updatedAt;
    }

    private static boolean isValid(long updatedAt) {
        long minValidTimestamp = Instant.ofEpochMilli(0L).getMillis();

        // Use Instant.now() to get the current time in seconds since epoch
        long currentSeconds = Instant.now().getMillis();

        // Check if the timestamp is within a reasonable range
        return minValidTimestamp <= updatedAt && updatedAt <= currentSeconds;
    }

    public QueryGroup(StreamInput in) throws IOException {
        this(in.readString(), in.readString(), QueryGroupMode.fromName(in.readString()), in.readMap(), in.readLong());
    }

    /**
     * Write this into the {@linkplain StreamOutput}.
     *
     * @param out
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        writeToOutputStream(out, name, _id, mode, resourceLimits, updatedAtInMillis);
    }

    public static void writeToOutputStream(
        StreamOutput out,
        String name,
        String _id,
        QueryGroupMode mode,
        Map<String, Object> resourceLimits,
        long updatedAtInMillis
    ) throws IOException {
        out.writeString(name);
        out.writeString(_id);
        out.writeString(mode.getName());
        out.writeMap(resourceLimits);
        out.writeLong(updatedAtInMillis);
    }

    private void validateResourceLimits(Map<String, Object> resourceLimits) {
        for (Map.Entry<String, Object> resource : resourceLimits.entrySet()) {
            String resourceName = resource.getKey();
            Double threshold = (Double) resource.getValue();
            Objects.requireNonNull(resourceName, "resourceName can't be null");
            Objects.requireNonNull(threshold, "resource value can't be null");

            if (Double.compare(threshold, 1.0) > 0) {
                throw new IllegalArgumentException("resource value should be less than 1.0");
            }

            if (!ALLOWED_RESOURCES.contains(resourceName.toLowerCase(Locale.ROOT))) {
                throw new IllegalArgumentException(
                    "resource has to be valid, valid resources " + ALLOWED_RESOURCES.stream().reduce((x, e) -> x + ", " + e).get()
                );
            }
        }
    }

    /**
     * @param builder
     * @param params
     * @return
     * @throws IOException
     */
    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        builder.startObject(this._id);
        builder.field("name", name);
        builder.field("mode", mode.getName());
        builder.field("updatedAt", updatedAtInMillis);
        builder.mapContents(resourceLimits);
        builder.endObject();
        builder.endObject();
        return builder;
    }

    public static QueryGroup fromXContent(final XContentParser parser) throws IOException {
        if (parser.currentToken() == null) { // fresh parser? move to the first token
            parser.nextToken();
        }
        if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
            parser.nextToken(); // move to field name
        }
        if (parser.currentToken() != XContentParser.Token.FIELD_NAME) {  // on a start object move to next token
            throw new IllegalArgumentException("Expected FIELD_NAME token but found [" + parser.currentName() + "]");
        }

        Builder builder = builder()._id(parser.currentName());

        XContentParser.Token token = parser.nextToken();

        if (token != XContentParser.Token.START_OBJECT) {
            throw new IllegalArgumentException("Expected START_OBJECT token but found [" + parser.currentName() + "]");
        }

        String fieldName = "";
        // Map to hold resources
        final Map<String, Object> resourceLimits_ = new HashMap<>();
        while ((token = parser.nextToken()) != null) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token.isValue()) {
                if (fieldName.equals("name")) {
                    builder.name(parser.text());
                } else if (fieldName.equals("mode")) {
                    builder.mode(parser.text());
                } else if (fieldName.equals("updatedAt")) {
                    builder.updatedAt(parser.longValue());
                } else if (ALLOWED_RESOURCES.contains(fieldName)) {
                    resourceLimits_.put(fieldName, parser.doubleValue());
                } else {
                    throw new IllegalArgumentException("unrecognised [field=" + fieldName + " in QueryGroup");
                }
            }
        }
        builder.resourceLimits(resourceLimits_);
        return builder.build();
    }

    public static Diff<QueryGroup> readDiff(final StreamInput in) throws IOException {
        return readDiffFrom(QueryGroup::new, in);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QueryGroup that = (QueryGroup) o;
        return Objects.equals(name, that.name) && Objects.equals(resourceLimits, that.resourceLimits);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, resourceLimits);
    }

    public String getName() {
        return name;
    }

    public QueryGroupMode getMode() {
        return mode;
    }

    public Map<String, Object> getResourceLimits() {
        return resourceLimits;
    }

    public String get_id() {
        return _id;
    }

    public long getUpdatedAtInMillis() {
        return updatedAtInMillis;
    }

    /**
     * builder method for this {@link QueryGroup}
     * @return
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * This enum models the different sandbox modes
     */
    @ExperimentalApi
    public enum QueryGroupMode {
        SOFT("soft"),
        ENFORCED("enforced"),
        MONITOR("monitor");

        private final String name;

        QueryGroupMode(String mode) {
            this.name = mode;
        }

        public String getName() {
            return name;
        }

        public static QueryGroupMode fromName(String s) {
            for (QueryGroupMode mode : values()) {
                if (mode.getName().equalsIgnoreCase(s)) return mode;

            }
            throw new IllegalArgumentException("Invalid value for QueryGroupMode: " + s);
        }

    }

    /**
     * Builder class for {@link QueryGroup}
     */
    @ExperimentalApi
    public static class Builder {
        private String name;
        private String _id;
        private QueryGroupMode mode;
        private long updatedAt;
        private Map<String, Object> resourceLimits;

        private Builder() {}

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder _id(String _id) {
            this._id = _id;
            return this;
        }

        public Builder mode(String mode) {
            this.mode = QueryGroupMode.fromName(mode);
            return this;
        }

        public Builder updatedAt(long updatedAt) {
            this.updatedAt = updatedAt;
            return this;
        }

        public Builder resourceLimits(Map<String, Object> resourceLimits) {
            this.resourceLimits = resourceLimits;
            return this;
        }

        public QueryGroup build() {
            return new QueryGroup(name, _id, mode, resourceLimits, updatedAt);
        }

    }
}
