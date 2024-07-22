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
import org.opensearch.common.UUIDs;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.search.resourcetypes.ResourceType;
import org.joda.time.Instant;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Class to define the QueryGroup schema
 * {
 *              "_id": "fafjafjkaf9ag8a9ga9g7ag0aagaga",
 *              "resourceLimits": {
 *                  "jvm": 0.4
 *              },
 *              "resiliency_mode": "enforced",
 *              "name": "analytics",
 *              "updatedAt": 4513232415
 * }
 */
@ExperimentalApi
public class QueryGroup extends AbstractDiffable<QueryGroup> implements ToXContentObject {

    private static final int MAX_CHARS_ALLOWED_IN_NAME = 50;
    private final String name;
    private final String _id;
    private final ResiliencyMode resiliencyMode;
    // It is an epoch in millis
    private final long updatedAtInMillis;
    private final Map<ResourceType, Object> resourceLimits;

    public QueryGroup(String name, ResiliencyMode resiliencyMode, Map<ResourceType, Object> resourceLimits) {
        this(name, UUIDs.randomBase64UUID(), resiliencyMode, resourceLimits, Instant.now().getMillis());
    }

    public QueryGroup(String name, String _id, ResiliencyMode resiliencyMode, Map<ResourceType, Object> resourceLimits, long updatedAt) {
        Objects.requireNonNull(name, "QueryGroup.name can't be null");
        Objects.requireNonNull(resourceLimits, "QueryGroup.resourceLimits can't be null");
        Objects.requireNonNull(resiliencyMode, "QueryGroup.resiliencyMode can't be null");
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
        this.resiliencyMode = resiliencyMode;
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
        this(
            in.readString(),
            in.readString(),
            ResiliencyMode.fromName(in.readString()),
            in.readMap((i) -> ResourceType.fromName(i.readString()), StreamInput::readGenericValue),
            in.readLong()
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeString(_id);
        out.writeString(resiliencyMode.getName());
        out.writeMap(resourceLimits, ResourceType::writeTo, StreamOutput::writeGenericValue);
        out.writeLong(updatedAtInMillis);
    }

    private void validateResourceLimits(Map<ResourceType, Object> resourceLimits) {
        for (Map.Entry<ResourceType, Object> resource : resourceLimits.entrySet()) {
            Double threshold = (Double) resource.getValue();
            Objects.requireNonNull(resource.getKey(), "resourceName can't be null");
            Objects.requireNonNull(threshold, "resource limit threshold for" + resource.getKey().getName() + " : can't be null");

            if (Double.compare(threshold, 1.0) > 0) {
                throw new IllegalArgumentException("resource value should be less than 1.0");
            }
        }
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        builder.field("_id", _id);
        builder.field("name", name);
        builder.field("resiliency_mode", resiliencyMode.getName());
        builder.field("updatedAt", updatedAtInMillis);
        // write resource limits
        builder.startObject("resourceLimits");
        for (ResourceType resourceType : ResourceType.values()) {
            if (resourceLimits.containsKey(resourceType)) {
                builder.field(resourceType.getName(), resourceLimits.get(resourceType));
            }
        }
        builder.endObject();

        builder.endObject();
        return builder;
    }

    public static QueryGroup fromXContent(final XContentParser parser) throws IOException {
        if (parser.currentToken() == null) { // fresh parser? move to the first token
            parser.nextToken();
        }

        Builder builder = builder();

        XContentParser.Token token = parser.currentToken();

        if (token != XContentParser.Token.START_OBJECT) {
            throw new IllegalArgumentException("Expected START_OBJECT token but found [" + parser.currentName() + "]");
        }

        String fieldName = "";
        // Map to hold resources
        final Map<ResourceType, Object> resourceLimits = new HashMap<>();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token.isValue()) {
                if (fieldName.equals("_id")) {
                    builder._id(parser.text());
                } else if (fieldName.equals("name")) {
                    builder.name(parser.text());
                } else if (fieldName.equals("resiliency_mode")) {
                    builder.mode(parser.text());
                } else if (fieldName.equals("updatedAt")) {
                    builder.updatedAt(parser.longValue());
                } else {
                    throw new IllegalArgumentException(fieldName + " is not a valid field in QueryGroup");
                }
            } else if (token == XContentParser.Token.START_OBJECT) {

                if (!fieldName.equals("resourceLimits")) {
                    throw new IllegalArgumentException(
                        "QueryGroup.resourceLimits is an object and expected token was { " + " but found " + token
                    );
                }

                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        fieldName = parser.currentName();
                    } else {
                        resourceLimits.put(ResourceType.fromName(fieldName), parser.doubleValue());
                    }
                }

            }
        }
        builder.resourceLimits(resourceLimits);
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
        return Objects.equals(name, that.name)
            && Objects.equals(resourceLimits, that.resourceLimits)
            && Objects.equals(_id, that._id)
            && updatedAtInMillis == that.updatedAtInMillis;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, resourceLimits, updatedAtInMillis, _id);
    }

    public String getName() {
        return name;
    }

    public ResiliencyMode getResiliencyMode() {
        return resiliencyMode;
    }

    public Map<ResourceType, Object> getResourceLimits() {
        return resourceLimits;
    }

    public String get_id() {
        return _id;
    }

    public long getUpdatedAtInMillis() {
        return updatedAtInMillis;
    }

    /**
     * builder method for the {@link QueryGroup}
     * @return Builder object
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * This enum models the different QueryGroup resiliency modes
     * SOFT - means that this query group can consume more than query group resource limits if node is not in duress
     * ENFORCED - means that it will never breach the assigned limits and will cancel as soon as the limits are breached
     * MONITOR - it will not cause any cancellation but just log the eligible task cancellations
     */
    @ExperimentalApi
    public enum ResiliencyMode {
        SOFT("soft"),
        ENFORCED("enforced"),
        MONITOR("monitor");

        private final String name;

        ResiliencyMode(String mode) {
            this.name = mode;
        }

        public String getName() {
            return name;
        }

        public static ResiliencyMode fromName(String s) {
            for (ResiliencyMode mode : values()) {
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
        private ResiliencyMode resiliencyMode;
        private long updatedAt;
        private Map<ResourceType, Object> resourceLimits;

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
            this.resiliencyMode = ResiliencyMode.fromName(mode);
            return this;
        }

        public Builder updatedAt(long updatedAt) {
            this.updatedAt = updatedAt;
            return this;
        }

        public Builder resourceLimits(Map<ResourceType, Object> resourceLimits) {
            this.resourceLimits = resourceLimits;
            return this;
        }

        public QueryGroup build() {
            return new QueryGroup(name, _id, resiliencyMode, resourceLimits, updatedAt);
        }

    }
}
