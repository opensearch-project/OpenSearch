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
import org.opensearch.wlm.ChangeableQueryGroup;
import org.opensearch.wlm.ChangeableQueryGroup.ResiliencyMode;
import org.opensearch.wlm.ResourceType;
import org.joda.time.Instant;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * Class to define the QueryGroup schema
 * {
 *              "_id": "fafjafjkaf9ag8a9ga9g7ag0aagaga",
 *              "resource_limits": {
 *                  "memory": 0.4,
 *                  "cpu": 0.2
 *              },
 *              "resiliency_mode": "enforced",
 *              "name": "analytics",
 *              "updated_at": 4513232415
 * }
 */
@ExperimentalApi
public class QueryGroup extends AbstractDiffable<QueryGroup> implements ToXContentObject {

    public static final String _ID_STRING = "_id";
    public static final String NAME_STRING = "name";
    public static final String UPDATED_AT_STRING = "updated_at";
    private static final int MAX_CHARS_ALLOWED_IN_NAME = 50;
    private final String name;
    private final String _id;
    // It is an epoch in millis
    private final long updatedAtInMillis;
    private final ChangeableQueryGroup changeableQueryGroup;

    public QueryGroup(String name, ChangeableQueryGroup changeableQueryGroup) {
        this(name, UUIDs.randomBase64UUID(), changeableQueryGroup, Instant.now().getMillis());
    }

    public QueryGroup(String name, String _id, ChangeableQueryGroup changeableQueryGroup, long updatedAt) {
        Objects.requireNonNull(name, "QueryGroup.name can't be null");
        Objects.requireNonNull(changeableQueryGroup.getResourceLimits(), "QueryGroup.resourceLimits can't be null");
        Objects.requireNonNull(changeableQueryGroup.getResiliencyMode(), "QueryGroup.resiliencyMode can't be null");
        Objects.requireNonNull(_id, "QueryGroup._id can't be null");
        validateName(name);

        if (changeableQueryGroup.getResourceLimits().isEmpty()) {
            throw new IllegalArgumentException("QueryGroup.resourceLimits should at least have 1 resource limit");
        }
        if (!isValid(updatedAt)) {
            throw new IllegalArgumentException("QueryGroup.updatedAtInMillis is not a valid epoch");
        }

        this.name = name;
        this._id = _id;
        this.changeableQueryGroup = changeableQueryGroup;
        this.updatedAtInMillis = updatedAt;
    }

    public static boolean isValid(long updatedAt) {
        long minValidTimestamp = Instant.ofEpochMilli(0L).getMillis();

        // Use Instant.now() to get the current time in seconds since epoch
        long currentSeconds = Instant.now().getMillis();

        // Check if the timestamp is within a reasonable range
        return minValidTimestamp <= updatedAt && updatedAt <= currentSeconds;
    }

    public QueryGroup(StreamInput in) throws IOException {
        this(in.readString(), in.readString(), new ChangeableQueryGroup(in), in.readLong());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeString(_id);
        changeableQueryGroup.writeTo(out);
        out.writeLong(updatedAtInMillis);
    }

    public static void validateName(String name) {
        if (name == null || name.isEmpty() || name.length() > MAX_CHARS_ALLOWED_IN_NAME) {
            throw new IllegalArgumentException("QueryGroup.name shouldn't be null, empty or more than 50 chars long");
        }
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        builder.field(_ID_STRING, _id);
        builder.field(NAME_STRING, name);
        for (String fieldName : ChangeableQueryGroup.acceptedFieldNames) {
            changeableQueryGroup.writeField(builder, fieldName);
        }
        builder.field(UPDATED_AT_STRING, updatedAtInMillis);
        builder.endObject();
        return builder;
    }

    public static QueryGroup fromXContent(final XContentParser parser) throws IOException {
        return Builder.fromXContent(parser).build();
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
            && Objects.equals(changeableQueryGroup, that.changeableQueryGroup)
            && Objects.equals(_id, that._id)
            && updatedAtInMillis == that.updatedAtInMillis;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, changeableQueryGroup, updatedAtInMillis, _id);
    }

    public String getName() {
        return name;
    }

    public ChangeableQueryGroup getChangeableQueryGroup() {
        return changeableQueryGroup;
    }

    public ResiliencyMode getResiliencyMode() {
        return getChangeableQueryGroup().getResiliencyMode();
    }

    public Map<ResourceType, Double> getResourceLimits() {
        return getChangeableQueryGroup().getResourceLimits();
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
     * Builder class for {@link QueryGroup}
     */
    @ExperimentalApi
    public static class Builder {
        private String name;
        private String _id;
        private ChangeableQueryGroup changeableQueryGroup;
        private long updatedAt;

        private Builder() {}

        public static Builder fromXContent(XContentParser parser) throws IOException {
            if (parser.currentToken() == null) { // fresh parser? move to the first token
                parser.nextToken();
            }

            Builder builder = builder();

            XContentParser.Token token = parser.currentToken();

            if (token != XContentParser.Token.START_OBJECT) {
                throw new IllegalArgumentException("Expected START_OBJECT token but found [" + parser.currentName() + "]");
            }

            String fieldName = "";
            ChangeableQueryGroup changeableQueryGroup1 = new ChangeableQueryGroup();
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    fieldName = parser.currentName();
                } else if (token.isValue()) {
                    if (fieldName.equals(_ID_STRING)) {
                        builder._id(parser.text());
                    } else if (fieldName.equals(NAME_STRING)) {
                        builder.name(parser.text());
                    } else if (ChangeableQueryGroup.shouldParse(fieldName)) {
                        changeableQueryGroup1.parseField(parser, fieldName);
                    } else if (fieldName.equals(UPDATED_AT_STRING)) {
                        builder.updatedAt(parser.longValue());
                    } else {
                        throw new IllegalArgumentException(fieldName + " is not a valid field in QueryGroup");
                    }
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if (!ChangeableQueryGroup.shouldParse(fieldName)) {
                        throw new IllegalArgumentException(fieldName + " is not a valid object in QueryGroup");
                    }
                    changeableQueryGroup1.parseField(parser, fieldName);
                }
            }
            return builder.changeableQueryGroup(changeableQueryGroup1);
        }

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder _id(String _id) {
            this._id = _id;
            return this;
        }

        public Builder changeableQueryGroup(ChangeableQueryGroup changeableQueryGroup) {
            this.changeableQueryGroup = changeableQueryGroup;
            return this;
        }

        public Builder updatedAt(long updatedAt) {
            this.updatedAt = updatedAt;
            return this;
        }

        public QueryGroup build() {
            return new QueryGroup(name, _id, changeableQueryGroup, updatedAt);
        }

        public ChangeableQueryGroup getChangeableQueryGroup() {
            return changeableQueryGroup;
        }
    }
}
