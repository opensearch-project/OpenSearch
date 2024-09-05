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
import org.opensearch.wlm.MutableQueryGroupFragment;
import org.opensearch.wlm.MutableQueryGroupFragment.ResiliencyMode;
import org.opensearch.wlm.ResourceType;
import org.joda.time.Instant;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

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
    private final MutableQueryGroupFragment mutableQueryGroupFragment;

    public QueryGroup(String name, MutableQueryGroupFragment mutableQueryGroupFragment) {
        this(name, UUIDs.randomBase64UUID(), mutableQueryGroupFragment, Instant.now().getMillis());
    }

    public QueryGroup(String name, String _id, MutableQueryGroupFragment mutableQueryGroupFragment, long updatedAt) {
        Objects.requireNonNull(name, "QueryGroup.name can't be null");
        Objects.requireNonNull(mutableQueryGroupFragment.getResourceLimits(), "QueryGroup.resourceLimits can't be null");
        Objects.requireNonNull(mutableQueryGroupFragment.getResiliencyMode(), "QueryGroup.resiliencyMode can't be null");
        Objects.requireNonNull(_id, "QueryGroup._id can't be null");
        validateName(name);

        if (mutableQueryGroupFragment.getResourceLimits().isEmpty()) {
            throw new IllegalArgumentException("QueryGroup.resourceLimits should at least have 1 resource limit");
        }
        if (!isValid(updatedAt)) {
            throw new IllegalArgumentException("QueryGroup.updatedAtInMillis is not a valid epoch");
        }

        this.name = name;
        this._id = _id;
        this.mutableQueryGroupFragment = mutableQueryGroupFragment;
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
        this(in.readString(), in.readString(), new MutableQueryGroupFragment(in), in.readLong());
    }

    public static QueryGroup updateExistingQueryGroup(QueryGroup existingGroup, MutableQueryGroupFragment mutableQueryGroupFragment) {
        final Map<ResourceType, Double> updatedResourceLimits = new HashMap<>(existingGroup.getResourceLimits());
        final Map<ResourceType, Double> mutableFragmentResourceLimits = mutableQueryGroupFragment.getResourceLimits();
        if (mutableFragmentResourceLimits != null && !mutableFragmentResourceLimits.isEmpty()) {
            updatedResourceLimits.putAll(mutableFragmentResourceLimits);
        }
        final ResiliencyMode mode = Optional.ofNullable(mutableQueryGroupFragment.getResiliencyMode())
            .orElse(existingGroup.getResiliencyMode());
        return new QueryGroup(
            existingGroup.getName(),
            existingGroup.get_id(),
            new MutableQueryGroupFragment(mode, updatedResourceLimits),
            Instant.now().getMillis()
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeString(_id);
        mutableQueryGroupFragment.writeTo(out);
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
        for (String fieldName : MutableQueryGroupFragment.acceptedFieldNames) {
            mutableQueryGroupFragment.writeField(builder, fieldName);
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
            && Objects.equals(mutableQueryGroupFragment, that.mutableQueryGroupFragment)
            && Objects.equals(_id, that._id)
            && updatedAtInMillis == that.updatedAtInMillis;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, mutableQueryGroupFragment, updatedAtInMillis, _id);
    }

    public String getName() {
        return name;
    }

    public MutableQueryGroupFragment getMutableQueryGroupFragment() {
        return mutableQueryGroupFragment;
    }

    public ResiliencyMode getResiliencyMode() {
        return getMutableQueryGroupFragment().getResiliencyMode();
    }

    public Map<ResourceType, Double> getResourceLimits() {
        return getMutableQueryGroupFragment().getResourceLimits();
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
        private MutableQueryGroupFragment mutableQueryGroupFragment;
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
            MutableQueryGroupFragment mutableQueryGroupFragment1 = new MutableQueryGroupFragment();
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    fieldName = parser.currentName();
                } else if (token.isValue()) {
                    if (fieldName.equals(_ID_STRING)) {
                        builder._id(parser.text());
                    } else if (fieldName.equals(NAME_STRING)) {
                        builder.name(parser.text());
                    } else if (MutableQueryGroupFragment.shouldParse(fieldName)) {
                        mutableQueryGroupFragment1.parseField(parser, fieldName);
                    } else if (fieldName.equals(UPDATED_AT_STRING)) {
                        builder.updatedAt(parser.longValue());
                    } else {
                        throw new IllegalArgumentException(fieldName + " is not a valid field in QueryGroup");
                    }
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if (!MutableQueryGroupFragment.shouldParse(fieldName)) {
                        throw new IllegalArgumentException(fieldName + " is not a valid object in QueryGroup");
                    }
                    mutableQueryGroupFragment1.parseField(parser, fieldName);
                }
            }
            return builder.mutableQueryGroupFragment(mutableQueryGroupFragment1);
        }

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder _id(String _id) {
            this._id = _id;
            return this;
        }

        public Builder mutableQueryGroupFragment(MutableQueryGroupFragment mutableQueryGroupFragment) {
            this.mutableQueryGroupFragment = mutableQueryGroupFragment;
            return this;
        }

        public Builder updatedAt(long updatedAt) {
            this.updatedAt = updatedAt;
            return this;
        }

        public QueryGroup build() {
            return new QueryGroup(name, _id, mutableQueryGroupFragment, updatedAt);
        }

        public MutableQueryGroupFragment getMutableQueryGroupFragment() {
            return mutableQueryGroupFragment;
        }
    }
}
