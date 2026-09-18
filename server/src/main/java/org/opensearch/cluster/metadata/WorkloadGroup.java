/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.AbstractDiffable;
import org.opensearch.cluster.Diff;
import org.opensearch.common.UUIDs;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.wlm.MutableWorkloadGroupFragment;
import org.opensearch.wlm.MutableWorkloadGroupFragment.ResiliencyMode;
import org.opensearch.wlm.ResourceType;
import org.opensearch.wlm.WorkloadGroupThrottleSettings;
import org.joda.time.Instant;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Class to define the WorkloadGroup schema
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
@PublicApi(since = "2.18.0")
public class WorkloadGroup extends AbstractDiffable<WorkloadGroup> implements ToXContentObject {

    private static final Logger logger = LogManager.getLogger(WorkloadGroup.class);

    public static final String _ID_STRING = "_id";
    public static final String NAME_STRING = "name";
    public static final String UPDATED_AT_STRING = "updated_at";
    private static final int MAX_CHARS_ALLOWED_IN_NAME = 50;
    private final String name;
    private final String _id;
    // It is an epoch in millis
    private final long updatedAtInMillis;
    private final MutableWorkloadGroupFragment mutableWorkloadGroupFragment;

    public WorkloadGroup(String name, MutableWorkloadGroupFragment mutableWorkloadGroupFragment) {
        this(name, UUIDs.randomBase64UUID(), mutableWorkloadGroupFragment, Instant.now().getMillis());
    }

    public WorkloadGroup(String name, String _id, MutableWorkloadGroupFragment mutableWorkloadGroupFragment, long updatedAt) {
        this(name, _id, mutableWorkloadGroupFragment, updatedAt, false);
    }

    private WorkloadGroup(
        String name,
        String _id,
        MutableWorkloadGroupFragment mutableWorkloadGroupFragment,
        long updatedAt,
        boolean deserializing
    ) {
        Objects.requireNonNull(name, "WorkloadGroup.name can't be null");
        Objects.requireNonNull(mutableWorkloadGroupFragment.getResourceLimits(), "WorkloadGroup.resourceLimits can't be null");
        Objects.requireNonNull(mutableWorkloadGroupFragment.getResiliencyMode(), "WorkloadGroup.resiliencyMode can't be null");
        Objects.requireNonNull(_id, "WorkloadGroup._id can't be null");
        validateName(name);

        if (mutableWorkloadGroupFragment.getResourceLimits().isEmpty()) {
            throw new IllegalArgumentException("WorkloadGroup.resourceLimits should at least have 1 resource limit");
        }
        if (updatedAt <= 0L) {
            throw new IllegalArgumentException("WorkloadGroup.updatedAtInMillis is not a valid epoch");
        }

        // Drop null-valued "clear" keys before storage (meaningful only during an update merge, not on create).
        Settings normalizedSettings = stripClearMarkers(mutableWorkloadGroupFragment.getSettings());
        Settings normalizedThrottling = stripClearMarkers(mutableWorkloadGroupFragment.getThrottling());
        if (normalizedSettings.equals(mutableWorkloadGroupFragment.getSettings()) == false
            || normalizedThrottling.equals(mutableWorkloadGroupFragment.getThrottling()) == false) {
            mutableWorkloadGroupFragment = new MutableWorkloadGroupFragment(
                mutableWorkloadGroupFragment.getResiliencyMode(),
                mutableWorkloadGroupFragment.getResourceLimits(),
                normalizedSettings,
                normalizedThrottling
            );
        }

        // Cross-field checks on the merged throttling config (attribute required with a limit; ceiling must be >= 1).
        // On the deserialization path these are advisory: a newer node may legitimately relax them (e.g. by adding a
        // second limit key), and throwing while applying published cluster state would wedge this node out of the
        // cluster rather than reject one API call. Enforcement fails open on config it cannot interpret.
        if (deserializing) {
            try {
                WorkloadGroupThrottleSettings.validateMergedConfig(mutableWorkloadGroupFragment.getThrottling());
            } catch (IllegalArgumentException e) {
                logger.warn(
                    "Accepting workload group [{}] with a throttling config this node considers invalid ({}); "
                        + "throttling will not be enforced for it here",
                    name,
                    e.getMessage()
                );
            }
        } else {
            WorkloadGroupThrottleSettings.validateMergedConfig(mutableWorkloadGroupFragment.getThrottling());
        }

        this.name = name;
        this._id = _id;
        this.mutableWorkloadGroupFragment = mutableWorkloadGroupFragment;
        this.updatedAtInMillis = updatedAt;
    }

    public static boolean isValid(long updatedAt) {
        long minValidTimestamp = Instant.ofEpochMilli(0L).getMillis();

        // Use Instant.now() to get the current time in seconds since epoch
        long currentSeconds = Instant.now().getMillis();

        // Check if the timestamp is within a reasonable range
        return minValidTimestamp <= updatedAt && updatedAt <= currentSeconds;
    }

    public WorkloadGroup(StreamInput in) throws IOException {
        this(in.readString(), in.readString(), new MutableWorkloadGroupFragment(in), in.readLong(), true);
    }

    public static WorkloadGroup updateExistingWorkloadGroup(
        WorkloadGroup existingGroup,
        MutableWorkloadGroupFragment mutableWorkloadGroupFragment
    ) {
        final Map<ResourceType, Double> updatedResourceLimits = new HashMap<>(existingGroup.getResourceLimits());
        final Map<ResourceType, Double> mutableFragmentResourceLimits = mutableWorkloadGroupFragment.getResourceLimits();
        if (mutableFragmentResourceLimits != null && !mutableFragmentResourceLimits.isEmpty()) {
            updatedResourceLimits.putAll(mutableFragmentResourceLimits);
        }
        final ResiliencyMode mode = Optional.ofNullable(mutableWorkloadGroupFragment.getResiliencyMode())
            .orElse(existingGroup.getResiliencyMode());
        final Settings updatedSettings = mergeSettings(existingGroup.getSettings(), mutableWorkloadGroupFragment.getSettings());
        final Settings updatedThrottling = mergeSettings(
            existingGroup.getMutableWorkloadGroupFragment().getThrottling(),
            mutableWorkloadGroupFragment.getThrottling()
        );
        return new WorkloadGroup(
            existingGroup.getName(),
            existingGroup.get_id(),
            new MutableWorkloadGroupFragment(mode, updatedResourceLimits, updatedSettings, updatedThrottling),
            Instant.now().getMillis()
        );
    }

    /**
     * Drops null-valued keys from a settings bag before storage. A null value is the API gesture for "clear this key",
     * which only carries meaning during an update merge (which consumes it); any that reach a persisted group, e.g. a
     * null sent on create where there is nothing to clear, are dropped so stored config never contains a null value.
     *
     * @param s the settings to normalize (may be null)
     * @return the settings with all null-valued keys removed, or empty if {@code s} is null
     */
    private static Settings stripClearMarkers(Settings s) {
        if (s == null) {
            return Settings.EMPTY;
        }
        Settings.Builder builder = Settings.builder();
        for (String key : s.keySet()) {
            String value = s.get(key);
            if (value != null) {
                builder.put(key, value);
            }
        }
        return builder.build();
    }

    /**
     * Merges an incoming settings bag from an update request onto the existing one:
     * a null incoming bag (field absent) keeps existing, an empty incoming bag clears all, and a non-empty bag overlays
     * its values with a per-key null value clearing that key.
     *
     * @param existing the currently stored settings
     * @param incoming the settings from the update request (may be null)
     * @return the merged settings
     */
    private static Settings mergeSettings(Settings existing, Settings incoming) {
        if (incoming == null) {
            return Settings.builder().put(existing).build();
        }
        if (incoming.isEmpty()) {
            return Settings.EMPTY;
        }
        Settings.Builder builder = Settings.builder().put(existing);
        for (String key : incoming.keySet()) {
            String value = incoming.get(key);
            if (value == null) {
                builder.remove(key);
            } else {
                builder.put(key, value);
            }
        }
        return builder.build();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeString(_id);
        mutableWorkloadGroupFragment.writeTo(out);
        out.writeLong(updatedAtInMillis);
    }

    public static void validateName(String name) {
        if (name == null || name.isEmpty() || name.length() > MAX_CHARS_ALLOWED_IN_NAME) {
            throw new IllegalArgumentException("WorkloadGroup.name shouldn't be null, empty or more than 50 chars long");
        }
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        builder.field(_ID_STRING, _id);
        builder.field(NAME_STRING, name);
        for (String fieldName : MutableWorkloadGroupFragment.acceptedFieldNames) {
            mutableWorkloadGroupFragment.writeField(builder, fieldName);
        }
        builder.field(UPDATED_AT_STRING, updatedAtInMillis);
        builder.endObject();
        return builder;
    }

    public static WorkloadGroup fromXContent(final XContentParser parser) throws IOException {
        return Builder.fromXContent(parser).build();
    }

    public static Diff<WorkloadGroup> readDiff(final StreamInput in) throws IOException {
        return readDiffFrom(WorkloadGroup::new, in);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WorkloadGroup that = (WorkloadGroup) o;
        return Objects.equals(name, that.name)
            && Objects.equals(mutableWorkloadGroupFragment, that.mutableWorkloadGroupFragment)
            && Objects.equals(_id, that._id)
            && updatedAtInMillis == that.updatedAtInMillis;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, mutableWorkloadGroupFragment, updatedAtInMillis, _id);
    }

    public String getName() {
        return name;
    }

    public MutableWorkloadGroupFragment getMutableWorkloadGroupFragment() {
        return mutableWorkloadGroupFragment;
    }

    public ResiliencyMode getResiliencyMode() {
        return getMutableWorkloadGroupFragment().getResiliencyMode();
    }

    public Map<ResourceType, Double> getResourceLimits() {
        return getMutableWorkloadGroupFragment().getResourceLimits();
    }

    @ExperimentalApi
    public Settings getSettings() {
        return getMutableWorkloadGroupFragment().getSettings();
    }

    /**
     * @deprecated Use {@link #getSettings()} instead. This method exists only for binary compatibility
     * with 3.6.x clients and will be removed in a future major version.
     */
    @Deprecated
    public Map<String, String> getSearchSettings() {
        Settings s = getSettings();
        Map<String, String> map = new HashMap<>();
        for (String key : s.keySet()) {
            map.put(key, s.get(key));
        }
        return map;
    }

    public String get_id() {
        return _id;
    }

    public long getUpdatedAtInMillis() {
        return updatedAtInMillis;
    }

    /**
     * builder method for the {@link WorkloadGroup}
     * @return Builder object
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder class for {@link WorkloadGroup}
     */
    @ExperimentalApi
    public static class Builder {
        private String name;
        private String _id;
        private MutableWorkloadGroupFragment mutableWorkloadGroupFragment;
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
            MutableWorkloadGroupFragment mutableWorkloadGroupFragment1 = new MutableWorkloadGroupFragment();
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    fieldName = parser.currentName();
                } else if (token.isValue()) {
                    if (fieldName.equals(_ID_STRING)) {
                        builder._id(parser.text());
                    } else if (fieldName.equals(NAME_STRING)) {
                        builder.name(parser.text());
                    } else if (MutableWorkloadGroupFragment.shouldParse(fieldName)) {
                        mutableWorkloadGroupFragment1.parseField(parser, fieldName);
                    } else if (fieldName.equals(UPDATED_AT_STRING)) {
                        builder.updatedAt(parser.longValue());
                    } else {
                        throw new IllegalArgumentException(fieldName + " is not a valid field in WorkloadGroup");
                    }
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if (!MutableWorkloadGroupFragment.shouldParse(fieldName)) {
                        throw new IllegalArgumentException(fieldName + " is not a valid object in WorkloadGroup");
                    }
                    mutableWorkloadGroupFragment1.parseField(parser, fieldName);
                } else if (token == XContentParser.Token.VALUE_NULL) {
                    if (fieldName.equals(MutableWorkloadGroupFragment.SETTINGS_STRING)
                        || fieldName.equals(MutableWorkloadGroupFragment.THROTTLING_STRING)) {
                        mutableWorkloadGroupFragment1.parseField(parser, fieldName);
                    }
                }
            }
            return builder.mutableWorkloadGroupFragment(mutableWorkloadGroupFragment1);
        }

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder _id(String _id) {
            this._id = _id;
            return this;
        }

        public Builder mutableWorkloadGroupFragment(MutableWorkloadGroupFragment mutableWorkloadGroupFragment) {
            this.mutableWorkloadGroupFragment = mutableWorkloadGroupFragment;
            return this;
        }

        public Builder updatedAt(long updatedAt) {
            this.updatedAt = updatedAt;
            return this;
        }

        public WorkloadGroup build() {
            return new WorkloadGroup(name, _id, mutableWorkloadGroupFragment, updatedAt);
        }

        public MutableWorkloadGroupFragment getMutableWorkloadGroupFragment() {
            return mutableWorkloadGroupFragment;
        }
    }
}
