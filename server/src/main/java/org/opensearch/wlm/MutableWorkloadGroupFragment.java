/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.Version;
import org.opensearch.cluster.AbstractDiffable;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Class to hold the fields that can be updated in a WorkloadGroup.
 */
@ExperimentalApi
public class MutableWorkloadGroupFragment extends AbstractDiffable<MutableWorkloadGroupFragment> {

    public static final String RESILIENCY_MODE_STRING = "resiliency_mode";
    public static final String RESOURCE_LIMITS_STRING = "resource_limits";
    public static final String SETTINGS_STRING = "settings";
    public static final String THROTTLING_STRING = "throttling";
    public static final String QUEUE_STRING = "queue";
    private ResiliencyMode resiliencyMode;
    private Map<ResourceType, Double> resourceLimits;
    private Settings settings;
    private Settings throttling;
    private Settings queue;

    public static final List<String> acceptedFieldNames = List.of(
        RESILIENCY_MODE_STRING,
        RESOURCE_LIMITS_STRING,
        SETTINGS_STRING,
        THROTTLING_STRING,
        QUEUE_STRING
    );

    public MutableWorkloadGroupFragment() {}

    /**
     * Constructor for tests only. Production code should use the full constructor below.
     */
    public MutableWorkloadGroupFragment(ResiliencyMode resiliencyMode, Map<ResourceType, Double> resourceLimits) {
        this(resiliencyMode, resourceLimits, Settings.EMPTY);
    }

    public MutableWorkloadGroupFragment(ResiliencyMode resiliencyMode, Map<ResourceType, Double> resourceLimits, Settings settings) {
        this(resiliencyMode, resourceLimits, settings, Settings.EMPTY);
    }

    public MutableWorkloadGroupFragment(
        ResiliencyMode resiliencyMode,
        Map<ResourceType, Double> resourceLimits,
        Settings settings,
        Settings throttling
    ) {
        this(resiliencyMode, resourceLimits, settings, throttling, Settings.EMPTY, true);
    }

    public MutableWorkloadGroupFragment(
        ResiliencyMode resiliencyMode,
        Map<ResourceType, Double> resourceLimits,
        Settings settings,
        Settings throttling,
        Settings queue
    ) {
        this(resiliencyMode, resourceLimits, settings, throttling, queue, true);
    }

    /**
     * @param queue    the queue bag, reserved for request queueing. Carried as is and never validated: this version only
     *                 parses, persists, emits and forwards it.
     * @param validate whether to eagerly validate the fields. Pass {@code false} only for internal reconstruction of an
     *                 already-parsed fragment (e.g. WorkloadGroup normalizing away null clear-markers), where throttling is
     *                 (re)checked by the WorkloadGroup constructor anyway.
     */
    public MutableWorkloadGroupFragment(
        ResiliencyMode resiliencyMode,
        Map<ResourceType, Double> resourceLimits,
        Settings settings,
        Settings throttling,
        Settings queue,
        boolean validate
    ) {
        if (validate) {
            validateResourceLimits(resourceLimits);
            WorkloadGroupSearchSettings.validate(settings);
            WorkloadGroupThrottleSettings.validate(throttling);
        }
        this.resiliencyMode = resiliencyMode;
        this.resourceLimits = resourceLimits;
        this.settings = settings != null ? settings : Settings.EMPTY;
        this.throttling = throttling != null ? throttling : Settings.EMPTY;
        this.queue = queue != null ? queue : Settings.EMPTY;
    }

    public MutableWorkloadGroupFragment(StreamInput in) throws IOException {
        if (in.readBoolean()) {
            resourceLimits = in.readMap((i) -> ResourceType.fromName(i.readString()), StreamInput::readDouble);
        } else {
            resourceLimits = new HashMap<>();
        }
        String updatedResiliencyMode = in.readOptionalString();
        resiliencyMode = updatedResiliencyMode == null ? null : ResiliencyMode.fromName(updatedResiliencyMode);
        if (in.getVersion().onOrAfter(Version.V_3_7_0)) {
            settings = Settings.readOptionalSettingsFromStream(in);
        } else if (in.getVersion().onOrAfter(Version.V_3_6_0)) {
            // Legacy 3.6 format: read and discard (experimental API, no backward compat guarantee)
            boolean isNull = in.readBoolean();
            if (isNull == false) {
                in.readMap(StreamInput::readString, StreamInput::readString);
            }
            settings = Settings.EMPTY;
        } else {
            settings = Settings.EMPTY;
        }
        // throttling and queue postdate settings, so they need their own gate; the queue format ships with throttling, so both
        // bags share it (queue trails throttling on the wire). Absent decodes to null ("keep existing"), not EMPTY, which on an
        // update fragment means "clear all throttling" (or the whole queue).
        if (in.getVersion().onOrAfter(WorkloadGroupThrottleSettings.MIN_SUPPORTED_VERSION)) {
            throttling = Settings.readOptionalSettingsFromStream(in);
            queue = Settings.readOptionalSettingsFromStream(in);
        } else {
            throttling = null;
            queue = null;
        }
    }

    interface FieldParser<T> {
        T parseField(XContentParser parser) throws IOException;
    }

    static class ResiliencyModeParser implements FieldParser<ResiliencyMode> {
        public ResiliencyMode parseField(XContentParser parser) throws IOException {
            return ResiliencyMode.fromName(parser.text());
        }
    }

    static class ResourceLimitsParser implements FieldParser<Map<ResourceType, Double>> {
        public Map<ResourceType, Double> parseField(XContentParser parser) throws IOException {
            String fieldName = "";
            XContentParser.Token token;
            final Map<ResourceType, Double> resourceLimits = new HashMap<>();
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    fieldName = parser.currentName();
                } else {
                    resourceLimits.put(ResourceType.fromName(fieldName), parser.doubleValue());
                }
            }
            return resourceLimits;
        }
    }

    static class SearchSettingsParser implements FieldParser<Settings> {
        public Settings parseField(XContentParser parser) throws IOException {
            // "settings": null means clear all settings
            if (parser.currentToken() == XContentParser.Token.VALUE_NULL) {
                return Settings.EMPTY;
            }
            Settings settings = Settings.fromXContent(parser);
            WorkloadGroupSearchSettings.validate(settings);
            return settings;
        }
    }

    // A nested Settings bag (throttling, queue): JSON null clears the whole bag. Not validated here: the gateway read shares
    // this parser and must stay lenient; WorkloadGroup's constructor validates throttling (see setThrottling/setQueue).
    static class SettingsBagParser implements FieldParser<Settings> {
        public Settings parseField(XContentParser parser) throws IOException {
            if (parser.currentToken() == XContentParser.Token.VALUE_NULL) {
                return Settings.EMPTY;
            }
            return Settings.fromXContent(parser);
        }
    }

    static class FieldParserFactory {
        static Optional<FieldParser<?>> fieldParserFor(String fieldName) {
            return switch (fieldName) {
                case RESILIENCY_MODE_STRING -> Optional.of(new ResiliencyModeParser());
                case RESOURCE_LIMITS_STRING -> Optional.of(new ResourceLimitsParser());
                case SETTINGS_STRING -> Optional.of(new SearchSettingsParser());
                case THROTTLING_STRING, QUEUE_STRING -> Optional.of(new SettingsBagParser());
                default -> Optional.empty();
            };
        }
    }

    private final Map<String, Function<XContentBuilder, Void>> toXContentMap = Map.of(RESILIENCY_MODE_STRING, (builder) -> {
        try {
            builder.field(RESILIENCY_MODE_STRING, resiliencyMode.getName());
            return null;
        } catch (IOException e) {
            throw new IllegalStateException("writing error encountered for the field " + RESILIENCY_MODE_STRING);
        }
    }, RESOURCE_LIMITS_STRING, (builder) -> {
        try {
            builder.startObject(RESOURCE_LIMITS_STRING);
            for (ResourceType resourceType : ResourceType.values()) {
                if (resourceLimits.containsKey(resourceType)) {
                    builder.field(resourceType.getName(), resourceLimits.get(resourceType));
                }
            }
            builder.endObject();
            return null;
        } catch (IOException e) {
            throw new IllegalStateException("writing error encountered for the field " + RESOURCE_LIMITS_STRING);
        }
    }, SETTINGS_STRING, (builder) -> {
        try {
            builder.startObject(SETTINGS_STRING);
            writeSettingsFields(builder, settings);
            builder.endObject();
            return null;
        } catch (IOException e) {
            throw new IllegalStateException("writing error encountered for the field " + SETTINGS_STRING);
        }
    }, THROTTLING_STRING, (builder) -> {
        try {
            // Unlike settings (always emitted as {}), throttling is omitted entirely when unset.
            Settings t = throttling != null ? throttling : Settings.EMPTY;
            if (t.isEmpty() == false) {
                builder.startObject(THROTTLING_STRING);
                writeSettingsBag(builder, t, WorkloadGroupThrottleSettings::isLimitKey);
                builder.endObject();
            }
            return null;
        } catch (IOException e) {
            throw new IllegalStateException("writing error encountered for the field " + THROTTLING_STRING);
        }
    }, QUEUE_STRING, (builder) -> {
        try {
            // Like throttling, queue config is omitted entirely when unset.
            Settings q = queue != null ? queue : Settings.EMPTY;
            if (q.isEmpty() == false) {
                builder.startObject(QUEUE_STRING);
                writeSettingsBag(builder, q, WorkloadGroupQueueSettings.SIZE_PER_BUCKET.getKey()::equals);
                builder.endObject();
            }
            return null;
        } catch (IOException e) {
            throw new IllegalStateException("writing error encountered for the field " + QUEUE_STRING);
        }
    });

    // Emit every stored key, not an allowlist: this is also the on-disk format, so an unemitted key is lost on restart.
    // Integer keys are emitted as JSON numbers; a value that isn't one (accepted leniently on deserialization, e.g. from a
    // newer peer) is emitted verbatim so writing the group (gateway persist, GET) never throws.
    private static void writeSettingsBag(XContentBuilder builder, Settings bag, Predicate<String> isIntegerKey) throws IOException {
        Map<String, String> sorted = new TreeMap<>();
        for (String key : bag.keySet()) {
            sorted.put(key, bag.get(key));
        }
        for (Map.Entry<String, String> e : sorted.entrySet()) {
            if (isIntegerKey.test(e.getKey()) && isInteger(e.getValue())) {
                builder.field(e.getKey(), Integer.parseInt(e.getValue()));
            } else {
                builder.field(e.getKey(), e.getValue());
            }
        }
    }

    private static boolean isInteger(String value) {
        if (value == null) {
            return false;
        }
        try {
            Integer.parseInt(value);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private static void writeSettingsFields(XContentBuilder builder, Settings s) throws IOException {
        Settings source = s != null ? s : Settings.EMPTY;
        Map<String, String> sorted = new TreeMap<>();
        for (String key : source.keySet()) {
            sorted.put(key, source.get(key));
        }
        for (Map.Entry<String, String> e : sorted.entrySet()) {
            builder.field(e.getKey(), e.getValue());
        }
    }

    public static boolean shouldParse(String field) {
        return FieldParserFactory.fieldParserFor(field).isPresent();
    }

    @SuppressWarnings("unchecked")
    public void parseField(XContentParser parser, String field) {
        FieldParserFactory.fieldParserFor(field).ifPresent(fieldParser -> {
            try {
                Object value = fieldParser.parseField(parser);
                switch (field) {
                    case RESILIENCY_MODE_STRING -> setResiliencyMode((ResiliencyMode) value);
                    case RESOURCE_LIMITS_STRING -> setResourceLimits((Map<ResourceType, Double>) value);
                    case SETTINGS_STRING -> setSettings((Settings) value);
                    case THROTTLING_STRING -> setThrottling((Settings) value);
                    case QUEUE_STRING -> setQueue((Settings) value);
                }
            } catch (IllegalArgumentException e) {
                throw e;
            } catch (IOException e) {
                throw new IllegalArgumentException(String.format(Locale.ROOT, "parsing error encountered for the field '%s'", field));
            }
        });
    }

    public void writeField(XContentBuilder builder, String field) {
        toXContentMap.get(field).apply(builder);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (resourceLimits == null || resourceLimits.isEmpty()) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeMap(resourceLimits, ResourceType::writeTo, StreamOutput::writeDouble);
        }
        out.writeOptionalString(resiliencyMode == null ? null : resiliencyMode.getName());
        if (out.getVersion().onOrAfter(Version.V_3_7_0)) {
            Settings.writeOptionalSettingsToStream(settings, out);
        } else if (out.getVersion().onOrAfter(Version.V_3_6_0)) {
            // Legacy 3.6 format: write empty map (experimental API, settings not preserved across versions)
            out.writeBoolean(false);
            out.writeMap(Map.of(), StreamOutput::writeString, StreamOutput::writeString);
        }
        // Mirrors the read path: only peers that support throttling expect the throttling and queue bags on the wire.
        if (out.getVersion().onOrAfter(WorkloadGroupThrottleSettings.MIN_SUPPORTED_VERSION)) {
            Settings.writeOptionalSettingsToStream(throttling, out);
            Settings.writeOptionalSettingsToStream(queue, out);
        }
    }

    public static void validateResourceLimits(Map<ResourceType, Double> resourceLimits) {
        if (resourceLimits == null) {
            return;
        }
        for (Map.Entry<ResourceType, Double> resource : resourceLimits.entrySet()) {
            Double threshold = resource.getValue();
            Objects.requireNonNull(resource.getKey(), "resourceName can't be null");
            Objects.requireNonNull(threshold, "resource limit threshold for" + resource.getKey().getName() + " : can't be null");

            if (Double.compare(threshold, 0.0) <= 0 || Double.compare(threshold, 1.0) > 0) {
                throw new IllegalArgumentException("resource value should be greater than 0 and less or equal to 1.0");
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MutableWorkloadGroupFragment that = (MutableWorkloadGroupFragment) o;
        return Objects.equals(resiliencyMode, that.resiliencyMode)
            && Objects.equals(resourceLimits, that.resourceLimits)
            && Objects.equals(settings, that.settings)
            && Objects.equals(throttling, that.throttling)
            && Objects.equals(queue, that.queue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(resiliencyMode, resourceLimits, settings, throttling, queue);
    }

    public ResiliencyMode getResiliencyMode() {
        return resiliencyMode;
    }

    public Map<ResourceType, Double> getResourceLimits() {
        return resourceLimits;
    }

    public Settings getSettings() {
        return settings;
    }

    public Settings getThrottling() {
        return throttling;
    }

    public Settings getQueue() {
        return queue;
    }

    /**
     * This enum models the different WorkloadGroup resiliency modes
     * SOFT - means that this workload group can consume more than workload group resource limits if node is not in duress
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
            throw new IllegalArgumentException("Invalid value for WorkloadGroupMode: " + s);
        }
    }

    void setResiliencyMode(ResiliencyMode resiliencyMode) {
        this.resiliencyMode = resiliencyMode;
    }

    void setResourceLimits(Map<ResourceType, Double> resourceLimits) {
        validateResourceLimits(resourceLimits);
        this.resourceLimits = resourceLimits;
    }

    void setSettings(Settings settings) {
        WorkloadGroupSearchSettings.validate(settings);
        this.settings = settings != null ? settings : Settings.EMPTY;
    }

    void setThrottling(Settings throttling) {
        // No per-key validation: the gateway read shares this parser and must stay lenient. WorkloadGroup's constructor validates.
        this.throttling = throttling != null ? throttling : Settings.EMPTY;
    }

    void setQueue(Settings queue) {
        // No validation, here or later: the queue is reserved for request queueing, so this version only carries it (parse,
        // persist, emit, forward). The gateway read shares this parser and must stay lenient anyway.
        this.queue = queue != null ? queue : Settings.EMPTY;
    }

}
