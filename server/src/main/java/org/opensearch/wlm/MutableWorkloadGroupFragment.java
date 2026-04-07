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

/**
 * Class to hold the fields that can be updated in a WorkloadGroup.
 */
@ExperimentalApi
public class MutableWorkloadGroupFragment extends AbstractDiffable<MutableWorkloadGroupFragment> {

    public static final String RESILIENCY_MODE_STRING = "resiliency_mode";
    public static final String RESOURCE_LIMITS_STRING = "resource_limits";
    public static final String SETTINGS_STRING = "settings";
    private ResiliencyMode resiliencyMode;
    private Map<ResourceType, Double> resourceLimits;
    private Settings settings;

    public static final List<String> acceptedFieldNames = List.of(RESILIENCY_MODE_STRING, RESOURCE_LIMITS_STRING, SETTINGS_STRING);

    public MutableWorkloadGroupFragment() {}

    /**
     * Constructor for tests only. Production code should use the full constructor below.
     */
    public MutableWorkloadGroupFragment(ResiliencyMode resiliencyMode, Map<ResourceType, Double> resourceLimits) {
        this(resiliencyMode, resourceLimits, Settings.EMPTY);
    }

    public MutableWorkloadGroupFragment(ResiliencyMode resiliencyMode, Map<ResourceType, Double> resourceLimits, Settings settings) {
        validateResourceLimits(resourceLimits);
        WorkloadGroupSearchSettings.validate(settings);
        this.resiliencyMode = resiliencyMode;
        this.resourceLimits = resourceLimits;
        this.settings = settings;
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
            // New 3.7 format: null marker boolean then Settings object
            boolean isNull = in.readBoolean();
            settings = isNull ? null : Settings.readSettingsFromStream(in);
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
            Map<String, String> rawMap = parser.mapStrings();
            Settings.Builder builder = Settings.builder();
            for (Map.Entry<String, String> entry : rawMap.entrySet()) {
                builder.put(entry.getKey(), entry.getValue());
            }
            Settings settings = builder.build();
            WorkloadGroupSearchSettings.validate(settings);
            return settings;
        }
    }

    static class FieldParserFactory {
        static Optional<FieldParser<?>> fieldParserFor(String fieldName) {
            return switch (fieldName) {
                case RESILIENCY_MODE_STRING -> Optional.of(new ResiliencyModeParser());
                case RESOURCE_LIMITS_STRING -> Optional.of(new ResourceLimitsParser());
                case SETTINGS_STRING -> Optional.of(new SearchSettingsParser());
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
            Settings s = settings != null ? settings : Settings.EMPTY;
            Map<String, String> sortedSettingsMap = new TreeMap<>();
            for (String key : s.keySet()) {
                sortedSettingsMap.put(key, s.get(key));
            }
            for (Map.Entry<String, String> e : sortedSettingsMap.entrySet()) {
                builder.field(e.getKey(), e.getValue());
            }
            builder.endObject();
            return null;
        } catch (IOException e) {
            throw new IllegalStateException("writing error encountered for the field " + SETTINGS_STRING);
        }
    });

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
            // New 3.7 format: null marker boolean then Settings object
            out.writeBoolean(settings == null);
            if (settings != null) {
                Settings.writeSettingsToStream(settings, out);
            }
        } else if (out.getVersion().onOrAfter(Version.V_3_6_0)) {
            // Legacy 3.6 format: write empty map (experimental API, settings not preserved across versions)
            out.writeBoolean(false);
            out.writeMap(Map.of(), StreamOutput::writeString, StreamOutput::writeString);
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
            && Objects.equals(settings, that.settings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(resiliencyMode, resourceLimits, settings);
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
        this.settings = settings;
    }
}
