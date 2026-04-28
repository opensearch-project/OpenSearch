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
    public static final String SEARCH_SETTINGS_STRING = "search_settings";
    private ResiliencyMode resiliencyMode;
    private Map<ResourceType, Double> resourceLimits;
    private Map<String, String> searchSettings;

    public static final List<String> acceptedFieldNames = List.of(RESILIENCY_MODE_STRING, RESOURCE_LIMITS_STRING, SEARCH_SETTINGS_STRING);

    public MutableWorkloadGroupFragment() {}

    /**
     * Constructor for tests only. Production code should use the full constructor below.
     */
    public MutableWorkloadGroupFragment(ResiliencyMode resiliencyMode, Map<ResourceType, Double> resourceLimits) {
        this(resiliencyMode, resourceLimits, new HashMap<>());
    }

    public MutableWorkloadGroupFragment(
        ResiliencyMode resiliencyMode,
        Map<ResourceType, Double> resourceLimits,
        Map<String, String> searchSettings
    ) {
        validateResourceLimits(resourceLimits);
        WorkloadGroupSearchSettings.validateSearchSettings(searchSettings);
        this.resiliencyMode = resiliencyMode;
        this.resourceLimits = resourceLimits;
        this.searchSettings = searchSettings;
    }

    public MutableWorkloadGroupFragment(StreamInput in) throws IOException {
        if (in.readBoolean()) {
            resourceLimits = in.readMap((i) -> ResourceType.fromName(i.readString()), StreamInput::readDouble);
        } else {
            resourceLimits = new HashMap<>();
        }
        String updatedResiliencyMode = in.readOptionalString();
        resiliencyMode = updatedResiliencyMode == null ? null : ResiliencyMode.fromName(updatedResiliencyMode);
        if (in.getVersion().onOrAfter(Version.V_3_6_0)) {
            // Read null marker: true means searchSettings is null (not specified)
            boolean isNull = in.readBoolean();
            searchSettings = isNull ? null : in.readMap(StreamInput::readString, StreamInput::readString);
        } else {
            searchSettings = new HashMap<>();
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

    static class SearchSettingsParser implements FieldParser<Map<String, String>> {
        public Map<String, String> parseField(XContentParser parser) throws IOException {
            return parser.mapStrings();
        }
    }

    static class FieldParserFactory {
        static Optional<FieldParser<?>> fieldParserFor(String fieldName) {
            return switch (fieldName) {
                case RESILIENCY_MODE_STRING -> Optional.of(new ResiliencyModeParser());
                case RESOURCE_LIMITS_STRING -> Optional.of(new ResourceLimitsParser());
                case SEARCH_SETTINGS_STRING -> Optional.of(new SearchSettingsParser());
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
    }, SEARCH_SETTINGS_STRING, (builder) -> {
        try {
            builder.startObject(SEARCH_SETTINGS_STRING);
            Map<String, String> settings = searchSettings != null ? searchSettings : Map.of();
            Map<String, String> sortedSettingsMap = new TreeMap<>(settings);
            for (Map.Entry<String, ?> e : sortedSettingsMap.entrySet()) {
                builder.field(e.getKey(), e.getValue());
            }
            builder.endObject();
            return null;
        } catch (IOException e) {
            throw new IllegalStateException("writing error encountered for the field " + SEARCH_SETTINGS_STRING);
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
                    case SEARCH_SETTINGS_STRING -> setSearchSettings((Map<String, String>) value);
                }
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
        if (out.getVersion().onOrAfter(Version.V_3_6_0)) {
            out.writeBoolean(searchSettings == null);
            if (searchSettings != null) {
                out.writeMap(searchSettings, StreamOutput::writeString, StreamOutput::writeString);
            }
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
            && Objects.equals(searchSettings, that.searchSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(resiliencyMode, resourceLimits, searchSettings);
    }

    public ResiliencyMode getResiliencyMode() {
        return resiliencyMode;
    }

    public Map<ResourceType, Double> getResourceLimits() {
        return resourceLimits;
    }

    public Map<String, String> getSearchSettings() {
        return searchSettings;
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

    void setSearchSettings(Map<String, String> searchSettings) {
        WorkloadGroupSearchSettings.validateSearchSettings(searchSettings);
        this.searchSettings = searchSettings;
    }
}
