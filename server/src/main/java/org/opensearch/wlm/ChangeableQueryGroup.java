/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.cluster.AbstractDiffable;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * Class to hold the fields that can be updated in a QueryGroup.
 */
@ExperimentalApi
public class ChangeableQueryGroup extends AbstractDiffable<ChangeableQueryGroup> {

    public static final String RESILIENCY_MODE_STRING = "resiliency_mode";
    public static final String RESOURCE_LIMITS_STRING = "resource_limits";
    private ResiliencyMode resiliencyMode;
    private Map<ResourceType, Double> resourceLimits;

    public static final List<String> acceptedFieldNames = List.of(RESILIENCY_MODE_STRING, RESOURCE_LIMITS_STRING);
    private final Map<String, Function<XContentParser, Void>> fromXContentMap = Map.of(RESILIENCY_MODE_STRING, (parser) -> {
        try {
            setResiliencyMode(ResiliencyMode.fromName(parser.text()));
            return null;
        } catch (IOException e) {
            throw new IllegalArgumentException("parsing error encountered for the field " + RESILIENCY_MODE_STRING);
        }
    }, RESOURCE_LIMITS_STRING, (parser) -> {
        try {
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
            setResourceLimits(resourceLimits);
            return null;
        } catch (IOException e) {
            throw new IllegalArgumentException("parsing error encountered for the object " + RESOURCE_LIMITS_STRING);
        }
    });

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
    });

    public ChangeableQueryGroup() {}

    public ChangeableQueryGroup(ResiliencyMode resiliencyMode, Map<ResourceType, Double> resourceLimits) {
        validateResourceLimits(resourceLimits);
        this.resiliencyMode = resiliencyMode;
        this.resourceLimits = resourceLimits;
    }

    public ChangeableQueryGroup(StreamInput in) throws IOException {
        if (in.readBoolean()) {
            resourceLimits = in.readMap((i) -> ResourceType.fromName(i.readString()), StreamInput::readDouble);
        } else {
            resourceLimits = new HashMap<>();
        }
        String updatedResiliencyMode = in.readOptionalString();
        resiliencyMode = updatedResiliencyMode == null ? null : ResiliencyMode.fromName(updatedResiliencyMode);
    }

    public static boolean shouldParse(String field) {
        return acceptedFieldNames.contains(field);
    }

    public void parseField(XContentParser parser, String field) {
        fromXContentMap.get(field).apply(parser);
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
        ChangeableQueryGroup that = (ChangeableQueryGroup) o;
        return Objects.equals(resiliencyMode, that.resiliencyMode) && Objects.equals(resourceLimits, that.resourceLimits);
    }

    @Override
    public int hashCode() {
        return Objects.hash(resiliencyMode, resourceLimits);
    }

    public ResiliencyMode getResiliencyMode() {
        return resiliencyMode;
    }

    public Map<ResourceType, Double> getResourceLimits() {
        return resourceLimits;
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

    public void setResiliencyMode(ResiliencyMode resiliencyMode) {
        this.resiliencyMode = resiliencyMode;
    }

    public void setResourceLimits(Map<ResourceType, Double> resourceLimits) {
        validateResourceLimits(resourceLimits);
        this.resourceLimits = resourceLimits;
    }
}
