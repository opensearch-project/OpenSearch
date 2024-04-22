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
import java.util.Locale;
import java.util.Objects;

/**
 * Class to define the ResourceLimitGroup schema
 * {
 *     "name": "analytics",
 *     "resourceLimits": [
 *          {
 *              "resourceName": "jvm",
 *              "value": 0.4
 *          }
 *     ]
 * }
 */
@ExperimentalApi
public class ResourceLimitGroup extends AbstractDiffable<ResourceLimitGroup> implements ToXContentObject {

    public static final int MAX_CHARS_ALLOWED_IN_NAME = 50;
    private final String name;
    private final List<ResourceLimit> resourceLimits;
    private final ResourceLimitGroupMode mode;

    private static final List<String> ALLOWED_RESOURCES = List.of("jvm");

    public static final ParseField NAME_FIELD = new ParseField("name");
    public static final ParseField RESOURCE_LIMITS_FIELD = new ParseField("resourceLimits");
    public static final ParseField MODE_FIELD = new ParseField("mode");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<ResourceLimitGroup, Void> PARSER = new ConstructingObjectParser<>(
        "ResourceLimitGroupParser",
        args -> new ResourceLimitGroup((String) args[0], (List<ResourceLimit>) args[1], (String) args[2])
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), NAME_FIELD);
        PARSER.declareObjectArray(
            ConstructingObjectParser.constructorArg(),
            (p, c) -> ResourceLimit.fromXContent(p),
            RESOURCE_LIMITS_FIELD
        );
        PARSER.declareString(ConstructingObjectParser.constructorArg(), MODE_FIELD);
    }

    public ResourceLimitGroup(final String name, final List<ResourceLimit> resourceLimits, final String modeName) {
        Objects.requireNonNull(name, "ResourceLimitGroup.name can't be null");
        Objects.requireNonNull(resourceLimits, "ResourceLimitGroup.resourceLimits can't be null");

        if (name.length() > MAX_CHARS_ALLOWED_IN_NAME) {
            throw new IllegalArgumentException("ResourceLimitGroup.name shouldn't be more than 50 chars long");
        }

        if (resourceLimits.isEmpty()) {
            throw new IllegalArgumentException("ResourceLimitGroup.resourceLimits should at least have 1 resource limit");
        }

        this.name = name;
        this.resourceLimits = resourceLimits;
        this.mode = ResourceLimitGroupMode.fromName(modeName);
    }

    public ResourceLimitGroup(StreamInput in) throws IOException {
        this(in.readString(), in.readList(ResourceLimit::new), in.readString());
    }

    /**
     * Class to hold the system resource limits;
     * sample Schema
     *
     */
    @ExperimentalApi
    public static class ResourceLimit implements Writeable, ToXContentObject {
        private final String resourceName;
        private final Double value;

        static final ParseField RESOURCE_NAME_FIELD = new ParseField("resourceName");
        static final ParseField RESOURCE_VALUE_FIELD = new ParseField("value");

        public static final ConstructingObjectParser<ResourceLimit, Void> PARSER = new ConstructingObjectParser<>(
            "ResourceLimitParser",
            args -> new ResourceLimit((String) args[0], (Double) args[1])
        );

        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), RESOURCE_NAME_FIELD);
            PARSER.declareDouble(ConstructingObjectParser.constructorArg(), RESOURCE_VALUE_FIELD);
        }

        public ResourceLimit(String resourceName, Double value) {
            Objects.requireNonNull(resourceName, "resourceName can't be null");
            Objects.requireNonNull(value, "resource value can't be null");

            if (Double.compare(value, 1.0) > 0) {
                throw new IllegalArgumentException("resource value should be less than 1.0");
            }

            if (!ALLOWED_RESOURCES.contains(resourceName.toLowerCase(Locale.ROOT))) {
                throw new IllegalArgumentException(
                    "resource has to be valid, valid resources " + ALLOWED_RESOURCES.stream().reduce((x, e) -> x + ", " + e).get()
                );
            }
            this.resourceName = resourceName;
            this.value = value;
        }

        public ResourceLimit(StreamInput in) throws IOException {
            this(in.readString(), in.readDouble());
        }

        /**
         * Write this into the {@linkplain StreamOutput}.
         *
         * @param out
         */
        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(resourceName);
            out.writeDouble(value);
        }

        /**
         * @param builder
         * @param params
         * @return
         * @throws IOException
         */
        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(RESOURCE_NAME_FIELD.getPreferredName(), resourceName);
            builder.field(RESOURCE_VALUE_FIELD.getPreferredName(), value);
            builder.endObject();
            return builder;
        }

        public static ResourceLimit fromXContent(final XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ResourceLimit that = (ResourceLimit) o;
            return Objects.equals(resourceName, that.resourceName) && Objects.equals(value, that.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(resourceName, value);
        }

        public String getResourceName() {
            return resourceName;
        }

        public Double getValue() {
            return value;
        }
    }

    /**
     * This enum models the different sandbox modes
     */
    @ExperimentalApi
    public static enum ResourceLimitGroupMode {
        SOFT("soft"),
        ENFORCED("enforced"),
        MONITOR("monitor");

        private final String name;

        ResourceLimitGroupMode(String mode) {
            this.name = mode;
        }

        public String getName() {
            return name;
        }

        public static ResourceLimitGroupMode fromName(String s) {
            switch (s) {
                case "soft":
                    return SOFT;
                case "enforced":
                    return ENFORCED;
                case "monitor":
                    return MONITOR;
                default:
                    throw new IllegalArgumentException("Invalid value for ResourceLimitGroupMode: " + s);
            }
        }

    }

    /**
     * Write this into the {@linkplain StreamOutput}.
     *
     * @param out
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeList(resourceLimits);
        out.writeString(mode.getName());
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
        builder.field(NAME_FIELD.getPreferredName(), name);
        builder.field(RESOURCE_LIMITS_FIELD.getPreferredName(), resourceLimits);
        builder.field(MODE_FIELD.getPreferredName(), mode.getName());
        builder.endObject();
        return builder;
    }

    public static ResourceLimitGroup fromXContent(final XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    public static Diff<ResourceLimitGroup> readDiff(final StreamInput in) throws IOException {
        return readDiffFrom(ResourceLimitGroup::new, in);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ResourceLimitGroup that = (ResourceLimitGroup) o;
        return Objects.equals(name, that.name) && Objects.equals(resourceLimits, that.resourceLimits);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, resourceLimits);
    }

    public String getName() {
        return name;
    }

    public ResourceLimitGroupMode getMode() {
        return mode;
    }

    public List<ResourceLimit> getResourceLimits() {
        return resourceLimits;
    }

    public ResourceLimit getResourceLimitFor(String resourceName) {
        return resourceLimits.stream()
            .filter(resourceLimit -> resourceLimit.getResourceName().equals(resourceName))
            .findFirst()
            .orElseGet(() -> new ResourceLimit(resourceName, 100.0));
    }
}
