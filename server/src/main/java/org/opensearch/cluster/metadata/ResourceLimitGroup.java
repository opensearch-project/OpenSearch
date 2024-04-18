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
 *     ]，
 *     "enforcement": "monitor"
 * }
 */
@ExperimentalApi
public class ResourceLimitGroup extends AbstractDiffable<ResourceLimitGroup> implements ToXContentObject {

    private final String name;
    private final List<ResourceLimit> resourceLimits;
    private final String enforcement;

    private static final List<String> ALLOWED_RESOURCES = List.of("jvm");
    private static final List<String> ALLOWED_ENFORCEMENTS = List.of("monitor");

    public static final ParseField NAME_FIELD = new ParseField("name");
    public static final ParseField RESOURCE_LIMITS_FIELD = new ParseField("resourceLimits");
    public static final ParseField ENFORCEMENT_FIELD = new ParseField("enforcement");


    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<ResourceLimitGroup, Void> PARSER = new ConstructingObjectParser<>(
        "ResourceLimitGroupParser",
        args -> new ResourceLimitGroup((String) args[0], (List<ResourceLimit>) args[1], (String) args[2])
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), NAME_FIELD);
        PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), (p, c) -> ResourceLimit.fromXContent(p), RESOURCE_LIMITS_FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), ENFORCEMENT_FIELD);
    }

    private static final ConstructingObjectParser<ResourceLimitGroup, Void> PARSER_OPTIONAL_FIELDS = new ConstructingObjectParser<>(
        "ResourceLimitGroupParser",
        args -> new ResourceLimitGroup((String) args[0], (List<ResourceLimit>) args[1], (String) args[2])
    );

    static {
        PARSER_OPTIONAL_FIELDS.declareString(ConstructingObjectParser.optionalConstructorArg(), NAME_FIELD);
        PARSER_OPTIONAL_FIELDS.declareObjectArray(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> ResourceLimit.fromXContent(p), RESOURCE_LIMITS_FIELD);
        PARSER_OPTIONAL_FIELDS.declareString(ConstructingObjectParser.optionalConstructorArg(), ENFORCEMENT_FIELD);
    }

    public ResourceLimitGroup(String name, List<ResourceLimit> resourceLimits, String enforcement) {
        isValidResourceLimitGroup(name, enforcement);
        this.name = name;
        this.resourceLimits = resourceLimits;
        this.enforcement = enforcement;
    }

    public ResourceLimitGroup(StreamInput in) throws IOException {
        this(in.readString(), in.readList(ResourceLimit::new), in.readString());
    }

    private void isValidResourceLimitGroup(String name, String enforcement) {
        if (name != null) {
            if (name.isEmpty()) {
                throw new IllegalArgumentException("Resource Limit Group name cannot be empty");
            }
            if (name.startsWith("-") || name.startsWith("_")) {
                throw new IllegalArgumentException("Resource Limit Group name cannot start with '_' or '-'.");
            }
            if (!name.toLowerCase(Locale.ROOT).equals(name)) {
                throw new IllegalArgumentException("Resource Limit Group name must be lowercase");
            }
            if (name.matches(".*[ ,:\"*+/\\\\|?#><].*")) {
                throw new IllegalArgumentException(
                    "Resource Limit Group names can't contain spaces, commas, quotes, slashes, :, *, +, |, ?, #, >, or <"
                );
            }
        }
        if (enforcement != null) {
            if (!ALLOWED_ENFORCEMENTS.contains(enforcement)) {
                throw new IllegalArgumentException("enforcement has to be valid, valid enforcements "
                    + ALLOWED_ENFORCEMENTS.stream().reduce((x, e) -> x + ", " + e).get());
            }
        }
    }

    /**
     * Class to hold the system resource limits;
     * sample Schema
     *
     */
    @ExperimentalApi
    public static class ResourceLimit implements Writeable, ToXContentObject {
        private final String resourceName;
        private Double value;

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
            isValidResourceLimit(resourceName, value);
            this.resourceName = resourceName;
            this.value = value;
        }

        public ResourceLimit(StreamInput in) throws IOException {
            this(in.readString(), in.readDouble());
        }

        private static void isValidResourceLimit(String resourceName, double value) {
            if (value < 0 || value > 1) {
                throw new IllegalArgumentException("Resource limit value should be between 0 and 1.");
            }
            String str = String.valueOf(value);
            if (str.contains(".") && str.split("\\.")[1].length() > 2) {
                throw new IllegalArgumentException("Resource limit value should have at most two digits after the decimal point");
            }
            if (!ALLOWED_RESOURCES.contains(resourceName.toLowerCase())) {
                throw new IllegalArgumentException("resource has to be valid, valid resources "
                    + ALLOWED_RESOURCES.stream().reduce((x, e) -> x + ", " + e).get());
            }
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

        public String getResourceName() {
            return resourceName;
        }

        public Double getValue() {
            return value;
        }

        public void setValue(Double value) {
            this.value = value;
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
    }

    /**
     * Write this into the {@linkplain StreamOutput}.
     *
     * @param out
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        writeToOutputStream(out, name, resourceLimits, enforcement);
    }

    public static void writeToOutputStream(StreamOutput out, String name, List<ResourceLimit> resourceLimits, String enforcement) throws IOException {
        out.writeString(name);
        out.writeList(resourceLimits);
        out.writeString(enforcement);
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
        builder.field(ENFORCEMENT_FIELD.getPreferredName(), enforcement);
        builder.endObject();
        return builder;
    }

    public static ResourceLimitGroup fromXContent(final XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    public static ResourceLimitGroup fromXContentOptionalFields(final XContentParser parser) throws IOException {
        return PARSER_OPTIONAL_FIELDS.parse(parser, null);
    }

    public static Diff<ResourceLimitGroup> readDiff(final StreamInput in) throws IOException {
        return readDiffFrom(ResourceLimitGroup::new, in);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ResourceLimitGroup that = (ResourceLimitGroup) o;
        return Objects.equals(name, that.name) && Objects.equals(resourceLimits, that.resourceLimits) && Objects.equals(enforcement, that.enforcement);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, resourceLimits, enforcement);
    }

    public String getName() {
        return name;
    }

    public List<ResourceLimit> getResourceLimits() {
        return resourceLimits;
    }

    public String getEnforcement() {
        return enforcement;
    }
}
