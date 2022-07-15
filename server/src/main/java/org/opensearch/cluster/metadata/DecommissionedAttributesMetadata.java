/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.OpenSearchParseException;
import org.opensearch.Version;
import org.opensearch.cluster.AbstractNamedDiffable;
import org.opensearch.cluster.NamedDiff;
import org.opensearch.cluster.decommission.DecommissionAttribute;
import org.opensearch.cluster.metadata.Metadata.Custom;
import org.opensearch.common.Nullable;
import org.opensearch.common.Strings;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;


/**
 * Contains metadata about decommissioned attributes
 *
 * @opensearch.internal
 */
public class DecommissionedAttributesMetadata extends AbstractNamedDiffable<Custom> implements Custom {

    public static final String TYPE = "decommissionedAttributes";

    private final List<DecommissionedAttributeMetadata> decommissionedAttributes;

    /**
     * Constructs new decommissioned attributes metadata
     *
     * @param decommissionedAttributes list of decommissionedAttribute
     */
    public DecommissionedAttributesMetadata(List<DecommissionedAttributeMetadata> decommissionedAttributes) {
        this.decommissionedAttributes = Collections.unmodifiableList(decommissionedAttributes);
    }

    /**
     * Creates a new instance that has the given attribute name moved to the given {@code values}.
     *
     * @param name                    attribute name
     * @param decommissionAttribute new decommissioned attribute metadata
     * @return new instance with updated attribute values
     */
    public DecommissionedAttributesMetadata withUpdatedAttributeValues(
        String name,
        DecommissionAttribute decommissionAttribute
    ) {
        int indexOfAttribute = -1;
        for (int i = 0; i < decommissionedAttributes.size(); i++) {
            if (decommissionedAttributes.get(i).name().equals(name)) {
                indexOfAttribute = i;
                break;
            }
        }
        if (indexOfAttribute < 0) {
            throw new IllegalArgumentException("Unknown attribute [" + name + "]");
        }
        final List<DecommissionedAttributeMetadata> updatedDecommissionedAttributes = new ArrayList<>(decommissionedAttributes);
        updatedDecommissionedAttributes.set(
            indexOfAttribute,
            new DecommissionedAttributeMetadata(decommissionedAttributes.get(indexOfAttribute), decommissionAttribute));
        return new DecommissionedAttributesMetadata(updatedDecommissionedAttributes);
    }

    /**
     * Returns list of currently decommissioned attributes
     *
     * @return list of decommissioned attributes
     */
    public List<DecommissionedAttributeMetadata> decommissionedAttributes() {
        return this.decommissionedAttributes;
    }

    /**
     * Returns a decommissioned attribute with a given name or null if such attribute doesn't exist
     *
     * @param name name of decommissioned attribute
     * @return decommissioned attribute metadata
     */
    public DecommissionedAttributeMetadata decommissionedAttribute(String name) {
        for (DecommissionedAttributeMetadata decommissionedAttribute : decommissionedAttributes) {
            if (name.equals(decommissionedAttribute.name())) {
                return decommissionedAttribute;
            }
        }
        return null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DecommissionedAttributesMetadata that = (DecommissionedAttributesMetadata) o;

        return decommissionedAttributes.equals(that.decommissionedAttributes);
    }

    /**
     * Checks if this instance and the given instance share the same decommissioned attributes by checking that this instances' attributes and the
     * attributes in {@code other} are equal or only differ in their attribute values of {@link DecommissionedAttributeMetadata#decommissionedAttribute()}.
     *
     * @param other other decommissioned attributes metadata
     * @return {@code true} iff both instances contain the same decommissioned attributes apart from differences in attribute values
     */
    public boolean equalsIgnoreValues(@Nullable DecommissionedAttributesMetadata other) {
        if (other == null) {
            return false;
        }
        if (other.decommissionedAttributes.size() != decommissionedAttributes.size()) {
            return false;
        }
        for (int i = 0; i < decommissionedAttributes.size(); i++) {
            if (decommissionedAttributes.get(i).equalsIgnoreValues(other.decommissionedAttributes.get(i)) == false) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        return decommissionedAttributes.hashCode();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.CURRENT.minimumCompatibilityVersion();
    }

    public DecommissionedAttributesMetadata(StreamInput in) throws IOException {
        this.decommissionedAttributes = in.readList(DecommissionedAttributeMetadata::new);
    }

    public static NamedDiff<Custom> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(Custom.class, TYPE, in);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(decommissionedAttributes);
    }

    public static DecommissionedAttributesMetadata fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token;
        List<DecommissionedAttributeMetadata> decommissionedAttributes = new ArrayList<>();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                String name = parser.currentName();
                if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                    throw new OpenSearchParseException("failed to parse decommission attribute [{}], expected object", name);
                }
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        String currentFieldName = parser.currentName();
                        List<String> values = new ArrayList<>();
                        token = parser.nextToken();
                        if (token == XContentParser.Token.VALUE_STRING) {
                            values.add(parser.text());
                        } else if (token == XContentParser.Token.START_ARRAY) {
                            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                                if (token == XContentParser.Token.VALUE_STRING) {
                                    values.add(parser.text());
                                } else {
                                    parser.skipChildren();
                                }
                            }
                        } else throw new OpenSearchParseException("failed to parse attribute [{}], unknown type", name);
                        decommissionedAttributes.add(new DecommissionedAttributeMetadata(name, currentFieldName, values));
                    } else {
                        throw new OpenSearchParseException("failed to parse attribute [{}]", name);
                    }
                }
            }
        }
        return new DecommissionedAttributesMetadata(decommissionedAttributes);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        for (DecommissionedAttributeMetadata decommissionedAttribute : decommissionedAttributes) {
            toXContent(decommissionedAttribute, builder, params);
        }
        return builder;
    }

    @Override
    public EnumSet<Metadata.XContentContext> context() {
        return Metadata.API_AND_GATEWAY;
    }

    /**
     * Serializes information about a single decommissioned attribute
     *
     * @param decommissionedAttribute decommissioned attribute metadata
     * @param builder                 XContent builder
     * @param params                  serialization parameters
     */
    public static void toXContent(DecommissionedAttributeMetadata decommissionedAttribute, XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject(decommissionedAttribute.name());
        builder.startArray(decommissionedAttribute.decommissionedAttribute().key());
        for (String value : decommissionedAttribute.decommissionedAttribute().values()) {
            builder.value(value);
        }
        builder.endArray();
        builder.endObject();
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

}
