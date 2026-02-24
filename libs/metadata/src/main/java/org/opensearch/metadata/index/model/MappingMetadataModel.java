/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.metadata.index.model;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.metadata.common.XContentContext;
import org.opensearch.metadata.compress.CompressedData;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * Pure data holder class for mapping metadata without dependencies on OpenSearch server packages.
 * This class follows the composition pattern and contains only data fields with getters.
 * <p>
 * MappingMetadataModel stores all essential properties of a mapping.
 */
@ExperimentalApi
public final class MappingMetadataModel implements Writeable, ToXContentFragment {

    private static final String ROUTING_FIELD = "_routing";
    private static final String REQUIRED_FIELD = "required";

    private final String type;
    private final CompressedData source;
    private final boolean routingRequired;

    /**
     * Creates a new MappingMetadataModel.
     *
     * @param type the mapping type name (must not be null)
     * @param source the compressed mapping source (must not be null)
     * @param routingRequired whether routing is required
     * @throws NullPointerException if type or source is null
     */
    public MappingMetadataModel(String type, CompressedData source, boolean routingRequired) {
        this.type = Objects.requireNonNull(type, "type must not be null");
        this.source = Objects.requireNonNull(source, "source must not be null");
        this.routingRequired = routingRequired;
    }

    /**
     * Deserialization constructor.
     *
     * @param in the stream input
     * @throws IOException if deserialization fails
     */
    public MappingMetadataModel(StreamInput in) throws IOException {
        this.type = in.readString();
        this.source = new CompressedData(in);
        this.routingRequired = in.readBoolean();
    }

    /**
     * Returns the mapping type name.
     *
     * @return the type name
     */
    public String type() {
        return type;
    }

    /**
     * Returns the mapping source.
     *
     * @return the source
     */
    public CompressedData source() {
        return source;
    }

    /**
     * Returns whether routing is required.
     *
     * @return true if routing is required
     */
    public boolean routingRequired() {
        return routingRequired;
    }

    /**
     * Writes this MappingMetadataModel to a stream.
     * The serialization format is compatible with MappingMetadata.
     *
     * @param out the stream to write to
     * @throws IOException if an I/O error occurs during serialization
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(type);
        source.writeTo(out);
        out.writeBoolean(routingRequired);
    }

    /**
     * Compares this MappingMetadataModel with another object for equality.
     * Two models are equal if all their fields are equal.
     *
     * @param o the object to compare with
     * @return true if the objects are equal, false otherwise
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MappingMetadataModel that = (MappingMetadataModel) o;

        return routingRequired == that.routingRequired && Objects.equals(type, that.type) && Objects.equals(source, that.source);
    }

    /**
     * Returns the hash code for this MappingMetadataModel.
     * The hash code is computed from all fields.
     *
     * @return the hash code
     */
    @Override
    public int hashCode() {
        return Objects.hash(type, source, routingRequired);
    }

    /**
     * Parses a MappingMetadataModel from XContent.
     * Expects the parser to be positioned at the mapping type field name.
     * The mapping type is taken from parser.currentName().
     * <p>
     * Expected format: { "type_name": { ... mapping content ... } }
     *
     * @param parser the XContent parser
     * @return the parsed MappingMetadataModel
     * @throws IOException if parsing fails
     */
    public static MappingMetadataModel fromXContent(XContentParser parser) throws IOException {
        String mappingType = parser.currentName();

        // Move to START_OBJECT of the mapping content
        XContentParser.Token token = parser.nextToken();
        if (token != XContentParser.Token.START_OBJECT) {
            throw new IllegalArgumentException("Expected START_OBJECT but got " + token);
        }

        // Parse the mapping content as a map
        Map<String, Object> mappingContent = parser.mapOrdered();

        // Determine if routing is required
        boolean routingRequired = isRoutingRequired(mappingContent);

        // Wrap the mapping content with the type name for storage
        // The source format is: { "type_name": { ... content ... } }
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        builder.field(mappingType);
        builder.map(mappingContent);
        builder.endObject();

        byte[] bytes = BytesReference.toBytes(BytesReference.bytes(builder));
        CompressedData source = new CompressedData(bytes);

        return new MappingMetadataModel(mappingType, source, routingRequired);
    }

    /**
     * Determines if routing is required from the mapping content.
     */
    @SuppressWarnings("unchecked")
    private static boolean isRoutingRequired(Map<String, Object> mappingContent) {
        if (mappingContent.containsKey(ROUTING_FIELD)) {
            Object routingNode = mappingContent.get(ROUTING_FIELD);
            if (routingNode instanceof Map) {
                Map<String, Object> routingMap = (Map<String, Object>) routingNode;
                Object requiredValue = routingMap.get(REQUIRED_FIELD);
                if (requiredValue instanceof Boolean) {
                    return (Boolean) requiredValue;
                } else if (requiredValue instanceof String) {
                    return Boolean.parseBoolean((String) requiredValue);
                }
            }
        }
        return false;
    }

    /**
     * Writes this MappingMetadataModel to XContent.
     * Outputs the mapping type as a field name with the mapping content as its value.
     * <p>
     * Output format: "type_name": { ... mapping content ... }
     *
     * @param builder the XContent builder
     * @param params the ToXContent params
     * @return the XContent builder
     * @throws IOException if writing fails
     */
    @Override
    @SuppressWarnings("unchecked")
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        boolean binary = params.paramAsBoolean("binary", false);
        XContentContext xContentContext = XContentContext.valueOf(params.param(XContentContext.PARAM_KEY, XContentContext.API.name()));

        if (xContentContext != XContentContext.API) {
            if (binary) {
                builder.value(source.compressedBytes());
            } else {
                byte[] uncompressed = source.uncompressed();
                try (XContentParser sourceParser = JsonXContent.jsonXContent.createParser(null, null, uncompressed)) {
                    sourceParser.nextToken();
                    builder.map(sourceParser.mapOrdered());
                }
            }
        } else {
            byte[] uncompressed = source.uncompressed();
            try (XContentParser sourceParser = JsonXContent.jsonXContent.createParser(null, null, uncompressed)) {
                sourceParser.nextToken();
                Map<String, Object> sourceMap = sourceParser.mapOrdered();
                Map<String, Object> mappingContent;
                if (sourceMap.size() == 1 && sourceMap.containsKey(type)) {
                    mappingContent = (Map<String, Object>) sourceMap.get(type);
                } else {
                    mappingContent = sourceMap;
                }
                builder.field(type);
                builder.map(mappingContent);
            }
        }
        return builder;
    }
}
