/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search;

import org.opensearch.index.mapper.DerivedField;
import org.opensearch.transport.grpc.proto.request.common.ObjectMapProtoUtils;
import org.opensearch.transport.grpc.proto.request.common.ScriptProtoUtils;

/**
 * Utility class for converting DerivedField Protocol Buffers to OpenSearch DerivedField objects.
 * This class handles the conversion of Protocol Buffer representations to their
 * corresponding OpenSearch derived field objects, matching the REST side pattern.
 *
 * @see org.opensearch.index.mapper.DerivedFieldMapper.Builder#build(org.opensearch.index.mapper.Mapper.BuilderContext)
 * @see org.opensearch.index.mapper.DefaultDerivedFieldResolver#initDerivedFieldTypes(java.util.Map, java.util.List)
 */
public class DerivedFieldProtoUtils {

    private DerivedFieldProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a Protocol Buffer DerivedField to its corresponding OpenSearch DerivedField.
     * Uses the same pattern as REST side: always use simple constructor, then conditionally
     * set optional fields based on presence (matches DerivedFieldMapper.Builder.build() pattern).
     *
     *
     * <p>This matches the REST side's {@code Parameter.isConfigured()} pattern in
     * {@link org.opensearch.index.mapper.DerivedFieldMapper.Builder#build}, where optional
     * fields are only set if they were present in the request.</p>
     *
     * @param name The name of the derived field
     * @param derivedFieldProto The Protocol Buffer DerivedField to convert
     * @return The corresponding OpenSearch DerivedField with optional fields set
     * @see org.opensearch.index.mapper.DerivedFieldMapper.Builder#build(org.opensearch.index.mapper.Mapper.BuilderContext)
     * @see org.opensearch.index.mapper.ParametrizedFieldMapper.Parameter#isConfigured()
     */
    public static DerivedField fromProto(String name, org.opensearch.protobufs.DerivedField derivedFieldProto) {
        // Always use simple constructor first (matches REST side pattern in DerivedFieldMapper.Builder.build())
        DerivedField derivedField = new DerivedField(
            name,
            derivedFieldProto.getType(),
            ScriptProtoUtils.parseFromProtoRequest(derivedFieldProto.getScript())
        );

        // Conditionally set optional fields based on presence (matches REST side's isConfigured() pattern)
        if (derivedFieldProto.hasProperties()) {
            derivedField.setProperties(ObjectMapProtoUtils.fromProto(derivedFieldProto.getProperties()));
        }
        if (derivedFieldProto.hasPrefilterField()) {
            derivedField.setPrefilterField(derivedFieldProto.getPrefilterField());
        }
        if (derivedFieldProto.hasFormat()) {
            derivedField.setFormat(derivedFieldProto.getFormat());
        }
        if (derivedFieldProto.hasIgnoreMalformed()) {
            derivedField.setIgnoreMalformed(derivedFieldProto.getIgnoreMalformed());
        }

        return derivedField;
    }
}
