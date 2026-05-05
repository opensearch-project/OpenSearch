/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.response.document.get;

import com.google.protobuf.ByteString;
import org.opensearch.common.document.DocumentField;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.get.GetResult;
import org.opensearch.index.mapper.IgnoredFieldMapper;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.protobufs.InlineGetDictUserDefined;
import org.opensearch.protobufs.ObjectMap;
import org.opensearch.protobufs.ResponseItem;
import org.opensearch.transport.grpc.proto.response.document.common.DocumentFieldProtoUtils;

/**
 * Utility class for converting GetResult objects to Protocol Buffers.
 * This class handles the conversion of document get operation results to their
 * Protocol Buffer representation.
 */
public class GetResultProtoUtils {

    private GetResultProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a GetResult to its Protocol Buffer representation.
     * This method is equivalent to the  {@link GetResult#toXContent(XContentBuilder, ToXContent.Params)}
     *
     * @param getResult           The GetResult to convert
     * @param responseItemBuilder The builder to populate with GetResult data
     * @return The populated builder
     */
    public static ResponseItem.Builder toProto(GetResult getResult, ResponseItem.Builder responseItemBuilder) {
        // Reuse the builder passed in by reference
        responseItemBuilder.setXIndex(getResult.getIndex());

        // Set document ID
        responseItemBuilder.setXId(getResult.getId());

        // Create the inline get dict builder only once
        InlineGetDictUserDefined.Builder inlineGetDictUserDefinedBuilder = InlineGetDictUserDefined.newBuilder();

        if (getResult.isExists()) {
            // Set document version if available
            if (getResult.getVersion() != -1) {
                responseItemBuilder.setXVersion(getResult.getVersion());
            }
            toProtoEmbedded(getResult, inlineGetDictUserDefinedBuilder);
        } else {
            inlineGetDictUserDefinedBuilder.setFound(false);
        }

        responseItemBuilder.setGet(inlineGetDictUserDefinedBuilder.build());
        return responseItemBuilder;
    }

    /**
     * Converts a GetResult to its Protocol Buffer representation for embedding in another message.
     * This method is equivalent to the {@link GetResult#toXContentEmbedded(XContentBuilder, ToXContent.Params)}
     *
     * @param getResult The GetResult to convert
     * @param builder The builder to add the GetResult data to
     */
    public static void toProtoEmbedded(GetResult getResult, InlineGetDictUserDefined.Builder builder) {
        // Set sequence number and primary term if available
        if (getResult.getSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO) {
            builder.setXSeqNo(getResult.getSeqNo());
            builder.setXPrimaryTerm(getResult.getPrimaryTerm());
        }

        // Set existence status
        builder.setFound(getResult.isExists());

        // Set source if available - avoid unnecessary copying if possible
        if (getResult.source() != null) {
            builder.setXSource(ByteString.copyFrom(getResult.source()));
        }

        // Process metadata fields
        if (!getResult.getMetadataFields().isEmpty()) {
            ObjectMap.Builder metadataFieldsBuilder = ObjectMap.newBuilder();
            for (DocumentField field : getResult.getMetadataFields().values()) {
                if (field.getName().equals(IgnoredFieldMapper.NAME)) {
                    metadataFieldsBuilder.putFields(field.getName(), DocumentFieldProtoUtils.toProto(field.getValues()));
                } else {
                    metadataFieldsBuilder.putFields(field.getName(), DocumentFieldProtoUtils.toProto(field.<Object>getValue()));
                }
            }
            builder.setMetadataFields(metadataFieldsBuilder.build());
        }

        // Process document fields - only create builder if needed
        if (!getResult.getDocumentFields().isEmpty()) {
            ObjectMap.Builder documentFieldsBuilder = ObjectMap.newBuilder();
            for (DocumentField field : getResult.getDocumentFields().values()) {
                documentFieldsBuilder.putFields(field.getName(), DocumentFieldProtoUtils.toProto(field.getValues()));
            }
            builder.setFields(documentFieldsBuilder.build());
        }
    }
}
