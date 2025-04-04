/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.plugin.transport.grpc.proto.response.document.get;

import com.google.protobuf.ByteString;
import com.google.protobuf.Struct;
import org.opensearch.common.document.DocumentField;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.get.GetResult;
import org.opensearch.index.mapper.IgnoredFieldMapper;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.plugin.transport.grpc.proto.response.document.common.DocumentFieldProtoUtils;
import org.opensearch.protobufs.InlineGetDictUserDefined;
import org.opensearch.protobufs.ResponseItem;

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
     * @param responseItemBuilder
     * @return A Protocol Buffer InlineGetDictUserDefined representation
     */
    public static ResponseItem.Builder toProto(GetResult getResult, ResponseItem.Builder responseItemBuilder) {
        InlineGetDictUserDefined.Builder inlineGetDictUserDefinedBuilder = InlineGetDictUserDefined.newBuilder();

        responseItemBuilder.setIndex(getResult.getIndex());
        responseItemBuilder.setId(ResponseItem.Id.newBuilder().setString(getResult.getId()).build());

        if (getResult.isExists()) {
            // Set document version if available
            if (getResult.getVersion() != -1) {
                responseItemBuilder.setVersion(getResult.getVersion());
            }
            inlineGetDictUserDefinedBuilder = toProtoEmbedded(getResult, inlineGetDictUserDefinedBuilder);
        } else {
            inlineGetDictUserDefinedBuilder.setFound(false);
        }

        responseItemBuilder.setGet(inlineGetDictUserDefinedBuilder);
        return responseItemBuilder;
    }

    /**
     * Converts a GetResult to its Protocol Buffer representation for embedding in another message.
     * This method is equivalent to the {@link GetResult#toXContentEmbedded(XContentBuilder, ToXContent.Params)}
     *
     * @param getResult The GetResult to convert
     * @param builder The builder to add the GetResult data to
     * @return The updated builder with the GetResult data
     */
    public static InlineGetDictUserDefined.Builder toProtoEmbedded(GetResult getResult, InlineGetDictUserDefined.Builder builder) {
        // Set sequence number and primary term if available
        if (getResult.getSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO) {
            builder.setSeqNo(getResult.getSeqNo());
            builder.setPrimaryTerm(getResult.getPrimaryTerm());
        }

        // TODO test output once GetDocument GRPC endpoint is implemented
        Struct.Builder metadataFieldsBuilder = Struct.newBuilder();
        for (DocumentField field : getResult.getMetadataFields().values()) {
            if (field.getName().equals(IgnoredFieldMapper.NAME)) {
                metadataFieldsBuilder.putFields(field.getName(), DocumentFieldProtoUtils.toProto(field.getValues()));
            } else {
                metadataFieldsBuilder.putFields(field.getName(), DocumentFieldProtoUtils.toProto(field.<Object>getValue()));
            }
        }
        builder.setMetadataFields(metadataFieldsBuilder.build());

        // Set existence status
        builder.setFound(getResult.isExists());

        // Set source if available
        if (getResult.source() != null) {
            builder.setSource(ByteString.copyFrom(getResult.source()));
        }

        // TODO test output once GetDocument GRPC endpoint is implemented
        Struct.Builder documentFieldsBuilder = Struct.newBuilder();
        if (!getResult.getDocumentFields().isEmpty()) {
            for (DocumentField field : getResult.getDocumentFields().values()) {
                documentFieldsBuilder.putFields(field.getName(), DocumentFieldProtoUtils.toProto(field.getValues()));
            }
        }
        builder.setFields(documentFieldsBuilder.build());

        return builder;
    }
}
