/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.plugin.transport.grpc.proto.response.document.bulk;

import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.get.GetResult;
import org.opensearch.plugin.transport.grpc.proto.response.common.OpenSearchExceptionProtoUtils;
import org.opensearch.plugin.transport.grpc.proto.response.document.common.DocWriteResponseProtoUtils;
import org.opensearch.plugin.transport.grpc.proto.response.document.get.GetResultProtoUtils;
import org.opensearch.protobufs.ErrorCause;
import org.opensearch.protobufs.Item;
import org.opensearch.protobufs.NullValue;
import org.opensearch.protobufs.ResponseItem;

import java.io.IOException;

/**
 * Utility class for converting BulkItemResponse objects to Protocol Buffers.
 * This class handles the conversion of individual bulk operation responses to their
 * Protocol Buffer representation.
 */
public class BulkItemResponseProtoUtils {

    private BulkItemResponseProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a BulkItemResponse to its Protocol Buffer representation.
     * This method is equivalent to the {@link BulkItemResponse#toXContent(XContentBuilder, ToXContent.Params)}
     *
     *
     * @param response The BulkItemResponse to convert
     * @return A Protocol Buffer Item representation
     * @throws IOException if there's an error during conversion
     *
     */
    public static Item toProto(BulkItemResponse response) throws IOException {
        Item.Builder itemBuilder = Item.newBuilder();

        ResponseItem.Builder responseItemBuilder;
        if (response.isFailed() == false) {
            DocWriteResponse docResponse = response.getResponse();
            responseItemBuilder = DocWriteResponseProtoUtils.toProto(docResponse);

            // TODO set the GRPC status instead of HTTP Status
            responseItemBuilder.setStatus(docResponse.status().getStatus());
        } else {
            BulkItemResponse.Failure failure = response.getFailure();
            responseItemBuilder = ResponseItem.newBuilder();

            responseItemBuilder.setIndex(failure.getIndex());
            if (response.getId().isEmpty()) {
                responseItemBuilder.setId(ResponseItem.Id.newBuilder().setNullValue(NullValue.NULL_VALUE_NULL).build());
            } else {
                responseItemBuilder.setId(ResponseItem.Id.newBuilder().setString(response.getId()).build());
            }
            // TODO set the GRPC status instead of HTTP Status
            responseItemBuilder.setStatus(failure.getStatus().getStatus());

            ErrorCause errorCause = OpenSearchExceptionProtoUtils.generateThrowableProto(failure.getCause());

            responseItemBuilder.setError(errorCause);
        }

        ResponseItem responseItem;
        switch (response.getOpType()) {
            case CREATE:
                responseItem = responseItemBuilder.build();
                itemBuilder.setCreate(responseItem);
                break;
            case INDEX:
                responseItem = responseItemBuilder.build();
                itemBuilder.setIndex(responseItem);
                break;
            case UPDATE:
                UpdateResponse updateResponse = response.getResponse();
                if (updateResponse != null) {
                    GetResult getResult = updateResponse.getGetResult();
                    if (getResult != null) {
                        responseItemBuilder = GetResultProtoUtils.toProto(getResult, responseItemBuilder);
                    }
                }
                responseItem = responseItemBuilder.build();
                itemBuilder.setUpdate(responseItem);
                break;
            case DELETE:
                responseItem = responseItemBuilder.build();
                itemBuilder.setDelete(responseItem);
                break;
            default:
                throw new UnsupportedOperationException("Invalid op type: " + response.getOpType());
        }

        return itemBuilder.build();
    }
}
