/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.response.document.bulk;

import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Utility class for converting BulkResponse objects to Protocol Buffers.
 * This class handles the conversion of bulk operation responses to their
 * Protocol Buffer representation.
 */
public class BulkResponseProtoUtils {

    private BulkResponseProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a BulkResponse to its Protocol Buffer representation.
     * This method is equivalent to {@link BulkResponse#toXContent(XContentBuilder, ToXContent.Params)}
     *
     * @param response The BulkResponse to convert
     * @return A Protocol Buffer BulkResponse representation
     * @throws IOException if there's an error during conversion
     */
    public static org.opensearch.protobufs.BulkResponse toProto(BulkResponse response) throws IOException {
        // System.out.println("=== grpc bulk response=" + response.toString());

        org.opensearch.protobufs.BulkResponse.Builder bulkResponse = org.opensearch.protobufs.BulkResponse.newBuilder();

        // Set the time taken for the bulk operation (excluding ingest preprocessing)
        bulkResponse.setTook(response.getTook().getMillis());

        // Set ingest preprocessing time if available
        if (response.getIngestTookInMillis() != BulkResponse.NO_INGEST_TOOK) {
            bulkResponse.setIngestTook(response.getIngestTookInMillis());
        }

        // Set whether any operations failed
        bulkResponse.setErrors(response.hasFailures());

        // Add individual item responses for each operation in the bulk request
        for (BulkItemResponse bulkItemResponse : response.getItems()) {
            org.opensearch.protobufs.ResponseItem responseItem = BulkItemResponseProtoUtils.toProto(bulkItemResponse);
            org.opensearch.protobufs.Item.Builder itemBuilder = org.opensearch.protobufs.Item.newBuilder();

            // Wrap ResponseItem in Item based on operation type
            switch (bulkItemResponse.getOpType()) {
                case CREATE:
                    itemBuilder.setCreate(responseItem);
                    break;
                case DELETE:
                    itemBuilder.setDelete(responseItem);
                    break;
                case INDEX:
                    itemBuilder.setIndex(responseItem);
                    break;
                case UPDATE:
                    itemBuilder.setUpdate(responseItem);
                    break;
            }

            bulkResponse.addItems(itemBuilder.build());
        }

        return bulkResponse.build();
    }
}
