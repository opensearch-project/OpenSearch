/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.response.document.bulk;

import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.protobufs.ErrorCause;
import org.opensearch.protobufs.Item;
import org.opensearch.protobufs.ResponseItem;
import org.opensearch.protobufs.ShardInfo;
import org.opensearch.test.OpenSearchTestCase;

public class BulkResponseProtoConverterTests extends OpenSearchTestCase {

    public void testTookAndIngestTookRoundTrip() {
        org.opensearch.protobufs.BulkResponse proto = org.opensearch.protobufs.BulkResponse.newBuilder()
            .setTook(42L)
            .setIngestTook(7L)
            .build();
        BulkResponse server = BulkResponseProtoConverter.fromProto(proto);
        assertEquals(42L, server.getTook().millis());
        assertEquals(7L, server.getIngestTookInMillis());
    }

    public void testIngestTookOptional_absent() {
        org.opensearch.protobufs.BulkResponse proto = org.opensearch.protobufs.BulkResponse.newBuilder().setTook(10L).build();
        BulkResponse server = BulkResponseProtoConverter.fromProto(proto);
        assertEquals(-1L, server.getIngestTookInMillis()); // -1 = not set
    }

    public void testIndexItem_mapsToIndexResponse() {
        org.opensearch.protobufs.BulkResponse proto = withItem(
            Item.newBuilder()
                .setIndex(
                    ResponseItem.newBuilder()
                        .setXIndex("orders")
                        .setXId("1")
                        .setXVersion(1L)
                        .setXSeqNo(0L)
                        .setXPrimaryTerm(1L)
                        .setResult("created")
                        .build()
                )
                .build()
        );
        BulkResponse server = BulkResponseProtoConverter.fromProto(proto);
        assertEquals(1, server.getItems().length);

        BulkItemResponse item = server.getItems()[0];
        assertEquals(DocWriteRequest.OpType.INDEX, item.getOpType());
        assertTrue(item.getResponse() instanceof IndexResponse);
        IndexResponse resp = item.getResponse();
        assertEquals("orders", resp.getIndex());
        assertEquals("1", resp.getId());
        assertEquals(DocWriteResponse.Result.CREATED, resp.getResult());
        assertEquals(1L, resp.getVersion());
    }

    public void testCreateItem_mapsToIndexResponseWithCreatedFlag() {
        org.opensearch.protobufs.BulkResponse proto = withItem(
            Item.newBuilder().setCreate(ResponseItem.newBuilder().setXIndex("o").setXId("1").setResult("created").build()).build()
        );
        BulkResponse server = BulkResponseProtoConverter.fromProto(proto);
        BulkItemResponse item = server.getItems()[0];
        assertEquals(DocWriteRequest.OpType.CREATE, item.getOpType());
        assertTrue(item.getResponse() instanceof IndexResponse);
    }

    public void testUpdateItem_mapsToUpdateResponse() {
        org.opensearch.protobufs.BulkResponse proto = withItem(
            Item.newBuilder().setUpdate(ResponseItem.newBuilder().setXIndex("o").setXId("1").setResult("updated").build()).build()
        );
        BulkResponse server = BulkResponseProtoConverter.fromProto(proto);
        BulkItemResponse item = server.getItems()[0];
        assertEquals(DocWriteRequest.OpType.UPDATE, item.getOpType());
        assertTrue(item.getResponse() instanceof UpdateResponse);
        assertEquals(DocWriteResponse.Result.UPDATED, item.getResponse().getResult());
    }

    public void testDeleteItem_mapsToDeleteResponse_withFoundFlag() {
        org.opensearch.protobufs.BulkResponse proto = withItem(
            Item.newBuilder().setDelete(ResponseItem.newBuilder().setXIndex("o").setXId("1").setResult("deleted").build()).build()
        );
        BulkResponse server = BulkResponseProtoConverter.fromProto(proto);
        BulkItemResponse item = server.getItems()[0];
        assertEquals(DocWriteRequest.OpType.DELETE, item.getOpType());
        assertTrue(item.getResponse() instanceof DeleteResponse);
        assertEquals(DocWriteResponse.Result.DELETED, item.getResponse().getResult());
    }

    public void testDeleteItem_notFound_setsFoundFalse() {
        org.opensearch.protobufs.BulkResponse proto = withItem(
            Item.newBuilder().setDelete(ResponseItem.newBuilder().setXIndex("o").setXId("missing").setResult("not_found").build()).build()
        );
        BulkResponse server = BulkResponseProtoConverter.fromProto(proto);
        DeleteResponse resp = (DeleteResponse) server.getItems()[0].getResponse();
        assertEquals(DocWriteResponse.Result.NOT_FOUND, resp.getResult());
    }

    public void testFailureItem_producesFailureWithOpenSearchException() {
        org.opensearch.protobufs.BulkResponse proto = withItem(
            Item.newBuilder()
                .setIndex(
                    ResponseItem.newBuilder()
                        .setXIndex("o")
                        .setXId("1")
                        .setError(ErrorCause.newBuilder().setType("version_conflict").setReason("doc exists").build())
                        .build()
                )
                .build()
        );
        BulkResponse server = BulkResponseProtoConverter.fromProto(proto);
        assertTrue(server.hasFailures());
        BulkItemResponse item = server.getItems()[0];
        assertTrue(item.isFailed());
        assertNotNull(item.getFailure());
        assertEquals("o", item.getFailure().getIndex());
        assertEquals("1", item.getFailure().getId());
        assertNotNull(item.getFailure().getCause());
        assertTrue(item.getFailure().getMessage().contains("version_conflict"));
        assertTrue(item.getFailure().getMessage().contains("doc exists"));
    }

    public void testShardInfo_populatedFromProto() {
        ShardInfo shards = ShardInfo.newBuilder().setTotal(2).setSuccessful(1).setFailed(1).build();
        org.opensearch.protobufs.BulkResponse proto = withItem(
            Item.newBuilder()
                .setIndex(ResponseItem.newBuilder().setXIndex("o").setXId("1").setResult("created").setXShards(shards).build())
                .build()
        );
        BulkResponse server = BulkResponseProtoConverter.fromProto(proto);
        IndexResponse resp = (IndexResponse) server.getItems()[0].getResponse();
        assertEquals(2, resp.getShardInfo().getTotal());
        assertEquals(1, resp.getShardInfo().getSuccessful());
    }

    public void testForcedRefresh_propagated() {
        org.opensearch.protobufs.BulkResponse proto = withItem(
            Item.newBuilder()
                .setIndex(ResponseItem.newBuilder().setXIndex("o").setXId("1").setResult("created").setForcedRefresh(true).build())
                .build()
        );
        BulkResponse server = BulkResponseProtoConverter.fromProto(proto);
        IndexResponse resp = (IndexResponse) server.getItems()[0].getResponse();
        assertTrue(resp.forcedRefresh());
    }

    public void testUnknownResultString_throws() {
        org.opensearch.protobufs.BulkResponse proto = withItem(
            Item.newBuilder().setIndex(ResponseItem.newBuilder().setXIndex("o").setXId("1").setResult("frobnicated").build()).build()
        );
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> BulkResponseProtoConverter.fromProto(proto));
        assertTrue(e.getMessage().contains("unknown result"));
    }

    private static org.opensearch.protobufs.BulkResponse withItem(Item item) {
        return org.opensearch.protobufs.BulkResponse.newBuilder().setTook(1L).addItems(item).build();
    }
}
