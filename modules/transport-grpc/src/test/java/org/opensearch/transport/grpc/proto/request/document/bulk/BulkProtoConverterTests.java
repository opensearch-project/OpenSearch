/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.document.bulk;

import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.action.support.WriteRequest.RefreshPolicy;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.VersionType;
import org.opensearch.protobufs.BulkRequestBody;
import org.opensearch.protobufs.IndexOperation;
import org.opensearch.protobufs.OpType;
import org.opensearch.protobufs.OperationContainer;
import org.opensearch.protobufs.Refresh;
import org.opensearch.protobufs.WaitForActiveShardOptions;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;

public class BulkProtoConverterTests extends OpenSearchTestCase {

    public void testIndexOp() {
        BulkRequest bulk = new BulkRequest();
        bulk.add(new IndexRequest("orders").id("1").source(Map.of("k", 1), XContentType.JSON));
        var proto = BulkProtoConverter.toProto(bulk);

        assertEquals(1, proto.getBulkRequestBodyCount());
        BulkRequestBody body = proto.getBulkRequestBody(0);
        OperationContainer container = body.getOperationContainer();
        assertTrue(container.hasIndex());
        IndexOperation idx = container.getIndex();
        assertEquals("orders", idx.getXIndex());
        assertEquals("1", idx.getXId());
        assertEquals(OpType.OP_TYPE_UNSPECIFIED, idx.getOpType()); // op_type unset for plain INDEX
        assertTrue(body.getObject().size() > 0); // doc bytes attached
    }

    public void testCreateOpWithoutVersionControl_emitsWriteOperation() {
        BulkRequest bulk = new BulkRequest();
        bulk.add(new IndexRequest("orders").id("1").opType(IndexRequest.OpType.CREATE).source(Map.of("k", 1), XContentType.JSON));
        var proto = BulkProtoConverter.toProto(bulk);

        OperationContainer container = proto.getBulkRequestBody(0).getOperationContainer();
        assertTrue("CREATE without version control uses WriteOperation", container.hasCreate());
        assertFalse(container.hasIndex());
        assertEquals("orders", container.getCreate().getXIndex());
    }

    public void testCreateOpWithVersionControl_fallsBackToIndexOpWithCreateType() {
        // WriteOperation has no version fields — fall back to IndexOperation{op_type=CREATE}.
        BulkRequest bulk = new BulkRequest();
        bulk.add(
            new IndexRequest("orders").id("1")
                .opType(IndexRequest.OpType.CREATE)
                .versionType(VersionType.EXTERNAL)
                .version(7L)
                .source(Map.of("k", 1), XContentType.JSON)
        );
        var proto = BulkProtoConverter.toProto(bulk);

        OperationContainer container = proto.getBulkRequestBody(0).getOperationContainer();
        assertTrue("CREATE with version control falls back to IndexOperation", container.hasIndex());
        assertFalse(container.hasCreate());
        IndexOperation idx = container.getIndex();
        assertEquals(OpType.OP_TYPE_CREATE, idx.getOpType());
        assertEquals(7L, idx.getVersion());
    }

    public void testUpdateOp_docBytesGoToBodyObject_notUpdateActionDoc() {
        BulkRequest bulk = new BulkRequest();
        bulk.add(new UpdateRequest("orders", "1").doc(Map.of("k", 2), XContentType.JSON));
        var proto = BulkProtoConverter.toProto(bulk);

        BulkRequestBody body = proto.getBulkRequestBody(0);
        assertTrue(body.getOperationContainer().hasUpdate());
        // Forward parses update doc bytes from body.object, ignoring update_action.doc.
        assertTrue("update doc must be on body.object", body.getObject().size() > 0);
        assertTrue("UpdateAction is still present for upsert/script flags", body.hasUpdateAction());
    }

    public void testDeleteOp() {
        BulkRequest bulk = new BulkRequest();
        bulk.add(new DeleteRequest("orders", "1").setIfSeqNo(100L).setIfPrimaryTerm(2L));
        var proto = BulkProtoConverter.toProto(bulk);

        var del = proto.getBulkRequestBody(0).getOperationContainer().getDelete();
        assertEquals("1", del.getXId());
        assertEquals(100L, del.getIfSeqNo());
        assertEquals(2L, del.getIfPrimaryTerm());
    }

    public void testRefreshPolicy_NONE_isNotEmitted() {
        BulkRequest bulk = newBulkWithOneIndex();
        bulk.setRefreshPolicy(RefreshPolicy.NONE);
        assertEquals("RefreshPolicy.NONE → proto unset", Refresh.REFRESH_UNSPECIFIED, BulkProtoConverter.toProto(bulk).getRefresh());
    }

    public void testRefreshPolicy_IMMEDIATE_mapsToTrue() {
        BulkRequest bulk = newBulkWithOneIndex();
        bulk.setRefreshPolicy(RefreshPolicy.IMMEDIATE);
        assertEquals(Refresh.REFRESH_TRUE, BulkProtoConverter.toProto(bulk).getRefresh());
    }

    public void testRefreshPolicy_WAIT_UNTIL_mapsToWaitFor() {
        BulkRequest bulk = newBulkWithOneIndex();
        bulk.setRefreshPolicy(RefreshPolicy.WAIT_UNTIL);
        assertEquals(Refresh.REFRESH_WAIT_FOR, BulkProtoConverter.toProto(bulk).getRefresh());
    }

    public void testVersionType_INTERNAL_isNotEmitted() {
        BulkRequest bulk = new BulkRequest();
        bulk.add(new IndexRequest("orders").id("1").versionType(VersionType.INTERNAL).source(Map.of(), XContentType.JSON));
        var idx = BulkProtoConverter.toProto(bulk).getBulkRequestBody(0).getOperationContainer().getIndex();
        assertFalse("VersionType.INTERNAL should not emit version_type on proto", idx.hasVersionType());
    }

    public void testVersionType_EXTERNAL_emitsExternal() {
        BulkRequest bulk = new BulkRequest();
        bulk.add(new IndexRequest("orders").id("1").versionType(VersionType.EXTERNAL).version(5L).source(Map.of(), XContentType.JSON));
        var idx = BulkProtoConverter.toProto(bulk).getBulkRequestBody(0).getOperationContainer().getIndex();
        assertEquals(org.opensearch.protobufs.VersionType.VERSION_TYPE_EXTERNAL, idx.getVersionType());
        assertEquals(5L, idx.getVersion());
    }

    public void testActiveShardCount_DEFAULT_isNotEmitted() {
        BulkRequest bulk = newBulkWithOneIndex();
        bulk.waitForActiveShards(ActiveShardCount.DEFAULT);
        assertFalse(BulkProtoConverter.toProto(bulk).hasWaitForActiveShards());
    }

    public void testActiveShardCount_ALL_emitsOptionEnum() {
        BulkRequest bulk = newBulkWithOneIndex();
        bulk.waitForActiveShards(ActiveShardCount.ALL);
        var was = BulkProtoConverter.toProto(bulk).getWaitForActiveShards();
        assertTrue(was.hasOption());
        assertEquals(WaitForActiveShardOptions.WAIT_FOR_ACTIVE_SHARD_OPTIONS_ALL, was.getOption());
    }

    public void testActiveShardCount_NONE_emitsCountZero() {
        BulkRequest bulk = newBulkWithOneIndex();
        bulk.waitForActiveShards(ActiveShardCount.NONE);
        var was = BulkProtoConverter.toProto(bulk).getWaitForActiveShards();
        assertTrue(was.hasCount());
        assertEquals(0, was.getCount());
    }

    public void testUnassignedSeqNoNotEmitted() {
        BulkRequest bulk = new BulkRequest();
        bulk.add(new IndexRequest("orders").id("1").source(Map.of(), XContentType.JSON));
        var idx = BulkProtoConverter.toProto(bulk).getBulkRequestBody(0).getOperationContainer().getIndex();
        assertFalse(idx.hasIfSeqNo());
        assertFalse(idx.hasIfPrimaryTerm());
    }

    public void testRequireAliasFalse_isNotEmitted() {
        BulkRequest bulk = new BulkRequest();
        bulk.add(new IndexRequest("orders").id("1").setRequireAlias(false).source(Map.of(), XContentType.JSON));
        var idx = BulkProtoConverter.toProto(bulk).getBulkRequestBody(0).getOperationContainer().getIndex();
        assertFalse(idx.getRequireAlias());
    }

    public void testEmptyBulk_producesEmptyBodyList() {
        var proto = BulkProtoConverter.toProto(new BulkRequest());
        assertEquals(0, proto.getBulkRequestBodyCount());
    }

    public void testTimeoutEmittedAsString() {
        BulkRequest bulk = newBulkWithOneIndex();
        bulk.timeout(TimeValue.timeValueSeconds(30));
        assertEquals("30s", BulkProtoConverter.toProto(bulk).getTimeout());
    }

    public void testFourOpMixedBulk_preservesOrder() {
        BulkRequest bulk = new BulkRequest();
        bulk.add(new IndexRequest("o").id("a").source(Map.of("k", 1), XContentType.JSON));
        bulk.add(new IndexRequest("o").id("b").source(Map.of("k", 2), XContentType.JSON).opType(IndexRequest.OpType.CREATE));
        bulk.add(new UpdateRequest("o", "c").doc(Map.of("k", 3), XContentType.JSON));
        bulk.add(new DeleteRequest("o", "d"));
        var proto = BulkProtoConverter.toProto(bulk);

        assertEquals(4, proto.getBulkRequestBodyCount());
        assertTrue(proto.getBulkRequestBody(0).getOperationContainer().hasIndex());
        assertTrue(proto.getBulkRequestBody(1).getOperationContainer().hasCreate());
        assertTrue(proto.getBulkRequestBody(2).getOperationContainer().hasUpdate());
        assertTrue(proto.getBulkRequestBody(3).getOperationContainer().hasDelete());
    }

    private static BulkRequest newBulkWithOneIndex() {
        BulkRequest bulk = new BulkRequest();
        bulk.add(new IndexRequest("orders").id("1").source(Map.of("k", 1), XContentType.JSON));
        return bulk;
    }
}
