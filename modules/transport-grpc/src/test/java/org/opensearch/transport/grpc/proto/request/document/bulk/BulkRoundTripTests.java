/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.document.bulk;

import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.action.support.WriteRequest.RefreshPolicy;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.VersionType;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;

/**
 * Round-trip parity tests for bulk requests:
 *   server BulkRequest s
 *     -> BulkProtoConverter.toProto(s)            (this module: server -> proto)
 *     -> BulkRequestProtoUtils.prepareRequest(p)  (this module: proto -> server)
 *   assert s ~= s'  (per-row + cluster-level fields)
 *
 * Co-locating both directions in this module is what makes this test possible without crossing
 * subproject boundaries. Failures here are early-warning signals that the two converters have
 * diverged as the proto schema evolves.
 *
 * Fixtures avoid known non-injective collapse zones so equality can hold (top-level
 * routing/index/pipeline are inlined per-row by the forward parser; we test per-row only).
 */
public class BulkRoundTripTests extends OpenSearchTestCase {

    public void testPlainIndex() {
        BulkRequest s = new BulkRequest();
        s.add(new IndexRequest("orders").id("o-1").source(Map.of("status", "new", "amount", 99), XContentType.JSON).routing("c-42"));
        assertRoundTrip(s);
    }

    public void testCreateWithExternalVersion() {
        BulkRequest s = new BulkRequest();
        s.add(
            new IndexRequest("orders").id("o-2")
                .source(Map.of("status", "new", "amount", 150), XContentType.JSON)
                .opType(IndexRequest.OpType.CREATE)
                .versionType(VersionType.EXTERNAL)
                .version(7L)
        );
        assertRoundTrip(s);
    }

    public void testUpdateWithDocAndUpsertAndRetry() {
        BulkRequest s = new BulkRequest();
        s.add(
            new UpdateRequest("orders", "o-1").doc(Map.of("status", "shipped"), XContentType.JSON)
                .upsert(Map.of("status", "shipped", "amount", 0), XContentType.JSON)
                .retryOnConflict(3)
        );
        assertRoundTrip(s);
    }

    public void testDeleteWithOptimisticConcurrency() {
        BulkRequest s = new BulkRequest();
        s.add(new DeleteRequest("orders", "o-3").setIfSeqNo(100L).setIfPrimaryTerm(2L));
        assertRoundTrip(s);
    }

    public void testClusterLevelFields() {
        BulkRequest s = new BulkRequest();
        s.timeout(TimeValue.timeValueSeconds(30));
        s.setRefreshPolicy(RefreshPolicy.IMMEDIATE);
        s.waitForActiveShards(ActiveShardCount.ALL);
        s.add(new IndexRequest("orders").id("a").source("{}", XContentType.JSON));
        assertRoundTrip(s);
    }

    public void testMixedFourOpBulk() {
        BulkRequest s = new BulkRequest();
        s.add(new IndexRequest("o").id("a").source(Map.of("k", 1), XContentType.JSON));
        s.add(new IndexRequest("o").id("b").source(Map.of("k", 2), XContentType.JSON).opType(IndexRequest.OpType.CREATE));
        s.add(new UpdateRequest("o", "c").doc(Map.of("k", 3), XContentType.JSON));
        s.add(new DeleteRequest("o", "d"));
        assertRoundTrip(s);
    }

    /** Runs the round trip and asserts the reconstructed BulkRequest equals the original on every field we care about. */
    private static void assertRoundTrip(BulkRequest s) {
        org.opensearch.protobufs.BulkRequest proto = BulkProtoConverter.toProto(s);
        BulkRequest s2 = BulkRequestProtoUtils.prepareRequest(proto);
        assertBulkEquivalent(s, s2);
    }

    /**
     * Manual field-by-field equivalence check. {@link BulkRequest} doesn't override {@code equals()}, and the framework's
     * notion of equality (reference identity) isn't useful for round-trip validation.
     */
    private static void assertBulkEquivalent(BulkRequest a, BulkRequest b) {
        assertEquals("timeout", a.timeout(), b.timeout());
        assertEquals("refresh", a.getRefreshPolicy(), b.getRefreshPolicy());
        assertEquals("waitForActiveShards", a.waitForActiveShards(), b.waitForActiveShards());
        assertEquals("ops count", a.requests().size(), b.requests().size());
        for (int i = 0; i < a.requests().size(); i++) {
            assertDocWriteEquivalent("op[" + i + "]", a.requests().get(i), b.requests().get(i));
        }
    }

    private static void assertDocWriteEquivalent(String prefix, DocWriteRequest<?> a, DocWriteRequest<?> b) {
        assertEquals(prefix + " class", a.getClass(), b.getClass());
        assertEquals(prefix + " index", a.index(), b.index());
        assertEquals(prefix + " id", a.id(), b.id());
        assertEquals(prefix + " routing", a.routing(), b.routing());
        assertEquals(prefix + " opType", a.opType(), b.opType());
        assertEquals(prefix + " version", a.version(), b.version());
        assertEquals(prefix + " versionType", a.versionType(), b.versionType());

        if (a instanceof IndexRequest && b instanceof IndexRequest) {
            IndexRequest ia = (IndexRequest) a, ib = (IndexRequest) b;
            assertEquals(prefix + " source", ia.source(), ib.source());
            assertEquals(prefix + " pipeline", ia.getPipeline(), ib.getPipeline());
            assertEquals(prefix + " ifSeqNo", ia.ifSeqNo(), ib.ifSeqNo());
            assertEquals(prefix + " ifPrimaryTerm", ia.ifPrimaryTerm(), ib.ifPrimaryTerm());
        } else if (a instanceof UpdateRequest && b instanceof UpdateRequest) {
            UpdateRequest ua = (UpdateRequest) a, ub = (UpdateRequest) b;
            assertEquals(prefix + " retryOnConflict", ua.retryOnConflict(), ub.retryOnConflict());
            assertEquals(prefix + " docAsUpsert", ua.docAsUpsert(), ub.docAsUpsert());
            assertEquals(prefix + " doc presence", ua.doc() == null, ub.doc() == null);
            if (ua.doc() != null && ub.doc() != null) {
                assertEquals(prefix + " doc source", ua.doc().source(), ub.doc().source());
            }
            assertEquals(prefix + " upsert presence", ua.upsertRequest() == null, ub.upsertRequest() == null);
            if (ua.upsertRequest() != null && ub.upsertRequest() != null) {
                assertEquals(prefix + " upsert source", ua.upsertRequest().source(), ub.upsertRequest().source());
            }
        } else if (a instanceof DeleteRequest && b instanceof DeleteRequest) {
            DeleteRequest da = (DeleteRequest) a, db = (DeleteRequest) b;
            assertEquals(prefix + " ifSeqNo", da.ifSeqNo(), db.ifSeqNo());
            assertEquals(prefix + " ifPrimaryTerm", da.ifPrimaryTerm(), db.ifPrimaryTerm());
        }
    }
}
