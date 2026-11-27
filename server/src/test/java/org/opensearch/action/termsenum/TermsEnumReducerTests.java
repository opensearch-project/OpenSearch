/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.termsenum;

import org.apache.lucene.util.BytesRef;
import org.opensearch.OpenSearchTimeoutException;
import org.opensearch.Version;
import org.opensearch.action.NoShardAvailableActionException;
import org.opensearch.action.search.SearchPhaseExecutionException;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.action.termsenum.TermsEnumReducer.ShardResult;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.ReceiveTimeoutTransportException;
import org.opensearch.transport.RemoteTransportException;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;

public class TermsEnumReducerTests extends OpenSearchTestCase {

    private static ShardId shard(int i) {
        return new ShardId("idx", "uuid", i);
    }

    private static ShardResult ok(int i, boolean timedOut, String... terms) {
        return ShardResult.of(shard(i), TermsEnumShardResponse.ok(List.of(terms), timedOut));
    }

    private static ShardResult of(int i, TermsEnumShardResponse response) {
        return ShardResult.of(shard(i), response);
    }

    private static ShardResult failed(int i) {
        return ShardResult.failed(shard(i), new NoShardAvailableActionException(shard(i), "no copy"));
    }

    private static ShardResult unanswered(int i) {
        return ShardResult.failed(shard(i), new OpenSearchTimeoutException("shard did not respond within [1s]"));
    }

    private static ShardResult unsupported(int i, String reason) {
        return of(i, TermsEnumShardResponse.fieldUnsupported(reason));
    }

    public void testMergeDedupesAndOrders() {
        assertEquals(
            List.of("a", "b", "c", "d", "e"),
            TermsEnumReducer.mergeTerms(List.of(List.of("a", "c", "e"), List.of("b", "c", "d")), 10, null)
        );
    }

    public void testMergeCutsAtSize() {
        assertEquals(List.of("a", "b"), TermsEnumReducer.mergeTerms(List.of(List.of("a", "c", "e"), List.of("b", "c", "d")), 2, null));
    }

    public void testMergeKeepsCaseVariants() {
        assertEquals(List.of("WEB", "Web", "web"), TermsEnumReducer.mergeTerms(List.of(List.of("Web", "web"), List.of("WEB")), 10, null));
    }

    public void testMergeUsesUtf8ByteOrder() {
        // U+1F600 is a surrogate pair in UTF-16 (sorts before U+FFFD with String.compareTo) but 4 bytes in UTF-8 (sorts after)
        String emoji = "😀";
        String replacement = "�";
        assertTrue(emoji.compareTo(replacement) < 0);
        assertEquals(List.of(replacement, emoji), TermsEnumReducer.mergeTerms(List.of(List.of(emoji), List.of(replacement)), 10, null));
    }

    public void testMergeDropsTermsNotAfterSearchAfter() {
        assertEquals(List.of("d", "e"), TermsEnumReducer.mergeTerms(List.of(List.of("a", "c", "e"), List.of("b", "d")), 10, "c"));
    }

    public void testMergeRandomMatchesSortedDistinct() {
        Comparator<String> byBytes = Comparator.comparing(BytesRef::new);
        int lists = randomIntBetween(1, 8);
        List<List<String>> input = new ArrayList<>();
        TreeSet<String> expected = new TreeSet<>(byBytes);
        for (int i = 0; i < lists; i++) {
            TreeSet<String> one = new TreeSet<>(byBytes);
            int n = randomIntBetween(0, 20);
            for (int j = 0; j < n; j++) {
                one.add(randomRealisticUnicodeOfLengthBetween(1, 4));
            }
            input.add(new ArrayList<>(one));
            expected.addAll(one);
        }
        int size = randomIntBetween(1, 40);
        List<String> expectedList = new ArrayList<>(expected);
        if (expectedList.size() > size) {
            expectedList = expectedList.subList(0, size);
        }
        assertEquals(expectedList, TermsEnumReducer.mergeTerms(input, size, null));
    }

    public void testReduceAllOk() {
        TermsEnumResponse r = TermsEnumReducer.reduce(List.of(ok(0, false, "a"), ok(1, false, "b")), 10, null);
        assertEquals(List.of("a", "b"), r.getTerms());
        assertTrue(r.isComplete());
        assertEquals(2, r.getTotalShards());
        assertEquals(2, r.getSuccessfulShards());
        assertEquals(0, r.getFailedShards());
    }

    public void testReduceTimedOutShardIsIncomplete() {
        TermsEnumResponse r = TermsEnumReducer.reduce(List.of(ok(0, true, "a"), ok(1, false, "b")), 10, null);
        assertEquals(List.of("a", "b"), r.getTerms());
        assertEquals(0, r.getFailedShards());
        assertFalse(r.isComplete());
    }

    public void testReduceFailureMakesIncomplete() {
        TermsEnumResponse r = TermsEnumReducer.reduce(List.of(ok(0, false, "a"), failed(1)), 10, null);
        assertEquals(List.of("a"), r.getTerms());
        assertEquals(1, r.getFailedShards());
        assertEquals(1, r.getSuccessfulShards());
        assertEquals(1, r.getShardFailures().length);
        assertFalse(r.isComplete());
    }

    public void testReduceUnsupportedWithOkIsPartial() {
        TermsEnumResponse r = TermsEnumReducer.reduce(
            List.of(ok(0, false, "a"), of(1, TermsEnumShardResponse.fieldUnsupported("field [f] of type [long] is not supported"))),
            10,
            null
        );
        assertEquals(1, r.getFailedShards());
        assertTrue(r.getShardFailures()[0].reason(), r.getShardFailures()[0].reason().contains("of type [long] is not supported"));
        assertEquals("idx", r.getShardFailures()[0].index());
        assertEquals(1, r.getShardFailures()[0].shardId());
        assertFalse(r.isComplete());
        assertEquals(List.of("a"), r.getTerms());
    }

    public void testReduceUnsupportedWithoutOkIsBadRequest() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> TermsEnumReducer.reduce(
                List.of(
                    of(0, TermsEnumShardResponse.fieldAbsent()),
                    of(1, TermsEnumShardResponse.fieldUnsupported("r1")),
                    of(2, TermsEnumShardResponse.fieldUnsupported("r2")),
                    of(3, TermsEnumShardResponse.skipped())
                ),
                10,
                null
            )
        );
        assertEquals("r1", e.getMessage());
    }

    public void testReduceUnsupportedWithUnansweredIsPartial() {
        // the unanswered shard may map the field as a keyword, so this is not "unsupported everywhere"
        TermsEnumResponse r = TermsEnumReducer.reduce(List.of(unsupported(0, "r0"), unanswered(1)), 10, null);
        assertEquals(List.of(), r.getTerms());
        assertFalse(r.isComplete());
        assertEquals(2, r.getTotalShards());
        assertEquals(2, r.getFailedShards());
        assertEquals(2, r.getShardFailures().length);
    }

    public void testReduceUnsupportedWithAbsentAndFailureIsPartial() {
        TermsEnumResponse r = TermsEnumReducer.reduce(
            List.of(of(0, TermsEnumShardResponse.fieldAbsent()), unsupported(1, "r1"), failed(2)),
            10,
            null
        );
        assertEquals(List.of(), r.getTerms());
        assertFalse(r.isComplete());
        assertEquals(2, r.getFailedShards());
    }

    public void testReduceUnsupportedWithNoShardAvailableIsAllShardsFailed() {
        SearchPhaseExecutionException e = expectThrows(
            SearchPhaseExecutionException.class,
            () -> TermsEnumReducer.reduce(List.of(unsupported(0, "r0"), failed(1)), 10, null)
        );
        assertEquals("all shards failed", e.getMessage());
        assertEquals(RestStatus.SERVICE_UNAVAILABLE, e.status());
        assertEquals(2, e.shardFailures().length);
    }

    public void testReduceUnsupportedWithRejectionKeepsRetryableStatus() {
        ShardResult rejected = ShardResult.failed(shard(1), new OpenSearchRejectedExecutionException("rejected"));
        SearchPhaseExecutionException e = expectThrows(
            SearchPhaseExecutionException.class,
            () -> TermsEnumReducer.reduce(List.of(unsupported(0, "r0"), rejected), 10, null)
        );
        assertEquals(RestStatus.TOO_MANY_REQUESTS, e.status());
        assertEquals(2, e.shardFailures().length);
    }

    public void testReduceAbsentOnlyIsCompleteAndEmpty() {
        TermsEnumResponse r = TermsEnumReducer.reduce(
            List.of(of(0, TermsEnumShardResponse.fieldAbsent()), of(1, TermsEnumShardResponse.fieldAbsent())),
            10,
            null
        );
        assertTrue(r.getTerms().isEmpty());
        assertTrue(r.isComplete());
        assertEquals(0, r.getFailedShards());
    }

    public void testReduceSkippedOnlyIsCompleteAndEmpty() {
        TermsEnumResponse r = TermsEnumReducer.reduce(
            List.of(of(0, TermsEnumShardResponse.skipped()), of(1, TermsEnumShardResponse.skipped())),
            10,
            null
        );
        assertTrue(r.getTerms().isEmpty());
        assertTrue(r.isComplete());
        assertEquals(2, r.getSkippedShards());
        assertEquals(2, r.getSuccessfulShards());
        assertEquals(2, r.getTotalShards());
    }

    public void testReduceAbsentPlusFailureIsIncomplete() {
        TermsEnumResponse r = TermsEnumReducer.reduce(List.of(of(0, TermsEnumShardResponse.fieldAbsent()), failed(1)), 10, null);
        assertTrue(r.getTerms().isEmpty());
        assertFalse(r.isComplete());
    }

    public void testReduceAllFailedIsAllShardsFailed() {
        SearchPhaseExecutionException e = expectThrows(
            SearchPhaseExecutionException.class,
            () -> TermsEnumReducer.reduce(List.of(failed(0), failed(1)), 10, null)
        );
        assertEquals("all shards failed", e.getMessage());
        assertEquals(RestStatus.SERVICE_UNAVAILABLE, e.status());
        assertEquals(2, e.shardFailures().length);
        for (int i = 0; i < 2; i++) {
            ShardSearchFailure failure = e.shardFailures()[i];
            assertEquals("idx", failure.index());
            assertEquals(i, failure.shardId());
            assertEquals(shard(i), failure.shard().getShardId());
        }
    }

    public void testReduceAllUnansweredIsEmptyAndIncomplete() {
        TermsEnumResponse r = TermsEnumReducer.reduce(List.of(unanswered(0), unanswered(1)), 10, null);
        assertTrue(r.getTerms().isEmpty());
        assertFalse(r.isComplete());
        assertEquals(2, r.getFailedShards());

        TermsEnumResponse mixed = TermsEnumReducer.reduce(List.of(unanswered(0), failed(1)), 10, null);
        assertTrue(mixed.getTerms().isEmpty());
        assertFalse(mixed.isComplete());
        assertEquals(2, mixed.getFailedShards());
    }

    public void testReduceHonorsSizeAndSearchAfter() {
        TermsEnumResponse r = TermsEnumReducer.reduce(List.of(ok(0, false, "a", "c", "e"), ok(1, false, "b", "d", "f")), 2, "b");
        assertEquals(List.of("c", "d"), r.getTerms());
        assertTrue(r.isComplete());
    }

    public void testReduceWrappedTimeoutCountsAsUnanswered() {
        DiscoveryNode node = new DiscoveryNode("n", buildNewFakeTransportAddress(), Version.CURRENT);
        // the coordinator's own timeout, with the transport timeout as its cause
        ShardResult withCause = ShardResult.failed(
            shard(0),
            new OpenSearchTimeoutException("shard did not respond within [1s]", new ReceiveTimeoutTransportException(node, "a", "t"))
        );
        // a timeout raised on another node arrives wrapped
        ShardResult wrapped = ShardResult.failed(
            shard(1),
            new RemoteTransportException("remote", new OpenSearchTimeoutException("shard did not respond within [1s]"))
        );
        TermsEnumResponse r = TermsEnumReducer.reduce(List.of(withCause, wrapped, failed(2)), 10, null);
        assertEquals(List.of(), r.getTerms());
        assertFalse(r.isComplete());
        assertEquals(3, r.getFailedShards());
    }

    public void testReduceNoShards() {
        TermsEnumResponse r = TermsEnumReducer.reduce(List.of(), 10, null);
        assertEquals(0, r.getTotalShards());
        assertTrue(r.getTerms().isEmpty());
        assertTrue(r.isComplete());
    }
}
