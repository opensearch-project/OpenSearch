/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.termsenum;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchException;
import org.opensearch.OpenSearchTimeoutException;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.admin.cluster.node.tasks.cancel.CancelTasksResponse;
import org.opensearch.action.admin.indices.datastream.DataStreamTestCase;
import org.opensearch.action.bulk.BulkRequestBuilder;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.index.IndexModule;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.indices.IndexClosedException;
import org.opensearch.plugins.Plugin;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;
import org.opensearch.test.OpenSearchIntegTestCase.Scope;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.test.transport.StubbableTransport;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportMessageListener;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestHandler;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportService;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.opensearch.action.termsenum.TransportTermsEnumAction.SHARD_ACTION_NAME;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

/**
 * Runs terms_enum on a real cluster: merging and paging across indices and shards, target resolution, shard skipping,
 * and what the coordinator answers when a shard does not answer, runs out of time, fails or is cancelled.
 */
@ClusterScope(scope = Scope.TEST, numDataNodes = 2)
public class TermsEnumIT extends DataStreamTestCase {

    /** A timeout no healthy shard comes close to, so a response is only incomplete when a test makes it so. */
    private static final TimeValue GENEROUS_TIMEOUT = TimeValue.timeValueSeconds(30);

    /** The {@code next()} call on a {@code tag} terms enum of a {@code slow-*} index that stalls. */
    private static final int STALL_AT_NEXT_CALL = 200;

    /**
     * Until when, as a {@link System#nanoTime()} reading, the stalling terms enum parks its thread. {@link Long#MIN_VALUE}
     * means it does not stall.
     */
    static volatile long STALL_UNTIL_NANOS = Long.MIN_VALUE;

    /** The longest single park of a stall, so that a stall notices a reset of {@link #STALL_UNTIL_NANOS} quickly. */
    private static final long MAX_PARK_NANOS = TimeUnit.MILLISECONDS.toNanos(10);

    /** Wraps the readers of {@code slow-*} indices so that listing their {@code tag} terms stalls once. */
    public static class StallingReaderWrapperPlugin extends Plugin {
        @Override
        public void onIndexModule(IndexModule indexModule) {
            if (indexModule.getIndex().getName().startsWith("slow-")) {
                indexModule.setReaderWrapper(indexService -> StallingDirectoryReader::new);
            }
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(MockTransportService.TestPlugin.class, StallingReaderWrapperPlugin.class);
    }

    public void testPagingAcrossIndicesAndShardsMatchesSortedDistinctTerms() {
        int shardsA = randomIntBetween(2, 5);
        int shardsB = randomIntBetween(1, 3);
        createTagIndex("logs-a", shards(shardsA, 0));
        createTagIndex("logs-b", shards(shardsB, 0));
        List<String> terms = new ArrayList<>();
        while (terms.size() < 150) {
            String term = randomAlphaOfLengthBetween(1, 6);
            terms.add(term);
            if (terms.size() < 150 && randomBoolean()) {
                // differs only in case, so it is a distinct term
                terms.add(swapCase(term));
            }
        }
        BulkRequestBuilder bulk = client().prepareBulk();
        for (String term : terms) {
            int target = randomIntBetween(0, 2);
            if (target != 1) {
                bulk.add(client().prepareIndex("logs-a").setSource("tag", term));
            }
            if (target != 0) {
                bulk.add(client().prepareIndex("logs-b").setSource("tag", term));
            }
        }
        assertNoFailures(bulk.get());
        refresh("logs-a", "logs-b");

        List<String> paged = new ArrayList<>();
        String searchAfter = null;
        for (int page = 0;; page++) {
            assertThat("paging did not end", page, lessThanOrEqualTo(terms.size() / 7 + 1));
            TermsEnumRequest request = new TermsEnumRequest("logs-a", "logs-b").field("tag").size(7).timeout(GENEROUS_TIMEOUT);
            if (searchAfter != null) {
                request.searchAfter(searchAfter);
            }
            TermsEnumResponse response = termsEnum(request);
            assertTrue(response.isComplete());
            assertEquals(shardsA + shardsB, response.getTotalShards());
            if (response.getTerms().isEmpty()) {
                break;
            }
            assertThat(response.getTerms().size(), lessThanOrEqualTo(7));
            paged.addAll(response.getTerms());
            searchAfter = response.getTerms().get(response.getTerms().size() - 1);
        }
        assertEquals(sortedDistinct(terms), paged);
    }

    public void testIndexFilterSkipsShards() {
        createYearIndices();
        TermsEnumResponse response = termsEnum(
            new TermsEnumRequest("if-2025", "if-2026").field("tag")
                .indexFilter(QueryBuilders.rangeQuery("@timestamp").gte("2026-01-01"))
                .timeout(GENEROUS_TIMEOUT)
        );
        assertEquals(List.of("new-b"), response.getTerms());
        assertEquals(4, response.getTotalShards());
        assertEquals(2, response.getSkippedShards());
        assertEquals(4, response.getSuccessfulShards());
        assertEquals(0, response.getFailedShards());
        assertTrue(response.isComplete());
    }

    public void testAliasAndWildcardTargets() {
        createYearIndices();
        assertAcked(client().admin().indices().prepareAliases().addAlias(new String[] { "if-2025", "if-2026" }, "both"));
        for (String target : List.of("both", "if-*")) {
            TermsEnumResponse response = termsEnum(new TermsEnumRequest(target).field("tag").timeout(GENEROUS_TIMEOUT));
            assertEquals(target, List.of("new-b", "old-a"), response.getTerms());
            assertEquals(target, 4, response.getTotalShards());
            assertTrue(target, response.isComplete());
        }
    }

    public void testClosedIndexIsRejectedOrSkippedLikeSearch() {
        createYearIndices();
        assertAcked(client().admin().indices().prepareClose("if-2025"));

        IndexClosedException named = expectThrows(
            IndexClosedException.class,
            () -> termsEnum(new TermsEnumRequest("if-2025", "if-2026").field("tag").timeout(GENEROUS_TIMEOUT))
        );
        assertEquals("if-2025", named.getIndex().getName());

        TermsEnumRequest lenient = new TermsEnumRequest("if-2025", "if-2026").field("tag").timeout(GENEROUS_TIMEOUT);
        lenient.indicesOptions(IndicesOptions.fromOptions(true, true, true, false, lenient.indicesOptions()));
        TermsEnumResponse skipped = termsEnum(lenient);
        assertEquals(List.of("new-b"), skipped.getTerms());
        assertEquals(2, skipped.getTotalShards());
        assertTrue(skipped.isComplete());

        TermsEnumResponse wildcard = termsEnum(new TermsEnumRequest("if-*").field("tag").timeout(GENEROUS_TIMEOUT));
        assertEquals(List.of("new-b"), wildcard.getTerms());
        assertEquals(2, wildcard.getTotalShards());
        assertTrue(wildcard.isComplete());

        TermsEnumRequest expandClosed = new TermsEnumRequest("if-*").field("tag").timeout(GENEROUS_TIMEOUT);
        expandClosed.indicesOptions(IndicesOptions.fromOptions(false, true, true, true, expandClosed.indicesOptions()));
        expectThrows(IndexClosedException.class, () -> termsEnum(expandClosed));
    }

    public void testDataStream() throws Exception {
        createDataStreamIndexTemplate("logs-template", List.of("logs-ds-*"));
        for (String host : List.of("web-02", "web-01")) {
            client().prepareIndex("logs-ds-web")
                .setOpType(DocWriteRequest.OpType.CREATE)
                .setSource("@timestamp", "2026-11-01T00:00:00Z", "host", host)
                .get();
        }
        ensureGreen();
        refresh("logs-ds-web");
        String backingIndex = getDataStreams("logs-ds-web").getDataStreams().get(0).getDataStream().getIndices().get(0).getName();

        TermsEnumResponse hosts = termsEnum(
            new TermsEnumRequest("logs-ds-web").field("host.keyword").string("web").timeout(GENEROUS_TIMEOUT)
        );
        assertEquals(List.of("web-01", "web-02"), hosts.getTerms());
        assertTrue(hosts.isComplete());
        TermsEnumResponse indices = termsEnum(
            new TermsEnumRequest("logs-ds-web").field("_index").string(".ds-logs-ds-web").timeout(GENEROUS_TIMEOUT)
        );
        assertEquals(List.of(backingIndex), indices.getTerms());
        assertTrue(indices.isComplete());
    }

    public void testUnansweredShardMakesResponseIncomplete() throws Exception {
        createTagIndex("partial", shards(2, 0));
        Map<Integer, List<String>> termsByShard = new HashMap<>();
        List<String> allTerms = new ArrayList<>();
        for (int i = 0; i < 20 || termsByShard.size() < 2; i++) {
            String term = randomAlphaOfLengthBetween(1, 6);
            IndexResponse indexed = client().prepareIndex("partial").setSource("tag", term).get();
            termsByShard.computeIfAbsent(indexed.getShardId().id(), shard -> new ArrayList<>()).add(term);
            allTerms.add(term);
        }
        refresh("partial");
        // without held requests every term comes back; this also warms up the path the timeout below relies on
        TermsEnumResponse baseline = termsEnum(new TermsEnumRequest("partial").field("tag").size(100).timeout(GENEROUS_TIMEOUT));
        assertEquals(sortedDistinct(allTerms), baseline.getTerms());
        assertTrue(baseline.isComplete());

        try (HeldShardRequests held = holdShardRequests(shardId -> shardId.id() == 0)) {
            // long enough for the shard that is not held to answer well within its budget, even on a loaded machine
            TermsEnumResponse response = termsEnum(
                new TermsEnumRequest("partial").field("tag").size(100).timeout(TimeValue.timeValueSeconds(2))
            );
            assertFalse(response.isComplete());
            assertEquals(2, response.getTotalShards());
            assertEquals(1, response.getFailedShards());
            TermsEnumResponse.ShardFailure failure = response.getShardFailures()[0];
            assertEquals(0, failure.shardId());
            assertThat(ExceptionsHelper.unwrapCause(failure.getCause()), instanceOf(OpenSearchTimeoutException.class));
            assertEquals(sortedDistinct(termsByShard.get(1)), response.getTerms());
            assertThat(held.budgets(), hasSize(2));
            for (long budget : held.budgets()) {
                assertThat(budget, allOf(greaterThan(0L), lessThan(2000L)));
            }
        }
    }

    public void testEveryShardUnansweredIsEmptyAndIncomplete() throws Exception {
        createTagIndex("silent", shards(1, 0));
        client().prepareIndex("silent").setSource("tag", "quiet").get();
        refresh("silent");
        try (HeldShardRequests held = holdShardRequests(shardId -> true)) {
            TermsEnumResponse response = termsEnum(new TermsEnumRequest("silent").field("tag").timeout(TimeValue.timeValueMillis(500)));
            assertEquals(List.of(), response.getTerms());
            assertFalse(response.isComplete());
            assertEquals(1, response.getTotalShards());
            assertEquals(0, response.getSuccessfulShards());
            assertEquals(1, response.getFailedShards());
            assertThat(
                ExceptionsHelper.unwrapCause(response.getShardFailures()[0].getCause()),
                instanceOf(OpenSearchTimeoutException.class)
            );
            assertThat(held.requests(), hasSize(1));
        }
    }

    /**
     * A shard that never answers is reported as unanswered when the coordinator's own timer fires. The transport timeout
     * on the shard request, which runs a grace period past that deadline, then drops the response handler even though
     * the shard still has not answered.
     */
    public void testSafetyTimeoutReleasesShardThatNeverAnswers() throws Exception {
        createTagIndex("mute", shards(1, 0));
        client().prepareIndex("mute").setSource("tag", "quiet").get();
        refresh("mute");
        String coordinator = randomFrom(internalCluster().getNodeNames());
        MockTransportService coordinatorTransport = mockTransport(coordinator);
        List<SentShardRequest> sent = new CopyOnWriteArrayList<>();
        TransportMessageListener recordSent = new TransportMessageListener() {
            @Override
            public void onRequestSent(
                DiscoveryNode node,
                long requestId,
                String action,
                TransportRequest request,
                TransportRequestOptions options
            ) {
                if (SHARD_ACTION_NAME.equals(action)) {
                    sent.add(new SentShardRequest(requestId, options.timeout()));
                }
            }
        };
        coordinatorTransport.addMessageListener(recordSent);
        try (HeldShardRequests held = holdShardRequests(shardId -> true)) {
            long startNanos = System.nanoTime();
            TermsEnumResponse response = client(coordinator).execute(
                TermsEnumAction.INSTANCE,
                new TermsEnumRequest("mute").field("tag").timeout(TimeValue.timeValueMillis(500))
            ).actionGet();
            long tookMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

            assertEquals(List.of(), response.getTerms());
            assertFalse(response.isComplete());
            assertEquals(1, response.getFailedShards());
            Throwable cause = ExceptionsHelper.unwrapCause(response.getShardFailures()[0].getCause());
            assertThat(cause, instanceOf(OpenSearchTimeoutException.class));
            // the coordinator's timer answered at the request's deadline; a transport timeout would be the cause
            assertThat(cause.getCause(), nullValue());
            assertThat(tookMillis, greaterThanOrEqualTo(500L));

            assertThat(sent, hasSize(1));
            SentShardRequest shardRequest = sent.get(0);
            assertNotNull("the shard request carries no transport timeout", shardRequest.transportTimeout());
            // the transport timeout runs past the client's deadline, so it never answers the client
            assertThat(shardRequest.transportTimeout().millis(), greaterThan(500L));
            assertBusy(() -> assertFalse(coordinatorTransport.transport().getResponseHandlers().contains(shardRequest.requestId())));
            assertThat(held.requests(), hasSize(1));
            assertFalse("the shard was never meant to answer", held.requests().get(0).released().get());
        } finally {
            coordinatorTransport.removeMessageListener(recordSent);
        }
    }

    /** The design's tiny budget against a large dictionary, made deterministic by stalling the shard past its own deadline. */
    public void testShardDeadlineReturnsPartialTerms() throws Exception {
        createTagIndex("slow-terms", shards(1, 0));
        List<String> expected = new ArrayList<>();
        BulkRequestBuilder bulk = client().prepareBulk();
        for (int i = 0; i < 1000; i++) {
            String term = String.format(Locale.ROOT, "t%04d", i);
            expected.add(term);
            bulk.add(client().prepareIndex("slow-terms").setSource("tag", term));
        }
        assertNoFailures(bulk.get());
        refresh("slow-terms");
        assertNoFailures(client().admin().indices().prepareForceMerge("slow-terms").setMaxNumSegments(1).get());
        refresh("slow-terms");

        List<Long> budgets = new CopyOnWriteArrayList<>();
        onShardRequests(node -> (handler, request, channel, task) -> {
            budgets.add(request.timeoutMillis());
            // the handler starts the shard's clock right after this, so the stall ends about 200 ms after the shard's
            // deadline, which leaves the shard about 800 ms of the coordinator's 1000 ms reserve to answer
            STALL_UNTIL_NANOS = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(request.timeoutMillis() + 200);
            handler.messageReceived(request, channel, task);
        });
        try {
            TermsEnumResponse response = termsEnum(
                new TermsEnumRequest("slow-terms").field("tag").size(1000).timeout(TimeValue.timeValueSeconds(10))
            );
            assertFalse(response.isComplete());
            assertEquals(0, response.getFailedShards());
            int returned = response.getTerms().size();
            assertThat(returned, allOf(greaterThan(0), lessThan(1000)));
            assertEquals(expected.subList(0, returned), response.getTerms());
            assertThat(budgets, hasSize(1));
            assertThat(budgets.get(0), allOf(greaterThan(0L), lessThan(10000L)));
        } finally {
            STALL_UNTIL_NANOS = Long.MIN_VALUE;
            clearShardRequestBehaviors();
        }
    }

    public void testCancellationStopsShardWork() throws Exception {
        int shards = randomIntBetween(1, 3);
        createTagIndex("cancel-me", shards(shards, 0));
        client().prepareIndex("cancel-me").setSource("tag", "doomed").get();
        refresh("cancel-me");
        try (HeldShardRequests held = holdShardRequests(shardId -> true)) {
            ActionFuture<TermsEnumResponse> future = client().execute(
                TermsEnumAction.INSTANCE,
                new TermsEnumRequest("cancel-me").field("tag").timeout(GENEROUS_TIMEOUT)
            );
            assertBusy(() -> assertThat(held.requests(), hasSize(shards)));

            CancelTasksResponse cancelled = client().admin().cluster().prepareCancelTasks().setActions(TermsEnumAction.NAME).get();
            assertThat(cancelled.getTasks(), hasSize(1));
            // cancelling the coordinator's task cancels the shard tasks it started
            assertBusy(() -> {
                for (HeldRequest request : held.requests()) {
                    assertTrue(((CancellableTask) request.task()).isCancelled());
                }
            });
            for (HeldRequest request : held.requests()) {
                request.runHandler();
            }
            // ends well before the 30 s timeout
            expectThrows(TaskCancelledException.class, () -> future.actionGet(TimeValue.timeValueSeconds(10)));
        }
        assertBusy(
            () -> assertThat(client().admin().cluster().prepareListTasks().setActions(TermsEnumAction.NAME + "*").get().getTasks(), empty())
        );
    }

    public void testFailsOverToAnotherCopy() {
        // green puts the replica on the data node that does not hold the primary
        createTagIndex("failover", shards(1, 1));
        List<String> terms = List.of("fo-a", "fo-b", "fo-c");
        for (String term : terms) {
            client().prepareIndex("failover").setSource("tag", term).get();
        }
        refresh("failover");

        AtomicBoolean injected = new AtomicBoolean();
        List<String> receivedOn = new CopyOnWriteArrayList<>();
        onShardRequests(node -> (handler, request, channel, task) -> {
            receivedOn.add(node);
            if (injected.compareAndSet(false, true)) {
                channel.sendResponse(new OpenSearchException("injected"));
            } else {
                handler.messageReceived(request, channel, task);
            }
        });
        try {
            TermsEnumResponse response = termsEnum(new TermsEnumRequest("failover").field("tag").timeout(GENEROUS_TIMEOUT));
            assertTrue(response.isComplete());
            assertEquals(0, response.getFailedShards());
            assertEquals(terms, response.getTerms());
            assertTrue(injected.get());
            // the replica is on the other data node, so the retry went to the other copy
            assertThat(receivedOn, hasSize(2));
            assertThat(receivedOn.get(1), not(receivedOn.get(0)));
        } finally {
            clearShardRequestBehaviors();
        }
    }

    private static Settings shards(int shards, int replicas) {
        return Settings.builder().put("index.number_of_shards", shards).put("index.number_of_replicas", replicas).build();
    }

    private void createTagIndex(String index, Settings settings) {
        assertAcked(prepareCreate(index).setSettings(settings).setMapping("tag", "type=keyword"));
        ensureGreen(index);
    }

    /** Creates {@code if-2025} with {@code old-a} and {@code if-2026} with {@code new-b}, each in both of its shards. */
    private void createYearIndices() {
        createYearIndex("if-2025", "old-a", "2025-06-01");
        createYearIndex("if-2026", "new-b", "2026-06-01");
        refresh("if-2025", "if-2026");
    }

    private void createYearIndex(String index, String tag, String timestamp) {
        assertAcked(prepareCreate(index).setSettings(shards(2, 0)).setMapping("tag", "type=keyword", "@timestamp", "type=date"));
        ensureGreen(index);
        // an empty shard has no points, so the range filter would skip it as well
        Set<Integer> filled = new HashSet<>();
        for (int i = 0; filled.size() < 2; i++) {
            IndexResponse response = client().prepareIndex(index).setRouting("r" + i).setSource("tag", tag, "@timestamp", timestamp).get();
            filled.add(response.getShardId().id());
        }
    }

    private static TermsEnumResponse termsEnum(TermsEnumRequest request) {
        return client().execute(TermsEnumAction.INSTANCE, request).actionGet();
    }

    /** The terms in UTF-8 byte order without duplicates, which is how terms_enum returns them. */
    private static List<String> sortedDistinct(Collection<String> terms) {
        return terms.stream().map(BytesRef::new).distinct().sorted().map(BytesRef::utf8ToString).collect(Collectors.toList());
    }

    private static String swapCase(String term) {
        StringBuilder swapped = new StringBuilder(term.length());
        for (char c : term.toCharArray()) {
            swapped.append(Character.isUpperCase(c) ? Character.toLowerCase(c) : Character.toUpperCase(c));
        }
        return swapped.toString();
    }

    private static MockTransportService mockTransport(String node) {
        return (MockTransportService) internalCluster().getInstance(TransportService.class, node);
    }

    /** Replaces the shard request handler on every node with the behavior made for that node's name. */
    private void onShardRequests(Function<String, StubbableTransport.RequestHandlingBehavior<TermsEnumShardRequest>> behavior) {
        for (String node : internalCluster().getNodeNames()) {
            mockTransport(node).addRequestHandlingBehavior(SHARD_ACTION_NAME, behavior.apply(node));
        }
    }

    private void clearShardRequestBehaviors() {
        for (String node : internalCluster().getNodeNames()) {
            mockTransport(node).clearAllRules();
        }
    }

    /**
     * Holds back, without running them, the shard requests whose shard matches {@code hold}, and passes the others to
     * the real handler. Every request's budget is recorded.
     */
    private HeldShardRequests holdShardRequests(Predicate<ShardId> hold) {
        HeldShardRequests held = new HeldShardRequests();
        onShardRequests(node -> (handler, request, channel, task) -> {
            held.budgets.add(request.timeoutMillis());
            if (hold.test(request.shardId())) {
                held.requests.add(new HeldRequest(handler, request, channel, task, new AtomicBoolean()));
            } else {
                handler.messageReceived(request, channel, task);
            }
        });
        return held;
    }

    /**
     * The shard requests held by {@link #holdShardRequests}. Closing it restores the real handlers and answers every
     * request still held with a failure, so no channel or shard task outlives the test.
     */
    private final class HeldShardRequests implements Closeable {
        private final List<HeldRequest> requests = new CopyOnWriteArrayList<>();
        private final List<Long> budgets = new CopyOnWriteArrayList<>();

        List<HeldRequest> requests() {
            return requests;
        }

        /** The {@code timeoutMillis()} of every shard request received, held or not. */
        List<Long> budgets() {
            return budgets;
        }

        @Override
        public void close() throws IOException {
            clearShardRequestBehaviors();
            IOException failure = null;
            for (HeldRequest request : requests) {
                try {
                    request.fail();
                } catch (IOException e) {
                    failure = ExceptionsHelper.useOrSuppress(failure, e);
                }
            }
            if (failure != null) {
                throw failure;
            }
        }
    }

    /** A shard request held back from its handler, released at most once. */
    private record HeldRequest(TransportRequestHandler<TermsEnumShardRequest> handler, TermsEnumShardRequest request,
        TransportChannel channel, Task task, AtomicBoolean released) {
        /** Runs the request through the real handler. */
        void runHandler() throws Exception {
            if (released.compareAndSet(false, true)) {
                handler.messageReceived(request, channel, task);
            }
        }

        /** Answers the request with a failure without running it. */
        void fail() throws IOException {
            if (released.compareAndSet(false, true)) {
                channel.sendResponse(new OpenSearchException("released"));
            }
        }
    }

    /** A shard request as the coordinator sent it. */
    private record SentShardRequest(long requestId, TimeValue transportTimeout) {
    }

    /** Wraps every leaf so that listing the {@code tag} terms stalls once, without changing any term. */
    private static final class StallingDirectoryReader extends FilterDirectoryReader {
        StallingDirectoryReader(DirectoryReader in) throws IOException {
            super(in, new SubReaderWrapper() {
                @Override
                public LeafReader wrap(LeafReader reader) {
                    return new StallingLeafReader(reader);
                }
            });
        }

        @Override
        protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
            return new StallingDirectoryReader(in);
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return in.getReaderCacheHelper();
        }
    }

    private static final class StallingLeafReader extends FilterLeafReader {
        StallingLeafReader(LeafReader in) {
            super(in);
        }

        @Override
        public Terms terms(String field) throws IOException {
            Terms terms = super.terms(field);
            if (terms == null || "tag".equals(field) == false) {
                return terms;
            }
            return new FilterTerms(terms) {
                @Override
                public TermsEnum iterator() throws IOException {
                    return new StallingTermsEnum(in.iterator());
                }
            };
        }

        @Override
        public CacheHelper getCoreCacheHelper() {
            return in.getCoreCacheHelper();
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return in.getReaderCacheHelper();
        }
    }

    /** Counts {@code next()} calls and stalls on the one numbered {@code STALL_AT_NEXT_CALL}. */
    private static final class StallingTermsEnum extends FilterLeafReader.FilterTermsEnum {
        private int nextCalls;

        StallingTermsEnum(TermsEnum in) {
            super(in);
        }

        @Override
        public BytesRef next() throws IOException {
            if (++nextCalls == STALL_AT_NEXT_CALL) {
                stall();
            }
            return in.next();
        }

        /**
         * Parks until {@link #STALL_UNTIL_NANOS} has passed; {@code Thread.sleep} is forbidden in this source set. Parks in
         * short steps that re-read the target, so resetting it to {@link Long#MIN_VALUE} ends a stall in progress.
         */
        private static void stall() {
            while (true) {
                long until = STALL_UNTIL_NANOS;
                if (until == Long.MIN_VALUE) {
                    return;
                }
                long remaining = until - System.nanoTime();
                if (remaining <= 0) {
                    return;
                }
                LockSupport.parkNanos(Math.min(remaining, MAX_PARK_NANOS));
            }
        }
    }
}
