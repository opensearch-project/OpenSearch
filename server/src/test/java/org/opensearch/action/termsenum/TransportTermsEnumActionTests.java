/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.termsenum;

import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchException;
import org.opensearch.Version;
import org.opensearch.action.NoShardAvailableActionException;
import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.opensearch.action.search.TransportSearchAction;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.GroupShardsIterator;
import org.opensearch.cluster.routing.OperationRouting;
import org.opensearch.cluster.routing.PlainShardIterator;
import org.opensearch.cluster.routing.ShardIterator;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.IndexService;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.plugins.NetworkPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.opensearch.transport.RemoteTransportException;
import org.opensearch.transport.TransportInterceptor;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestHandler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;

public class TransportTermsEnumActionTests extends OpenSearchSingleNodeTestCase {

    /** Holds the shard requests of one index until the test releases them, so the coordinator's timer answers first. */
    public static class HoldShardRequestsPlugin extends Plugin implements NetworkPlugin {
        static volatile String heldIndex;
        static final List<Runnable> HELD = new CopyOnWriteArrayList<>();

        @Override
        public List<TransportInterceptor> getTransportInterceptors(NamedWriteableRegistry registry, ThreadContext threadContext) {
            return List.of(new TransportInterceptor() {
                @Override
                public <T extends TransportRequest> TransportRequestHandler<T> interceptHandler(
                    String action,
                    String executor,
                    boolean forceExecution,
                    TransportRequestHandler<T> actualHandler
                ) {
                    if (TransportTermsEnumAction.SHARD_ACTION_NAME.equals(action) == false) {
                        return actualHandler;
                    }
                    return (request, channel, task) -> {
                        if (((TermsEnumShardRequest) request).shardId().getIndexName().equals(heldIndex)) {
                            HELD.add(() -> {
                                try {
                                    actualHandler.messageReceived(request, channel, task);
                                } catch (Exception e) {
                                    throw new AssertionError(e);
                                }
                            });
                        } else {
                            actualHandler.messageReceived(request, channel, task);
                        }
                    };
                }
            });
        }

        /** Stops holding and lets every held request run, then waits until each has answered. */
        static void releaseAll(Runnable awaitAnswered) {
            heldIndex = null;
            for (Runnable held : HELD) {
                held.run();
            }
            HELD.clear();
            awaitAnswered.run();
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(HoldShardRequestsPlugin.class);
    }

    private static Settings oneShard() {
        return Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0).build();
    }

    private IndexService createMapped(String index, String mapping) {
        return createMapped(index, Settings.EMPTY, mapping);
    }

    private IndexService createMapped(String index, Settings extra, String mapping) {
        return createIndexWithMappingSource(index, Settings.builder().put(oneShard()).put(extra).build(), mapping);
    }

    private void createKeywordIndex(String index, String... tags) {
        createMapped(index, "{\"properties\":{\"tag\":{\"type\":\"keyword\"}}}");
        for (int i = 0; i < tags.length; i++) {
            index(index, Integer.toString(i), "{\"tag\":\"" + tags[i] + "\"}");
        }
        client().admin().indices().prepareRefresh(index).get();
    }

    private void index(String index, String id, String json) {
        client().prepareIndex(index).setId(id).setSource(json, MediaTypeRegistry.JSON).get();
    }

    private TermsEnumResponse run(TermsEnumRequest r) {
        return client().execute(TermsEnumAction.INSTANCE, r).actionGet();
    }

    public void testMixedMappingsReturnPartialTerms() {
        createKeywordIndex("kw", "a1", "a2");
        createMapped("num", "{\"properties\":{\"tag\":{\"type\":\"long\"}}}");
        index("num", "1", "{\"tag\":1}");
        createMapped("none", "{\"properties\":{\"other\":{\"type\":\"keyword\"}}}");
        index("none", "1", "{\"other\":\"x\"}");
        client().admin().indices().prepareRefresh("num", "none").get();

        TermsEnumResponse response = run(new TermsEnumRequest("kw", "num", "none").field("tag"));
        assertEquals(List.of("a1", "a2"), response.getTerms());
        assertFalse(response.isComplete());
        assertEquals(3, response.getTotalShards());
        assertEquals(1, response.getFailedShards());
        assertEquals("num", response.getShardFailures()[0].index());
        assertThat(response.getShardFailures()[0].reason(), containsString("of type [long] does not support terms enumeration"));
    }

    public void testUnsupportedEverywhereIsBadRequest() {
        createMapped("num", "{\"properties\":{\"tag\":{\"type\":\"long\"}}}");
        index("num", "1", "{\"tag\":1}");
        client().admin().indices().prepareRefresh("num").get();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> run(new TermsEnumRequest("num").field("tag")));
        assertEquals("Field [tag] of type [long] does not support terms enumeration", e.getMessage());
        assertEquals(RestStatus.BAD_REQUEST, ExceptionsHelper.status(e));
    }

    public void testFlatObjectOnlyIsBadRequest() {
        createMapped("flat", "{\"properties\":{\"flat\":{\"type\":\"flat_object\"}}}");
        index("flat", "1", "{\"flat\":{\"a\":\"x\"}}");
        client().admin().indices().prepareRefresh("flat").get();
        // the flat_object field and its internal value subfield both resolve to the flat_object type, which cannot enumerate
        for (String field : List.of("flat", "flat._value")) {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> run(new TermsEnumRequest("flat").field(field)));
            assertThat(field, e.getMessage(), containsString("of type [flat_object] does not support terms enumeration"));
            assertEquals(RestStatus.BAD_REQUEST, ExceptionsHelper.status(e));
        }
    }

    public void testUnmappedEverywhereIsEmptyAndComplete() {
        createKeywordIndex("other");
        TermsEnumResponse response = run(new TermsEnumRequest("other").field("nope"));
        assertEquals(List.of(), response.getTerms());
        assertTrue(response.isComplete());
        assertEquals(1, response.getTotalShards());
        assertEquals(0, response.getFailedShards());
    }

    public void testWildcardMatchingNothingIsEmptyAndComplete() {
        TermsEnumResponse response = run(new TermsEnumRequest("no-such-*").field("tag"));
        assertEquals(List.of(), response.getTerms());
        assertTrue(response.isComplete());
        assertEquals(0, response.getTotalShards());
    }

    public void testIndexFilterSkippingEveryShardIsEmptyAndComplete() {
        createMapped("num", "{\"properties\":{\"tag\":{\"type\":\"long\"}}}");
        index("num", "1", "{\"tag\":1}");
        client().admin().indices().prepareRefresh("num").get();
        // num has no @timestamp, so the range rewrites to match_none and the unsupported field is never looked at
        TermsEnumResponse response = run(
            new TermsEnumRequest("num").field("tag").indexFilter(QueryBuilders.rangeQuery("@timestamp").gte("2030-01-01"))
        );
        assertEquals(List.of(), response.getTerms());
        assertTrue(response.isComplete());
        assertEquals(1, response.getTotalShards());
        assertEquals(response.getTotalShards(), response.getSkippedShards());
    }

    public void testMergesAndDedupesAcrossIndices() {
        createKeywordIndex("kw1", "a1", "b1");
        createKeywordIndex("kw2", "b1", "c1");
        TermsEnumResponse response = run(new TermsEnumRequest("kw1", "kw2").field("tag"));
        assertEquals(List.of("a1", "b1", "c1"), response.getTerms());
        assertTrue(response.isComplete());
    }

    public void testUnansweredShardTimesOutWithPartialTerms() {
        createKeywordIndex("fast", "a1");
        createKeywordIndex("slow", "z1");
        HoldShardRequestsPlugin.heldIndex = "slow";
        try {
            // the fast shard must answer within its budget even on a loaded machine; the held shard still times out
            TimeValue timeout = TimeValue.timeValueSeconds(2);
            TermsEnumResponse response = run(new TermsEnumRequest("fast", "slow").field("tag").timeout(timeout));
            assertEquals(List.of("a1"), response.getTerms());
            assertFalse(response.isComplete());
            assertEquals(2, response.getTotalShards());
            assertEquals(1, response.getFailedShards());
            assertEquals("slow", response.getShardFailures()[0].index());
            // no nested ReceiveTimeoutTransportException: the coordinator's timer answered, not the transport safety timeout
            assertEquals(
                "OpenSearchTimeoutException[shard did not respond within [" + timeout + "]]",
                response.getShardFailures()[0].reason()
            );
        } finally {
            // the late reply reaches a coordinator that has already answered, and is dropped
            HoldShardRequestsPlugin.releaseAll(this::assertNoShardTasks);
        }
    }

    public void testEveryShardUnansweredIsEmptyAndIncomplete() {
        createKeywordIndex("slow", "z1");
        HoldShardRequestsPlugin.heldIndex = "slow";
        try {
            // not answering is a timeout rather than a failure, so this is not the all-shards-failed error
            TermsEnumResponse response = run(new TermsEnumRequest("slow").field("tag").timeout(TimeValue.timeValueMillis(300)));
            assertEquals(List.of(), response.getTerms());
            assertFalse(response.isComplete());
            assertEquals(1, response.getTotalShards());
            assertEquals(1, response.getFailedShards());
        } finally {
            HoldShardRequestsPlugin.releaseAll(this::assertNoShardTasks);
        }
    }

    private void assertNoShardTasks() {
        try {
            assertBusy(
                () -> assertEquals(
                    List.of(),
                    client().admin().cluster().prepareListTasks().setActions(TransportTermsEnumAction.SHARD_ACTION_NAME).get().getTasks()
                )
            );
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    public void testCrossClusterExpressionIsRejected() {
        createKeywordIndex("idx", "a");
        try {
            assertAcked(
                client().admin()
                    .cluster()
                    .prepareUpdateSettings()
                    .setTransientSettings(Settings.builder().put("cluster.remote.r1.seeds", "127.0.0.1:1"))
            );
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> run(new TermsEnumRequest("r1:idx").field("tag"))
            );
            assertEquals("[terms_enum] does not support cross-cluster requests", e.getMessage());
        } finally {
            assertAcked(
                client().admin()
                    .cluster()
                    .prepareUpdateSettings()
                    .setTransientSettings(Settings.builder().putNull("cluster.remote.r1.seeds"))
            );
        }
    }

    public void testReadBlockIsRejected() {
        createKeywordIndex("blocked", "a");
        try {
            assertAcked(
                client().admin().indices().prepareUpdateSettings("blocked").setSettings(Settings.builder().put("index.blocks.read", true))
            );
            expectThrows(ClusterBlockException.class, () -> run(new TermsEnumRequest("blocked").field("tag")));
        } finally {
            assertAcked(
                client().admin().indices().prepareUpdateSettings("blocked").setSettings(Settings.builder().putNull("index.blocks.read"))
            );
        }
    }

    public void testShardBudgetLeavesReserve() {
        assertEquals(900, TransportTermsEnumAction.shardBudgetMillis(1000));
        assertEquals(90, TransportTermsEnumAction.shardBudgetMillis(100));
        assertEquals(40, TransportTermsEnumAction.shardBudgetMillis(50));
        assertEquals(5, TransportTermsEnumAction.shardBudgetMillis(15));
        assertEquals(1, TransportTermsEnumAction.shardBudgetMillis(5));
        assertEquals(1, TransportTermsEnumAction.shardBudgetMillis(1));
    }

    public void testShouldFailOver() {
        assertTrue(TransportTermsEnumAction.shouldFailOver(new OpenSearchException("boom"), 1, 2, 1_000));
        assertFalse(TransportTermsEnumAction.shouldFailOver(new OpenSearchException("boom"), 1, 2, 0));
        assertFalse(TransportTermsEnumAction.shouldFailOver(new OpenSearchException("boom"), 2, 2, 1_000));
        Exception cancelled = new RemoteTransportException("remote", new TaskCancelledException("cancelled"));
        assertFalse(TransportTermsEnumAction.shouldFailOver(cancelled, 1, 2, 1_000));
    }

    public void testMaxSizeSetting() {
        createKeywordIndex("kw", "a1", "a2");
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> run(new TermsEnumRequest("kw").field("tag").size(1001))
        );
        assertEquals("[size] must be less than or equal to [search.terms_enum.max_size] ([1000]) but was [1001]", e.getMessage());
        assertEquals(List.of("a1", "a2"), run(new TermsEnumRequest("kw").field("tag").size(1000)).getTerms());
        try {
            assertAcked(
                client().admin()
                    .cluster()
                    .prepareUpdateSettings()
                    .setTransientSettings(Settings.builder().put(TransportTermsEnumAction.MAX_SIZE_SETTING.getKey(), 5))
            );
            e = expectThrows(IllegalArgumentException.class, () -> run(new TermsEnumRequest("kw").field("tag").size(6)));
            assertEquals("[size] must be less than or equal to [search.terms_enum.max_size] ([5]) but was [6]", e.getMessage());
            assertEquals(List.of("a1", "a2"), run(new TermsEnumRequest("kw").field("tag").size(5)).getTerms());
        } finally {
            assertAcked(
                client().admin()
                    .cluster()
                    .prepareUpdateSettings()
                    .setTransientSettings(Settings.builder().putNull(TransportTermsEnumAction.MAX_SIZE_SETTING.getKey()))
            );
        }
    }

    public void testFilteredAliasIsRejected() {
        createKeywordIndex("aliased", "x", "y");
        assertAcked(
            client().admin()
                .indices()
                .prepareAliases()
                .addAlias("aliased", "f-alias", QueryBuilders.termQuery("tag", "x"))
                .addAlias("aliased", "p-alias")
        );
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> run(new TermsEnumRequest("f-alias").field("tag")));
        assertThat(e.getMessage(), containsString("filtered alias [f-alias]"));
        assertEquals(List.of("x", "y"), run(new TermsEnumRequest("p-alias").field("tag")).getTerms());
    }

    public void testRoutingAliasTargetsTheSameShardsAsSearch() {
        createIndexWithMappingSource(
            "routed",
            Settings.builder().put("index.number_of_shards", 4).put("index.number_of_replicas", 0).build(),
            "{\"properties\":{\"tag\":{\"type\":\"keyword\"}}}"
        );
        IndexMetadata metadata = client().admin().cluster().prepareState().get().getState().metadata().index("routed");
        int routedShard = OperationRouting.generateShardId(metadata, null, "r00");
        List<String> onRoutedShard = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            String routing = String.format(Locale.ROOT, "r%02d", i);
            String tag = "t-" + routing;
            client().prepareIndex("routed").setId(routing).setRouting(routing).setSource("tag", tag).get();
            if (OperationRouting.generateShardId(metadata, null, routing) == routedShard) {
                onRoutedShard.add(tag);
            }
        }
        client().admin().indices().prepareRefresh("routed").get();
        // routing is deterministic: some documents live on other shards
        assertTrue(onRoutedShard.toString(), onRoutedShard.size() < 20);
        assertAcked(
            client().admin()
                .indices()
                .prepareAliases()
                .addAliasAction(IndicesAliasesRequest.AliasActions.add().index("routed").alias("tenant").searchRouting("r00"))
        );

        TermsEnumResponse response = run(new TermsEnumRequest("tenant").field("tag").size(100));
        assertEquals(1, response.getTotalShards());
        assertEquals(client().prepareSearch("tenant").setSize(0).get().getTotalShards(), response.getTotalShards());
        assertEquals(onRoutedShard, response.getTerms());
        assertTrue(response.isComplete());

        // the index name itself still targets every shard, as search does
        TermsEnumResponse all = run(new TermsEnumRequest("routed", "tenant").field("tag").size(100));
        assertEquals(4, all.getTotalShards());
        assertEquals(20, all.getTerms().size());
    }

    public void testShardCountLimitMatchesSearch() {
        createKeywordIndex("one", "a1");
        createKeywordIndex("two", "b1");
        try {
            assertAcked(
                client().admin()
                    .cluster()
                    .prepareUpdateSettings()
                    .setTransientSettings(Settings.builder().put(TransportSearchAction.SHARD_COUNT_LIMIT_SETTING.getKey(), 1))
            );
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> run(new TermsEnumRequest("one", "two").field("tag"))
            );
            assertThat(e.getMessage(), startsWith("Trying to query 2 shards, which is over the limit of 1."));
            assertThat(e.getMessage(), containsString("[" + TransportSearchAction.SHARD_COUNT_LIMIT_SETTING.getKey() + "]"));
            assertEquals(RestStatus.BAD_REQUEST, ExceptionsHelper.status(e));
            IllegalArgumentException searchError = expectThrows(
                IllegalArgumentException.class,
                () -> client().prepareSearch("one", "two").get()
            );
            assertEquals(searchError.getMessage(), e.getMessage());
            assertEquals(List.of("a1"), run(new TermsEnumRequest("one").field("tag")).getTerms());
        } finally {
            assertAcked(
                client().admin()
                    .cluster()
                    .prepareUpdateSettings()
                    .setTransientSettings(Settings.builder().putNull(TransportSearchAction.SHARD_COUNT_LIMIT_SETTING.getKey()))
            );
        }
    }

    public void testSearchIdleShardSeesPendingRefresh() {
        IndexService indexService = createMapped(
            "idle",
            Settings.builder().put("index.search.idle.after", TimeValue.ZERO).build(),
            "{\"properties\":{\"tag\":{\"type\":\"keyword\"}}}"
        );
        index("idle", "1", "{\"tag\":\"fresh\"}");
        IndexShard shard = indexService.getShard(0);
        assertFalse(shard.scheduledRefresh());
        assertTrue(shard.hasRefreshPending());
        // the shard waits for the next scheduled refresh, so give it more than the default one second budget
        TermsEnumResponse response = run(new TermsEnumRequest("idle").field("tag").timeout(TimeValue.timeValueSeconds(30)));
        assertEquals(List.of("fresh"), response.getTerms());
        assertTrue(response.isComplete());
    }

    private static final DiscoveryNode OLD_NODE = new DiscoveryNode("old", buildNewFakeTransportAddress(), Version.V_3_10_0);
    private static final DiscoveryNode NEW_NODE = new DiscoveryNode("new", buildNewFakeTransportAddress(), Version.CURRENT);

    private static DiscoveryNodes nodes() {
        return DiscoveryNodes.builder().add(OLD_NODE).add(NEW_NODE).build();
    }

    private static ShardRouting copy(ShardId shardId, String nodeId, boolean primary) {
        return TestShardRouting.newShardRouting(shardId, nodeId, primary, ShardRoutingState.STARTED);
    }

    private static GroupShardsIterator<ShardIterator> shards(ShardIterator... iterators) {
        return new GroupShardsIterator<>(List.of(iterators));
    }

    public void testPlanShardsSkipsOldNodes() {
        ShardId shardId = new ShardId("idx", "uuid", 0);
        ShardRouting onNew = copy(shardId, "new", false);
        List<TransportTermsEnumAction.ShardPlan> plans = TransportTermsEnumAction.planShards(
            shards(new PlainShardIterator(shardId, List.of(copy(shardId, "old", true), onNew))),
            nodes()
        );
        assertEquals(1, plans.size());
        assertEquals(shardId, plans.get(0).shardId());
        assertEquals(List.of(onNew), plans.get(0).copies());
        assertNull(plans.get(0).failure());
    }

    public void testPlanShardsFailsShardWithOnlyOldCopies() {
        ShardId oldOnly = new ShardId("idx", "uuid", 0);
        ShardId mixed = new ShardId("idx", "uuid", 1);
        ShardRouting onNew = copy(mixed, "new", true);
        List<TransportTermsEnumAction.ShardPlan> plans = TransportTermsEnumAction.planShards(
            shards(new PlainShardIterator(oldOnly, List.of(copy(oldOnly, "old", true))), new PlainShardIterator(mixed, List.of(onNew))),
            nodes()
        );
        assertEquals(2, plans.size());
        assertEquals(List.of(), plans.get(0).copies());
        assertThat(plans.get(0).failure(), instanceOf(IllegalStateException.class));
        assertThat(plans.get(0).failure().getMessage(), containsString("3.11.0"));
        assertEquals(List.of(onNew), plans.get(1).copies());
        assertNull(plans.get(1).failure());
    }

    public void testPlanShardsRejectsWhenEveryShardIsOnOldNodes() {
        ShardId first = new ShardId("idx", "uuid", 0);
        ShardId second = new ShardId("idx", "uuid", 1);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> TransportTermsEnumAction.planShards(
                shards(
                    new PlainShardIterator(first, List.of(copy(first, "old", true))),
                    new PlainShardIterator(second, List.of(copy(second, "old", true)))
                ),
                nodes()
            )
        );
        assertEquals("[terms_enum] requires nodes on version 3.11.0 or later, but no copy of any target shard is on one", e.getMessage());
    }

    public void testPlanShardsReportsShardWithoutCopies() {
        ShardId shardId = new ShardId("idx", "uuid", 0);
        List<TransportTermsEnumAction.ShardPlan> plans = TransportTermsEnumAction.planShards(
            shards(new PlainShardIterator(shardId, List.of())),
            nodes()
        );
        assertEquals(1, plans.size());
        assertEquals(List.of(), plans.get(0).copies());
        assertThat(plans.get(0).failure(), instanceOf(NoShardAvailableActionException.class));
    }

    public void testPlanShardsEmptyInputReturnsEmptyPlan() {
        assertEquals(List.of(), TransportTermsEnumAction.planShards(shards(), nodes()));
    }
}
