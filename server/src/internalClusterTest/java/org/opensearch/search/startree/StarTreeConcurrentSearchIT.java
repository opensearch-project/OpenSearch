/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.startree;

import org.opensearch.action.admin.indices.segments.IndexShardSegments;
import org.opensearch.action.admin.indices.segments.ShardSegments;
import org.opensearch.action.bulk.BulkRequestBuilder;
import org.opensearch.action.search.SearchRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.MergePolicyProvider;
import org.opensearch.index.engine.Segment;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.search.stats.SearchStats;
import org.opensearch.plugins.Plugin;
import org.opensearch.search.SearchService;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.opensearch.search.aggregations.metrics.Sum;
import org.opensearch.search.aggregations.metrics.ValueCount;
import org.opensearch.search.profile.ProfileResult;
import org.opensearch.search.profile.ProfileShardResult;
import org.opensearch.test.InternalSettingsPlugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertNoFailures;

/**
 * Star-tree aggregations must return the same results with and without concurrent segment search.
 * <p>
 * Two failure modes are covered:
 * <ul>
 *     <li>Filtered queries on a multi-segment index, where slices run concurrently on different segments.
 *     Every segment uses a different keyword dictionary, so a dimension value has a different ordinal per segment.</li>
 *     <li>Unfiltered metric aggregations on a single segment that intra-segment search splits into partitions.</li>
 * </ul>
 * Expected values are computed from the generated documents, not from another search. Index search stats confirm
 * that the star-tree answered every request, and that concurrent search ran with several slices where expected.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 1, numClientNodes = 0)
public class StarTreeConcurrentSearchIT extends OpenSearchIntegTestCase {

    private static final long T0 = Instant.parse("2024-01-01T00:00:00Z").toEpochMilli();
    private static final long HOUR = 3_600_000L;
    private static final int MAX_SLICES = 4;
    private static final List<String> MODES = List.of("none", "auto", "all");

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        // Registers index.merge.enabled, which keeps the multi-segment index's segments as indexed
        return List.of(InternalSettingsPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(SearchService.CONCURRENT_SEGMENT_SEARCH_MAX_SLICE_COUNT_KEY, MAX_SLICES)
            .put(SearchService.CONCURRENT_SEGMENT_SEARCH_PARTITION_STRATEGY.getKey(), "balanced")
            .put(SearchService.CONCURRENT_SEGMENT_SEARCH_PARTITION_MIN_SEGMENT_SIZE.getKey(), 1000)
            .build();
    }

    /** A generated document. */
    private record Doc(long timestamp, String d1, String d2, long m1) {
    }

    /**
     * Document {@code j} of segment {@code segment}. Each segment has {@code segment + 2} filler values that sort
     * before "A" and "B", so "A" and "B" get a different ordinal in every segment; even segments also have "C", which
     * sorts after them, so "B" is the last term of only some segments.
     */
    private static Doc doc(int segment, int j) {
        String d1 = switch (j % 4) {
            case 0 -> "A";
            case 1 -> "B";
            case 2 -> segment % 2 == 0 ? "C" : "0f" + (j / 4) % (segment + 2);
            default -> "0f" + (j / 4) % (segment + 2);
        };
        return new Doc(T0 + (j % 48) * HOUR, d1, j % 2 == 0 ? "X" : "Y", (segment + 1) * 10L + j % 7);
    }

    /** A query, its expected result computed from the documents, and how to read the actual result. */
    private record Probe(String name, QueryBuilder query, List<AggregationBuilder> aggs, String expected, Function<
        Aggregations,
        String> actual) {
    }

    private static Probe metricsProbe(String name, QueryBuilder query, Predicate<Doc> filter, List<Doc> docs) {
        long count = 0;
        long sum = 0;
        for (Doc doc : docs) {
            if (filter.test(doc)) {
                count++;
                sum += doc.m1();
            }
        }
        return new Probe(
            name,
            query,
            List.of(AggregationBuilders.sum("total").field("m1"), AggregationBuilders.count("count").field("m1")),
            "count=" + count + " sum=" + sum,
            aggs -> "count=" + ((ValueCount) aggs.get("count")).getValue() + " sum=" + (long) ((Sum) aggs.get("total")).getValue()
        );
    }

    private static Probe bucketsProbe(
        String name,
        QueryBuilder query,
        Predicate<Doc> filter,
        List<Doc> docs,
        AggregationBuilder bucketAgg,
        Function<Doc, String> expectedKey,
        Function<MultiBucketsAggregation.Bucket, String> actualKey
    ) {
        Map<String, long[]> expected = new TreeMap<>();
        for (Doc doc : docs) {
            if (filter.test(doc)) {
                long[] countAndSum = expected.computeIfAbsent(expectedKey.apply(doc), k -> new long[2]);
                countAndSum[0]++;
                countAndSum[1] += doc.m1();
            }
        }
        StringJoiner joiner = new StringJoiner(" ");
        expected.forEach((key, countAndSum) -> joiner.add(key + "=" + countAndSum[0] + "/" + countAndSum[1]));
        return new Probe(
            name,
            query,
            List.of(bucketAgg.subAggregation(AggregationBuilders.sum("total").field("m1"))),
            joiner.toString(),
            aggs -> {
                Map<String, String> actual = new TreeMap<>();
                for (MultiBucketsAggregation.Bucket bucket : ((MultiBucketsAggregation) aggs.get("by")).getBuckets()) {
                    if (bucket.getDocCount() > 0) {
                        long sum = (long) ((Sum) bucket.getAggregations().get("total")).getValue();
                        actual.put(actualKey.apply(bucket), bucket.getDocCount() + "/" + sum);
                    }
                }
                StringJoiner actualJoiner = new StringJoiner(" ");
                actual.forEach((key, value) -> actualJoiner.add(key + "=" + value));
                return actualJoiner.toString();
            }
        );
    }

    private static Function<MultiBucketsAggregation.Bucket, String> epochMillisKey() {
        return bucket -> String.valueOf(((ZonedDateTime) bucket.getKey()).toInstant().toEpochMilli());
    }

    /** With the default max_leaf_docs, filters are matched entry by entry below leaf nodes. */
    public void testFilteredAggregationsAcrossConcurrentSlices() throws Exception {
        assertFilteredAggregationsAcrossConcurrentSlices("star_tree_multi_segment", null);
    }

    /** With every node split, filters are matched against star-tree nodes, with each segment's ordinals. */
    public void testFilteredAggregationsAcrossConcurrentSlicesOnSplitNodes() throws Exception {
        assertFilteredAggregationsAcrossConcurrentSlices("star_tree_multi_segment_split", 1);
    }

    private void assertFilteredAggregationsAcrossConcurrentSlices(String index, Integer maxLeafDocs) throws Exception {
        // No background merges: the segments, each with its own keyword dictionary, stay as indexed
        createStarTreeIndex(index, false, maxLeafDocs);
        int segments = 8;
        List<Doc> docs = new ArrayList<>();
        for (int segment = 0; segment < segments; segment++) {
            List<Doc> batch = new ArrayList<>();
            for (int j = 0; j < 400; j++) {
                batch.add(doc(segment, j));
            }
            indexAndRefresh(index, batch);
            docs.addAll(batch);
        }
        assertEquals(segments, searchableSegments(index));

        long windowStart = T0 + 6 * HOUR;
        long windowEnd = T0 + 30 * HOUR;
        List<Probe> probes = List.of(
            metricsProbe("term_metrics", QueryBuilders.termQuery("d1", "A"), d -> d.d1().equals("A"), docs),
            bucketsProbe(
                "term_terms",
                QueryBuilders.termQuery("d1", "A"),
                d -> d.d1().equals("A"),
                docs,
                AggregationBuilders.terms("by").field("d2"),
                Doc::d2,
                MultiBucketsAggregation.Bucket::getKeyAsString
            ),
            bucketsProbe(
                "term_date_histogram",
                QueryBuilders.termQuery("d1", "B"),
                d -> d.d1().equals("B"),
                docs,
                AggregationBuilders.dateHistogram("by").field("@timestamp").calendarInterval(DateHistogramInterval.HOUR),
                d -> String.valueOf(d.timestamp()),
                epochMillisKey()
            ),
            bucketsProbe(
                "range_and_term_terms",
                QueryBuilders.boolQuery()
                    .filter(QueryBuilders.rangeQuery("@timestamp").gte(windowStart).lt(windowEnd).format("epoch_millis"))
                    .filter(QueryBuilders.termQuery("d1", "B")),
                d -> d.d1().equals("B") && d.timestamp() >= windowStart && d.timestamp() < windowEnd,
                docs,
                AggregationBuilders.terms("by").field("d2"),
                Doc::d2,
                MultiBucketsAggregation.Bucket::getKeyAsString
            ),
            bucketsProbe(
                "keyword_range_terms",
                QueryBuilders.rangeQuery("d1").gte("A").lte("B"),
                d -> d.d1().compareTo("A") >= 0 && d.d1().compareTo("B") <= 0,
                docs,
                AggregationBuilders.terms("by").field("d2"),
                Doc::d2,
                MultiBucketsAggregation.Bucket::getKeyAsString
            ),
            metricsProbe("terms_metrics", QueryBuilders.termsQuery("d1", "A", "B"), d -> d.d1().equals("A") || d.d1().equals("B"), docs),
            // "C" is only in even segments: the range is skipped in odd ones, and the terms bind one ordinal there, two elsewhere
            metricsProbe("range_after_last_term_metrics", QueryBuilders.rangeQuery("d1").gt("B"), d -> d.d1().compareTo("B") > 0, docs),
            metricsProbe(
                "terms_in_some_segments_metrics",
                QueryBuilders.termsQuery("d1", "A", "C"),
                d -> d.d1().equals("A") || d.d1().equals("C"),
                docs
            ),
            metricsProbe("unfiltered_metrics", null, d -> true, docs)
        );
        assertProbes(index, probes, 30, true);
    }

    /**
     * One segment large enough for intra-segment search to split it: requests the star-tree answers must not be
     * partitioned, while a request it cannot answer still is.
     */
    public void testUnfilteredMetricsOnAPartitionableSegment() throws Exception {
        String index = "star_tree_single_segment";
        createStarTreeIndex(index, true, null);
        List<Doc> docs = new ArrayList<>();
        for (int j = 0; j < 6000; j++) {
            docs.add(doc(3, j));
        }
        indexAndRefresh(index, docs);
        assertNoFailures(client().admin().indices().prepareForceMerge(index).setMaxNumSegments(1).get());
        refresh(index);
        assertEquals(1, searchableSegments(index));

        assertProbes(index, List.of(metricsProbe("unfiltered_metrics", null, d -> true, docs)), 5, false);

        // A control: "max" is not a stored statistic of m1, so the regular path answers it, in several partitions
        for (String mode : List.of("auto", "all")) {
            client().admin()
                .indices()
                .prepareUpdateSettings(index)
                .setSettings(Settings.builder().put(IndexSettings.INDEX_CONCURRENT_SEGMENT_SEARCH_MODE.getKey(), mode))
                .get();
            SearchStats.Stats before = searchStats(index);
            client().prepareSearch(index)
                .setSize(0)
                .setRequestCache(false)
                .addAggregation(AggregationBuilders.max("max").field("m1"))
                .get();
            SearchStats.Stats after = searchStats(index);
            assertEquals(
                "[" + mode + "] the star-tree must not answer the control",
                before.getStarTreeQueryCount(),
                after.getStarTreeQueryCount()
            );
            double slices = after.getConcurrentAvgSliceCount() * after.getConcurrentQueryCount() - before.getConcurrentAvgSliceCount()
                * before.getConcurrentQueryCount();
            assertTrue("[" + mode + "] the control must be partitioned, but ran in " + slices + " slices", slices >= 2);
        }
    }

    /**
     * Runs every probe {@code iterations} times in each concurrent search mode (three times in {@code none}, which is
     * deterministic) and fails with all wrong answers. It also
     * fails when the star-tree did not answer every request, when concurrent search did not run in {@code auto} and
     * {@code all}, or, with {@code severalSlices}, when it ran with one slice: the correct answer would then prove nothing.
     */
    /** The profiled aggregations, at any depth, that collected documents one at a time instead of pre-computing. */
    private static List<String> collectingAggregations(SearchResponse response) {
        List<String> collecting = new ArrayList<>();
        for (ProfileShardResult shard : response.getProfileResults().values()) {
            Deque<ProfileResult> pending = new ArrayDeque<>(shard.getAggregationProfileResults().getProfileResults());
            while (pending.isEmpty() == false) {
                ProfileResult result = pending.pop();
                long collected = result.getTimeBreakdown().getOrDefault("collect_count", 0L);
                if (collected > 0) {
                    collecting.add(result.getLuceneDescription() + "=" + collected);
                }
                pending.addAll(result.getProfiledChildren());
            }
        }
        return collecting;
    }

    private void assertProbes(String index, List<Probe> probes, int iterations, boolean severalSlices) {
        List<String> failures = new ArrayList<>();
        List<String> notExercised = new ArrayList<>();
        Map<String, int[]> wrongAndTotal = new TreeMap<>();
        int checks = 0;
        for (String mode : MODES) {
            client().admin()
                .indices()
                .prepareUpdateSettings(index)
                .setSettings(Settings.builder().put(IndexSettings.INDEX_CONCURRENT_SEGMENT_SEARCH_MODE.getKey(), mode))
                .get();
            SearchStats.Stats before = searchStats(index);
            // Sequential search is deterministic: a few runs suffice
            int runs = mode.equals("none") ? Math.min(iterations, 3) : iterations;
            for (int i = 0; i < runs; i++) {
                for (Probe probe : probes) {
                    SearchRequestBuilder request = client().prepareSearch(index).setSize(0).setRequestCache(false).setTrackTotalHits(false);
                    if (probe.query() != null) {
                        request.setQuery(probe.query());
                    }
                    probe.aggs().forEach(request::addAggregation);
                    String actual;
                    try {
                        actual = probe.actual().apply(request.get().getAggregations());
                    } catch (Exception e) {
                        // A race on shared filter state can also fail the request (a corrupted TreeSet)
                        actual = "failed: " + e.getMessage();
                    }
                    checks++;
                    int[] counts = wrongAndTotal.computeIfAbsent(mode + "/" + probe.name(), k -> new int[2]);
                    counts[1]++;
                    if (probe.expected().equals(actual) == false) {
                        counts[0]++;
                        failures.add("[" + mode + "] " + probe.name() + ": expected [" + probe.expected() + "] but was [" + actual + "]");
                    }
                }
            }
            // The star-tree must pre-compute each probe, not only be set up for it: a pre-computed aggregation collects no
            // document. (Profiled requests are not counted below: they come after the stats are read.)
            SearchStats.Stats after = searchStats(index);
            for (Probe probe : probes) {
                SearchRequestBuilder request = client().prepareSearch(index)
                    .setSize(0)
                    .setRequestCache(false)
                    .setTrackTotalHits(false)
                    .setProfile(true);
                if (probe.query() != null) {
                    request.setQuery(probe.query());
                }
                probe.aggs().forEach(request::addAggregation);
                List<String> collected = collectingAggregations(request.get());
                if (collected.isEmpty() == false) {
                    notExercised.add("[" + mode + "] " + probe.name() + ": " + collected + " collected documents");
                }
            }
            long requests = (long) runs * probes.size();
            long starTree = after.getStarTreeQueryCount() - before.getStarTreeQueryCount();
            long concurrent = after.getConcurrentQueryCount() - before.getConcurrentQueryCount();
            if (starTree != requests) {
                notExercised.add("[" + mode + "] the star-tree answered " + starTree + " of " + requests + " requests");
            }
            if (concurrent != (mode.equals("none") ? 0 : requests)) {
                notExercised.add("[" + mode + "] " + concurrent + " of " + requests + " requests used concurrent search");
            }
            if (severalSlices && concurrent > 0) {
                double slices = after.getConcurrentAvgSliceCount() * after.getConcurrentQueryCount() - before.getConcurrentAvgSliceCount()
                    * before.getConcurrentQueryCount();
                if (slices / concurrent < 2) {
                    notExercised.add("[" + mode + "] " + (slices / concurrent) + " slices per concurrent request");
                }
            }
        }
        StringJoiner breakdown = new StringJoiner(", ");
        wrongAndTotal.forEach((key, counts) -> breakdown.add(key + " " + counts[0] + "/" + counts[1]));
        assertTrue(
            "wrong answers: "
                + failures.size()
                + " of "
                + checks
                + " ("
                + breakdown
                + "); first: "
                + failures.subList(0, Math.min(failures.size(), 4))
                + "; not exercised: "
                + notExercised,
            failures.isEmpty() && notExercised.isEmpty()
        );
    }

    private SearchStats.Stats searchStats(String index) {
        return client().admin().indices().prepareStats(index).setSearch(true).get().getTotal().getSearch().getTotal();
    }

    private void createStarTreeIndex(String index, boolean mergesEnabled, Integer maxLeafDocs) throws IOException {
        createIndex(
            index,
            Settings.builder()
                .put(MergePolicyProvider.INDEX_MERGE_ENABLED, mergesEnabled)
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put("index.composite_index", true)
                .put("index.append_only.enabled", true)
                .put("index.refresh_interval", "-1")
                .put("index.requests.cache.enable", false)
                .put("index.search.star_tree_index.enabled", true)
                .put("index.codec", "default")
                .build(),
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("composite")
                .startObject("st")
                .field("type", "star_tree")
                .startObject("config")
                .field("max_leaf_docs", maxLeafDocs == null ? 10000 : maxLeafDocs)
                .startArray("ordered_dimensions")
                .startObject()
                .field("name", "@timestamp")
                .field("calendar_intervals", new String[] { "hour", "day" })
                .endObject()
                .startObject()
                .field("name", "d1")
                .endObject()
                .startObject()
                .field("name", "d2")
                .endObject()
                .endArray()
                .startArray("metrics")
                .startObject()
                .field("name", "m1")
                .field("stats", new String[] { "sum", "value_count" })
                .endObject()
                .endArray()
                .endObject()
                .endObject()
                .endObject()
                .startObject("properties")
                .startObject("@timestamp")
                .field("type", "date")
                .endObject()
                .startObject("d1")
                .field("type", "keyword")
                .endObject()
                .startObject("d2")
                .field("type", "keyword")
                .endObject()
                .startObject("m1")
                .field("type", "long")
                .endObject()
                .endObject()
                .endObject()
                .toString()
        );
        ensureGreen(index);
    }

    private void indexAndRefresh(String index, List<Doc> docs) {
        BulkRequestBuilder bulk = client().prepareBulk(index);
        for (Doc doc : docs) {
            bulk.add(client().prepareIndex(index).setSource("@timestamp", doc.timestamp(), "d1", doc.d1(), "d2", doc.d2(), "m1", doc.m1()));
        }
        assertNoFailures(bulk.get());
        refresh(index);
    }

    private int searchableSegments(String index) {
        int count = 0;
        for (IndexShardSegments shard : client().admin().indices().prepareSegments(index).get().getIndices().get(index)) {
            for (ShardSegments copy : shard) {
                for (Segment segment : copy) {
                    if (segment.isSearch()) {
                        count++;
                    }
                }
            }
        }
        return count;
    }
}
