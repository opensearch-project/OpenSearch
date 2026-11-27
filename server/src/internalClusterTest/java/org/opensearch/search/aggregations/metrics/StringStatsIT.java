/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.metrics;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.opensearch.ExceptionsHelper;
import org.opensearch.action.admin.cluster.shards.ClusterSearchShardsGroup;
import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.plugins.Plugin;
import org.opensearch.script.MockScriptPlugin;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptType;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.bucket.histogram.Histogram;
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.opensearch.search.aggregations.pipeline.BucketMetricValue;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.ParameterizedStaticSettingsOpenSearchIntegTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.opensearch.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_MODE;
import static org.opensearch.search.SearchService.CONCURRENT_SEGMENT_SEARCH_MODE_ALL;
import static org.opensearch.search.SearchService.CONCURRENT_SEGMENT_SEARCH_MODE_NONE;
import static org.opensearch.search.aggregations.AggregationBuilders.histogram;
import static org.opensearch.search.aggregations.AggregationBuilders.stringStats;
import static org.opensearch.search.aggregations.AggregationBuilders.terms;
import static org.opensearch.search.aggregations.PipelineAggregatorBuilders.bucketSelector;
import static org.opensearch.search.aggregations.PipelineAggregatorBuilders.maxBucket;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertFailures;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Runs the string_stats aggregation on indices with several shards, with and without concurrent segment search, so that the characters
 * counted by every shard, and by every slice of a shard, have to be merged into one result. The entropy of the merged result has to
 * be that of all the values together, which no combination of the entropies of the shards gives.
 */
@OpenSearchIntegTestCase.SuiteScopeTestCase
public class StringStatsIT extends ParameterizedStaticSettingsOpenSearchIntegTestCase {

    private static final String INDEX = "idx";
    private static final String INDEX_WITHOUT_THE_FIELD = "idx_without_domain";
    private static final String ENTROPY_ABOVE_TWO = "params.e > 2";

    private static final String LOW = "low-host";
    private static final String MID = "mid-host";
    private static final String HIGH = "high-host";

    // the domains of each host, which have little entropy for low, a single bit for mid, and many bits for high
    private static final Map<String, List<String>> DOMAINS = new TreeMap<>();
    private static final Map<String, List<List<String>>> TAGS = new TreeMap<>();

    public StringStatsIT(Settings staticSettings) {
        super(staticSettings);
    }

    @ParametersFactory
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
            new Object[] {
                Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_MODE.getKey(), CONCURRENT_SEGMENT_SEARCH_MODE_ALL).build() },
            new Object[] {
                Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_MODE.getKey(), CONCURRENT_SEGMENT_SEARCH_MODE_NONE).build() }
        );
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(EntropyScriptPlugin.class);
    }

    /**
     * Painless is not available to the tests of the server, so the script of the bucket selector is a mock script.
     */
    public static class EntropyScriptPlugin extends MockScriptPlugin {

        @Override
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            return Collections.singletonMap(ENTROPY_ABOVE_TWO, vars -> ((Number) vars.get("e")).doubleValue() > 2);
        }
    }

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        assertAcked(
            prepareCreate(INDEX).setSettings(
                Settings.builder().put(SETTING_NUMBER_OF_SHARDS, randomIntBetween(2, 5)).put(SETTING_NUMBER_OF_REPLICAS, 0)
            ).setMapping("host", "type=keyword", "domain", "type=keyword", "tags", "type=keyword", "number", "type=long")
        );
        assertAcked(
            prepareCreate(INDEX_WITHOUT_THE_FIELD).setSettings(
                Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 2).put(SETTING_NUMBER_OF_REPLICAS, 0)
            ).setMapping("other", "type=keyword")
        );

        final List<String> low = new ArrayList<>();
        final List<String> mid = new ArrayList<>();
        final List<String> high = new ArrayList<>();
        for (int i = 0; i < 30; i++) {
            low.add("a".repeat(randomIntBetween(3, 12)));
            // as many as a as b, so that the two are equally frequent
            mid.add("ab".repeat(randomIntBetween(2, 6)));
            high.add(randomAlphaOfLengthBetween(10, 20) + randomRealisticUnicodeOfCodepointLengthBetween(1, 4));
        }
        DOMAINS.put(LOW, low);
        DOMAINS.put(MID, mid);
        DOMAINS.put(HIGH, high);

        // Every host is indexed and refreshed on its own, so that the shards have several segments for concurrent search to slice.
        for (Map.Entry<String, List<String>> entry : DOMAINS.entrySet()) {
            final List<List<String>> tagsOfHost = new ArrayList<>();
            final List<IndexRequestBuilder> builders = new ArrayList<>();
            for (String domain : entry.getValue()) {
                // one to three tags, and a document without any tags
                final List<String> tags = new ArrayList<>();
                final int numberOfTags = randomIntBetween(0, 3);
                for (int t = 0; t < numberOfTags; t++) {
                    final String tag = randomFrom("alpha", "beta", "gamma-ray", "δ", "😀x");
                    if (tags.contains(tag) == false) {
                        // the doc values of a document hold a value once
                        tags.add(tag);
                    }
                }
                tagsOfHost.add(tags);
                builders.add(
                    client().prepareIndex(INDEX).setSource("host", entry.getKey(), "domain", domain, "tags", tags, "number", tags.size())
                );
            }
            TAGS.put(entry.getKey(), tagsOfHost);
            indexRandom(true, builders);
        }
        indexRandom(
            true,
            client().prepareIndex(INDEX_WITHOUT_THE_FIELD).setSource("other", "abc"),
            client().prepareIndex(INDEX_WITHOUT_THE_FIELD).setSource("other", "de")
        );
        ensureSearchable(INDEX, INDEX_WITHOUT_THE_FIELD);
    }

    /**
     * What the statistics of values are, worked out the slow and obvious way.
     */
    private static final class Expected {
        final long count;
        final int min;
        final int max;
        final double avg;
        final double entropy;
        final Map<String, Double> distribution = new TreeMap<>();

        Expected(Collection<String> values) {
            final Map<Integer, Long> frequencies = new TreeMap<>();
            int shortest = Integer.MAX_VALUE;
            int longest = 0;
            long characters = 0;
            for (String value : values) {
                final int length = value.codePointCount(0, value.length());
                shortest = Math.min(shortest, length);
                longest = Math.max(longest, length);
                characters += length;
                value.codePoints().forEach(codePoint -> frequencies.merge(codePoint, 1L, Long::sum));
            }
            this.count = values.size();
            this.min = shortest;
            this.max = longest;
            this.avg = (double) characters / values.size();
            double sum = 0;
            for (Map.Entry<Integer, Long> entry : frequencies.entrySet()) {
                final double probability = (double) entry.getValue() / characters;
                sum -= probability * Math.log(probability) / Math.log(2);
                distribution.put(new String(Character.toChars(entry.getKey())), probability);
            }
            this.entropy = sum;
        }

        void assertMatches(StringStats actual) {
            assertThat(actual.getCount(), equalTo(count));
            assertThat(actual.getMinLength(), equalTo((double) min));
            assertThat(actual.getMaxLength(), equalTo((double) max));
            assertEquals(avg, actual.getAvgLength(), 1e-9);
            assertEquals(entropy, actual.getEntropy(), 1e-9);
        }
    }

    private static List<String> allDomains() {
        final List<String> all = new ArrayList<>();
        DOMAINS.values().forEach(all::addAll);
        return all;
    }

    public void testAcrossShards() {
        SearchResponse response = client().prepareSearch(INDEX).addAggregation(stringStats("shape").field("domain")).get();

        assertSearchResponse(response);
        assertThat(response.getTotalShards(), greaterThan(1));
        StringStats shape = response.getAggregations().get("shape");
        assertThat(shape, notNullValue());
        assertThat(shape.getName(), equalTo("shape"));
        new Expected(allDomains()).assertMatches(shape);
        assertTrue(shape.getDistribution().isEmpty());
    }

    public void testDistributionAcrossShards() {
        SearchResponse response = client().prepareSearch(INDEX)
            .addAggregation(stringStats("shape").field("domain").showDistribution(true))
            .get();

        assertSearchResponse(response);
        StringStats shape = response.getAggregations().get("shape");
        final Expected expected = new Expected(allDomains());
        assertThat(shape.getDistribution().size(), equalTo(expected.distribution.size()));
        double total = 0;
        for (Map.Entry<String, Double> entry : expected.distribution.entrySet()) {
            assertEquals("frequency of [" + entry.getKey() + "]", entry.getValue(), shape.getDistribution().get(entry.getKey()), 1e-12);
            total += shape.getDistribution().get(entry.getKey());
        }
        assertEquals(1.0, total, 1e-9);
        // the most frequent character comes first
        double previous = Double.POSITIVE_INFINITY;
        for (double frequency : shape.getDistribution().values()) {
            assertTrue(frequency <= previous);
            previous = frequency;
        }
    }

    public void testMultiValuedField() {
        SearchResponse response = client().prepareSearch(INDEX).addAggregation(stringStats("shape").field("tags")).get();

        assertSearchResponse(response);
        StringStats shape = response.getAggregations().get("shape");
        final List<String> allTags = new ArrayList<>();
        TAGS.values().forEach(perHost -> perHost.forEach(allTags::addAll));
        // every tag of a document counts, and a document without tags counts for nothing
        new Expected(allTags).assertMatches(shape);
    }

    public void testMissing() {
        SearchResponse response = client().prepareSearch(INDEX).addAggregation(stringStats("shape").field("tags").missing("none")).get();

        assertSearchResponse(response);
        StringStats shape = response.getAggregations().get("shape");
        final List<String> values = new ArrayList<>();
        for (List<List<String>> perHost : TAGS.values()) {
            for (List<String> tags : perHost) {
                if (tags.isEmpty()) {
                    values.add("none");
                } else {
                    values.addAll(tags);
                }
            }
        }
        new Expected(values).assertMatches(shape);
    }

    public void testIndexWhereTheFieldIsNotMapped() {
        // two shards of the other index have no such field at all, and have to leave the result of the others as it is
        SearchResponse response = client().prepareSearch(INDEX, INDEX_WITHOUT_THE_FIELD)
            .addAggregation(stringStats("shape").field("domain"))
            .get();

        assertSearchResponse(response);
        StringStats shape = response.getAggregations().get("shape");
        new Expected(allDomains()).assertMatches(shape);
    }

    public void testOnlyIndexWhereTheFieldIsNotMapped() {
        SearchResponse response = client().prepareSearch(INDEX_WITHOUT_THE_FIELD)
            .addAggregation(stringStats("shape").field("domain").showDistribution(true))
            .get();

        assertSearchResponse(response);
        StringStats shape = response.getAggregations().get("shape");
        assertThat(shape.getCount(), equalTo(0L));
        assertTrue(Double.isNaN(shape.getEntropy()));
        assertTrue(shape.getDistribution().isEmpty());
    }

    public void testOrderByEntropy() {
        for (boolean descending : new boolean[] { true, false }) {
            SearchResponse response = client().prepareSearch(INDEX)
                .addAggregation(
                    terms("by_host").field("host")
                        .order(BucketOrder.aggregation("shape.entropy", descending == false))
                        .subAggregation(stringStats("shape").field("domain"))
                )
                .get();

            assertSearchResponse(response);
            Terms byHost = response.getAggregations().get("by_host");
            assertThat(byHost.getBuckets().size(), equalTo(3));
            final List<String> keys = new ArrayList<>();
            for (Terms.Bucket bucket : byHost.getBuckets()) {
                keys.add(bucket.getKeyAsString());
                StringStats shape = bucket.getAggregations().get("shape");
                new Expected(DOMAINS.get(bucket.getKeyAsString())).assertMatches(shape);
            }
            // the entropy is zero for low, one bit for mid, and well above that for high
            assertThat(keys, equalTo(descending ? List.of(HIGH, MID, LOW) : List.of(LOW, MID, HIGH)));
        }
    }

    public void testOrderByAverageLengthAndByCount() {
        for (String statistic : List.of("avg_length", "min_length", "max_length", "count")) {
            SearchResponse response = client().prepareSearch(INDEX)
                .addAggregation(
                    terms("by_host").field("host")
                        .order(BucketOrder.aggregation("shape." + statistic, false))
                        .subAggregation(stringStats("shape").field("domain"))
                )
                .get();

            assertSearchResponse(response);
            Terms byHost = response.getAggregations().get("by_host");
            double previous = Double.POSITIVE_INFINITY;
            for (Terms.Bucket bucket : byHost.getBuckets()) {
                StringStats shape = bucket.getAggregations().get("shape");
                final double value = valueOf(shape, statistic);
                assertTrue(statistic + " is not in descending order", value <= previous);
                previous = value;
            }
        }
    }

    public void testSiblingPipelineOnEntropy() {
        SearchResponse response = client().prepareSearch(INDEX)
            .addAggregation(terms("by_host").field("host").subAggregation(stringStats("domain_shape").field("domain")))
            .addAggregation(maxBucket("most_random", "by_host>domain_shape.entropy"))
            .get();

        assertSearchResponse(response);
        BucketMetricValue mostRandom = response.getAggregations().get("most_random");
        assertThat(mostRandom, notNullValue());
        assertThat(mostRandom.keys(), equalTo(new String[] { HIGH }));
        assertEquals(new Expected(DOMAINS.get(HIGH)).entropy, mostRandom.value(), 1e-9);
    }

    public void testBucketSelectorOnEntropy() {
        Script script = new Script(ScriptType.INLINE, EntropyScriptPlugin.NAME, ENTROPY_ABOVE_TWO, Collections.emptyMap());

        SearchResponse response = client().prepareSearch(INDEX)
            .addAggregation(
                terms("by_host").field("host")
                    .subAggregation(stringStats("shape").field("domain"))
                    .subAggregation(bucketSelector("random", Collections.singletonMap("e", "shape.entropy"), script))
            )
            .get();

        assertSearchResponse(response);
        Terms byHost = response.getAggregations().get("by_host");
        assertThat(byHost.getBuckets().size(), equalTo(1));
        assertThat(byHost.getBuckets().get(0).getKeyAsString(), equalTo(HIGH));
    }

    public void testEntropyOfAllShardsIsNotTheAverageOfTheEntropyOfEachShard() throws Exception {
        final String index = "two_shards";
        assertAcked(
            prepareCreate(index).setSettings(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 2).put(SETTING_NUMBER_OF_REPLICAS, 0))
                .setMapping("word", "type=keyword")
        );
        try {
            // find a routing for each of the two shards, so that the values of a shard are all of one kind
            final String[] routing = new String[2];
            for (int i = 0; i < 1000 && (routing[0] == null || routing[1] == null); i++) {
                final String candidate = "route-" + i;
                final ClusterSearchShardsGroup[] groups = client().admin()
                    .cluster()
                    .prepareSearchShards(index)
                    .setRouting(candidate)
                    .get()
                    .getGroups();
                assertThat(groups.length, equalTo(1));
                final int shard = groups[0].getShardId().id();
                if (routing[shard] == null) {
                    routing[shard] = candidate;
                }
            }
            assertNotNull(routing[0]);
            assertNotNull(routing[1]);
            final List<IndexRequestBuilder> builders = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                // all that the first shard sees is a, and all that the second shard sees is b
                builders.add(client().prepareIndex(index).setRouting(routing[0]).setSource("word", "aaaa"));
                builders.add(client().prepareIndex(index).setRouting(routing[1]).setSource("word", "bbbb"));
            }
            indexRandom(true, builders);
            ensureSearchable(index);

            for (int shard = 0; shard < 2; shard++) {
                SearchResponse single = client().prepareSearch(index)
                    .setPreference("_shards:" + shard)
                    .addAggregation(stringStats("shape").field("word"))
                    .get();
                assertSearchResponse(single);
                StringStats ofOneShard = single.getAggregations().get("shape");
                assertThat(ofOneShard.getCount(), equalTo(10L));
                assertThat(ofOneShard.getEntropy(), equalTo(0.0));
            }

            SearchResponse response = client().prepareSearch(index)
                .addAggregation(stringStats("shape").field("word").showDistribution(true))
                .get();

            assertSearchResponse(response);
            StringStats shape = response.getAggregations().get("shape");
            assertThat(shape.getCount(), equalTo(20L));
            // each shard has an entropy of zero, and together they have a bit: a and b are equally frequent
            assertEquals(1.0, shape.getEntropy(), 1e-12);
            assertThat(shape.getDistribution(), equalTo(Map.of("a", 0.5, "b", 0.5)));
        } finally {
            // the cluster is shared by all tests of the suite, and the test may run again in it
            assertAcked(client().admin().indices().prepareDelete(index));
        }
    }

    public void testEmptyHistogramBucket() {
        SearchResponse response = client().prepareSearch(INDEX)
            .addAggregation(
                histogram("histo").field("number")
                    .interval(10)
                    .extendedBounds(0, 40)
                    .minDocCount(0)
                    .subAggregation(stringStats("shape").field("domain").showDistribution(true))
            )
            .get();

        assertSearchResponse(response);
        Histogram histo = response.getAggregations().get("histo");
        List<? extends Histogram.Bucket> buckets = histo.getBuckets();
        assertThat(buckets.size(), equalTo(5));

        // the number of tags of a document is at most three, so the first bucket holds all documents and the others hold none
        StringStats filled = buckets.get(0).getAggregations().get("shape");
        new Expected(allDomains()).assertMatches(filled);

        for (int i = 1; i < buckets.size(); i++) {
            assertThat(buckets.get(i).getDocCount(), equalTo(0L));
            StringStats empty = buckets.get(i).getAggregations().get("shape");
            assertThat(empty, notNullValue());
            assertThat(empty.getCount(), equalTo(0L));
            assertTrue(Double.isNaN(empty.getMinLength()));
            assertTrue(Double.isNaN(empty.getEntropy()));
            assertTrue(empty.getDistribution().isEmpty());
        }
    }

    public void testNumericFieldIsRejected() {
        assertFailures(
            client().prepareSearch(INDEX).addAggregation(stringStats("shape").field("number")),
            RestStatus.BAD_REQUEST,
            containsString("Field [number] of type [long] is not supported for aggregation [string_stats]")
        );
    }

    public void testOrderByAnUnknownStatisticFails() {
        final Exception e = expectThrows(
            Exception.class,
            () -> client().prepareSearch(INDEX)
                .addAggregation(
                    terms("by_host").field("host")
                        .order(BucketOrder.aggregation("shape.bogus", true))
                        .subAggregation(stringStats("shape").field("domain"))
                )
                .get()
        );

        assertThat(
            ExceptionsHelper.stackTrace(e),
            containsString("Unknown metric name [bogus] on multi-value metrics aggregation [shape]")
        );
        assertNotNull(ExceptionsHelper.unwrap(e, IllegalArgumentException.class));
    }

    public void testBucketsPathToAnUnknownStatisticFails() {
        final Exception e = expectThrows(
            Exception.class,
            () -> client().prepareSearch(INDEX)
                .addAggregation(terms("by_host").field("host").subAggregation(stringStats("shape").field("domain")))
                .addAggregation(maxBucket("most_random", "by_host>shape.bogus"))
                .get()
        );

        assertThat(
            ExceptionsHelper.stackTrace(e),
            containsString(
                "Unknown value [bogus] in string_stats aggregation, expected one of [count, min_length, max_length, avg_length, entropy]"
            )
        );
        assertNotNull(ExceptionsHelper.unwrap(e, IllegalArgumentException.class));
    }

    public void testEveryStatisticCanBeUsedInBucketsPath() {
        final Set<String> seen = new HashSet<>();
        for (String statistic : List.of("count", "min_length", "max_length", "avg_length", "entropy")) {
            SearchResponse response = client().prepareSearch(INDEX)
                .addAggregation(terms("by_host").field("host").subAggregation(stringStats("shape").field("domain")))
                .addAggregation(maxBucket("top", "by_host>shape." + statistic))
                .get();

            assertSearchResponse(response);
            BucketMetricValue top = response.getAggregations().get("top");
            double expected = Double.NEGATIVE_INFINITY;
            for (List<String> domains : DOMAINS.values()) {
                expected = Math.max(expected, valueOf(new Expected(domains), statistic));
            }
            assertEquals(statistic, expected, top.value(), 1e-9);
            seen.add(statistic);
        }
        assertThat(seen.size(), equalTo(5));
    }

    private static double valueOf(Expected expected, String statistic) {
        switch (statistic) {
            case "count":
                return expected.count;
            case "min_length":
                return expected.min;
            case "max_length":
                return expected.max;
            case "avg_length":
                return expected.avg;
            default:
                return expected.entropy;
        }
    }

    private static double valueOf(StringStats shape, String statistic) {
        switch (statistic) {
            case "count":
                return shape.getCount();
            case "min_length":
                return shape.getMinLength();
            case "max_length":
                return shape.getMaxLength();
            case "avg_length":
                return shape.getAvgLength();
            default:
                return shape.getEntropy();
        }
    }
}
