/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.metrics;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.opensearch.common.document.DocumentField;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.core.ParseField;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.ParsedAggregation;
import org.opensearch.test.InternalAggregationTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

public class InternalTopMetricsTests extends InternalAggregationTestCase<InternalTopMetrics> {

    @Override
    protected Predicate<String> excludePathsFromXContentInsertion() {
        return path -> path.contains("top") || path.contains("metrics");
    }

    @Override
    protected InternalTopMetrics createTestInstance(String name, Map<String, Object> metadata) {
        String metricField = randomFrom("metric_a", "metric_b");
        int sortValue = randomIntBetween(1, 1000);
        float score = randomFloat();
        InternalTopHits topHits = createTopHits(name, "doc-" + sortValue, metricField, "value-" + sortValue, sortValue, score, metadata);
        return new InternalTopMetrics(name, topHits, List.of(metricField), metadata);
    }

    @Override
    protected void assertReduced(InternalTopMetrics reduced, List<InternalTopMetrics> inputs) {
        float maxScore = Float.NEGATIVE_INFINITY;
        String expectedMetricField = null;
        String expectedMetricValue = null;
        for (InternalTopMetrics input : inputs) {
            SearchHit hit = input.getTopHits().getHits().getAt(0);
            if (hit.getScore() > maxScore) {
                maxScore = hit.getScore();
                expectedMetricField = input.getMetricFields().get(0);
                expectedMetricValue = hit.field(expectedMetricField).getValue();
            }
        }
        SearchHit reducedHit = reduced.getTopHits().getHits().getAt(0);
        assertEquals(expectedMetricValue, reducedHit.field(expectedMetricField).getValue());
    }

    @Override
    protected void assertFromXContent(InternalTopMetrics aggregation, ParsedAggregation parsedAggregation) {
        ParsedTopMetrics parsed = (ParsedTopMetrics) parsedAggregation;
        assertEquals(1, parsed.top.size());
        Map<String, Object> parsedEntry = parsed.top.get(0);
        Map<String, Object> parsedMetrics = (Map<String, Object>) parsedEntry.get("metrics");
        String metricField = aggregation.getMetricFields().get(0);
        assertEquals(aggregation.getTopHits().getHits().getAt(0).field(metricField).getValue(), parsedMetrics.get(metricField));
    }

    @Override
    protected InternalTopMetrics mutateInstance(InternalTopMetrics instance) {
        String metricField = instance.getMetricFields().get(0);
        SearchHit hit = instance.getTopHits().getHits().getAt(0);
        int currentSort = ((Number) hit.getSortValues()[0]).intValue();
        float currentScore = hit.getScore();
        Map<String, Object> metadata = instance.getMetadata();
        switch (between(0, 2)) {
            case 0:
                return createTestInstance(instance.getName() + randomAlphaOfLength(4), metadata);
            case 1:
                return new InternalTopMetrics(
                    instance.getName(),
                    createTopHits(
                        instance.getName(),
                        hit.getId(),
                        metricField,
                        hit.field(metricField).getValue() + "_mutated",
                        currentSort + 1,
                        currentScore + 1.0f,
                        metadata
                    ),
                    instance.getMetricFields(),
                    metadata
                );
            default:
                Map<String, Object> newMetadata = metadata == null ? new HashMap<>() : new HashMap<>(metadata);
                newMetadata.put(randomAlphaOfLength(4), randomAlphaOfLength(4));
                return new InternalTopMetrics(instance.getName(), instance.getTopHits(), instance.getMetricFields(), newMetadata);
        }
    }

    @Override
    protected List<NamedXContentRegistry.Entry> getNamedXContents() {
        List<NamedXContentRegistry.Entry> entries = new ArrayList<>(super.getNamedXContents());
        entries.add(
            new NamedXContentRegistry.Entry(
                Aggregation.class,
                new ParseField(TopMetricsAggregationBuilder.NAME),
                (parser, name) -> ParsedTopMetrics.fromXContent(parser, (String) name)
            )
        );
        return entries;
    }

    private static InternalTopHits createTopHits(
        String name,
        String id,
        String metricFieldName,
        String metricValue,
        int sortValue,
        float score,
        Map<String, Object> metadata
    ) {
        Map<String, DocumentField> documentFields = new HashMap<>();
        documentFields.put(metricFieldName, new DocumentField(metricFieldName, List.of(metricValue)));
        SearchHit hit = new SearchHit(0, id, documentFields, Collections.emptyMap());
        hit.score(score);
        hit.sortValues(new Object[] { sortValue }, new DocValueFormat[] { DocValueFormat.RAW });

        SearchHits searchHits = new SearchHits(new SearchHit[] { hit }, new TotalHits(1, TotalHits.Relation.EQUAL_TO), score);
        TopDocs topDocs = new TopDocs(new TotalHits(1, TotalHits.Relation.EQUAL_TO), new ScoreDoc[] { new ScoreDoc(0, score) });
        return new InternalTopHits(name, 0, 1, new TopDocsAndMaxScore(topDocs, score), searchHits, metadata);
    }

    private static class ParsedTopMetrics extends ParsedAggregation {
        private final List<Map<String, Object>> top = new ArrayList<>();

        @Override
        public String getType() {
            return TopMetricsAggregationBuilder.NAME;
        }

        static ParsedTopMetrics fromXContent(XContentParser parser, String name) throws IOException {
            ParsedTopMetrics aggregation = new ParsedTopMetrics();
            aggregation.setName(name);
            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if ("top".equals(currentFieldName) && token == XContentParser.Token.START_ARRAY) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        Map<String, Object> parsedTopEntry = parser.map();
                        Map<String, Object> filteredTopEntry = new HashMap<>(2);
                        if (parsedTopEntry.containsKey("sort")) {
                            filteredTopEntry.put("sort", parsedTopEntry.get("sort"));
                        }
                        if (parsedTopEntry.containsKey("metrics")) {
                            filteredTopEntry.put("metrics", parsedTopEntry.get("metrics"));
                        }
                        aggregation.top.add(filteredTopEntry);
                    }
                } else if (InternalAggregation.CommonFields.META.getPreferredName().equals(currentFieldName)
                    && token == XContentParser.Token.START_OBJECT) {
                    aggregation.metadata = Collections.unmodifiableMap(parser.map());
                } else {
                    parser.skipChildren();
                }
            }
            return aggregation;
        }

        @Override
        protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
            builder.startArray("top");
            for (Map<String, Object> entry : top) {
                builder.map(entry);
            }
            builder.endArray();
            return builder;
        }
    }
}
