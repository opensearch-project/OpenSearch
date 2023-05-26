/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.index.rankeval;

import org.opensearch.action.OriginalIndices;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import static org.opensearch.common.xcontent.XContentHelper.toXContent;
import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;
import static org.opensearch.test.XContentTestUtils.insertRandomFields;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertToXContentEquivalent;

public class EvalQueryQualityTests extends OpenSearchTestCase {

    private static NamedWriteableRegistry namedWritableRegistry = new NamedWriteableRegistry(
        new RankEvalModulePlugin().getNamedWriteables()
    );

    @SuppressWarnings("resource")
    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(new RankEvalModulePlugin().getNamedXContent());
    }

    public static EvalQueryQuality randomEvalQueryQuality() {
        int numberOfSearchHits = randomInt(5);
        List<RatedSearchHit> ratedHits = new ArrayList<>();
        for (int i = 0; i < numberOfSearchHits; i++) {
            RatedSearchHit ratedSearchHit = RatedSearchHitTests.randomRatedSearchHit();
            // we need to associate each hit with an index name otherwise rendering will not work
            ratedSearchHit.getSearchHit().shard(new SearchShardTarget("_na_", new ShardId("index", "_na_", 0), null, OriginalIndices.NONE));
            ratedHits.add(ratedSearchHit);
        }
        EvalQueryQuality evalQueryQuality = new EvalQueryQuality(randomAlphaOfLength(10), randomDoubleBetween(0.0, 1.0, true));
        if (randomBoolean()) {
            int metricDetail = randomIntBetween(0, 2);
            switch (metricDetail) {
                case 0:
                    evalQueryQuality.setMetricDetails(new PrecisionAtK.Detail(randomIntBetween(0, 1000), randomIntBetween(0, 1000)));
                    break;
                case 1:
                    evalQueryQuality.setMetricDetails(new MeanReciprocalRank.Detail(randomIntBetween(0, 1000)));
                    break;
                case 2:
                    evalQueryQuality.setMetricDetails(
                        new DiscountedCumulativeGain.Detail(
                            randomDoubleBetween(0, 1, true),
                            randomBoolean() ? randomDoubleBetween(0, 1, true) : 0,
                            randomInt()
                        )
                    );
                    break;
                default:
                    throw new IllegalArgumentException("illegal randomized value in test");
            }
        }
        evalQueryQuality.addHitsAndRatings(ratedHits);
        return evalQueryQuality;
    }

    public void testSerialization() throws IOException {
        EvalQueryQuality original = randomEvalQueryQuality();
        EvalQueryQuality deserialized = copy(original);
        assertEquals(deserialized, original);
        assertEquals(deserialized.hashCode(), original.hashCode());
        assertNotSame(deserialized, original);
    }

    public void testXContentParsing() throws IOException {
        EvalQueryQuality testItem = randomEvalQueryQuality();
        boolean humanReadable = randomBoolean();
        XContentType xContentType = randomFrom(XContentType.values());
        BytesReference originalBytes = toShuffledXContent(testItem, xContentType, ToXContent.EMPTY_PARAMS, humanReadable);
        // skip inserting random fields for:
        // - the root object, since we expect a particular queryId there in this test
        // - the `metric_details` section, which can potentially contain different namedXContent names
        // - everything under `hits` (we test lenient SearchHit parsing elsewhere)
        Predicate<String> pathsToExclude = path -> path.isEmpty() || path.endsWith("metric_details") || path.contains("hits");
        BytesReference withRandomFields = insertRandomFields(xContentType, originalBytes, pathsToExclude, random());
        EvalQueryQuality parsedItem;
        try (XContentParser parser = createParser(xContentType.xContent(), withRandomFields)) {
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
            ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.nextToken(), parser);
            String queryId = parser.currentName();
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
            parsedItem = EvalQueryQuality.fromXContent(parser, queryId);
            ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.currentToken(), parser);
            ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.nextToken(), parser);
            assertNull(parser.nextToken());
        }
        assertNotSame(testItem, parsedItem);
        // we cannot check equality of object here because some information (e.g. SearchHit#shard) cannot fully be
        // parsed back after going through the rest layer. That's why we only check that the original and the parsed item
        // have the same xContent representation
        assertToXContentEquivalent(originalBytes, toXContent(parsedItem, xContentType, humanReadable), xContentType);
    }

    private static EvalQueryQuality copy(EvalQueryQuality original) throws IOException {
        return OpenSearchTestCase.copyWriteable(original, namedWritableRegistry, EvalQueryQuality::new);
    }

    public void testEqualsAndHash() throws IOException {
        checkEqualsAndHashCode(randomEvalQueryQuality(), EvalQueryQualityTests::copy, EvalQueryQualityTests::mutateTestItem);
    }

    private static EvalQueryQuality mutateTestItem(EvalQueryQuality original) {
        String id = original.getId();
        double metricScore = original.metricScore();
        List<RatedSearchHit> ratedHits = new ArrayList<>(original.getHitsAndRatings());
        MetricDetail metricDetails = original.getMetricDetails();
        switch (randomIntBetween(0, 3)) {
            case 0:
                id = id + "_";
                break;
            case 1:
                metricScore = metricScore + 0.1;
                break;
            case 2:
                if (metricDetails == null) {
                    metricDetails = new PrecisionAtK.Detail(1, 5);
                } else {
                    metricDetails = null;
                }
                break;
            case 3:
                ratedHits.add(RatedSearchHitTests.randomRatedSearchHit());
                break;
            default:
                throw new IllegalStateException("The test should only allow four parameters mutated");
        }
        EvalQueryQuality evalQueryQuality = new EvalQueryQuality(id, metricScore);
        evalQueryQuality.setMetricDetails(metricDetails);
        evalQueryQuality.addHitsAndRatings(ratedHits);
        return evalQueryQuality;
    }
}
