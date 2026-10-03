/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.sampler;

import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.BaseAggregationTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;

public class RandomSamplerAggregationBuilderTests extends BaseAggregationTestCase<RandomSamplerAggregationBuilder> {

    @Override
    protected RandomSamplerAggregationBuilder createTestAggregatorBuilder() {
        RandomSamplerAggregationBuilder builder = AggregationBuilders.randomSampler(randomAlphaOfLengthBetween(3, 10));
        builder.probability(randomBoolean() ? 1.0 : randomDoubleBetween(1e-4, 0.49, false));
        if (randomBoolean()) {
            builder.seed(randomInt());
        }
        return builder;
    }

    public void testRejectsProbabilityOutsideTheUsefulRange() {
        for (double probability : new double[] { 0.0, -0.1, 0.5, 0.7, 1.5, Double.NaN }) {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> AggregationBuilders.randomSampler("test").probability(probability)
            );
            assertThat(e.getMessage(), containsString("[probability]"));
            assertThat(e.getMessage(), containsString("exactly 1.0"));
        }
    }

    public void testProbabilityIsRequired() throws IOException {
        String json = "{ \"seed\": 42 }";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            parser.nextToken();
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> RandomSamplerAggregationBuilder.PARSER.parse(parser, "sampled")
            );
            assertThat(e.getMessage(), containsString("probability"));
        }
    }

    public void testUnknownFieldIsRejected() throws IOException {
        String json = "{ \"probability\": 0.1, \"nonsense\": 3 }";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            parser.nextToken();
            Exception e = expectThrows(Exception.class, () -> RandomSamplerAggregationBuilder.PARSER.parse(parser, "sampled"));
            assertThat(e.getMessage(), containsString("nonsense"));
        }
    }

    public void testParsesProbabilityAndSeed() throws IOException {
        String json = "{ \"probability\": 0.25, \"seed\": 7 }";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            parser.nextToken();
            RandomSamplerAggregationBuilder builder = RandomSamplerAggregationBuilder.PARSER.parse(parser, "sampled");
            assertEquals(0.25, builder.probability(), 0.0);
            assertEquals(Integer.valueOf(7), builder.seed());
        }
    }

    public void testSeedIsOptional() throws IOException {
        String json = "{ \"probability\": 0.25 }";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            parser.nextToken();
            RandomSamplerAggregationBuilder builder = RandomSamplerAggregationBuilder.PARSER.parse(parser, "sampled");
            assertNull(builder.seed());
        }
    }
}
