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

package org.opensearch.search.aggregations.metrics.weighted_avg;

import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.search.SearchModule;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.metrics.WeightedAvgAggregationBuilder;
import org.opensearch.search.aggregations.support.MultiValuesSourceFieldConfig;
import org.opensearch.test.AbstractSerializingTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;

import static org.hamcrest.Matchers.hasSize;

public class WeightedAvgAggregationBuilderTests extends AbstractSerializingTestCase<WeightedAvgAggregationBuilder> {
    String aggregationName;

    @Before
    public void setupName() {
        aggregationName = randomAlphaOfLength(10);
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        return new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    @Override
    protected WeightedAvgAggregationBuilder doParseInstance(XContentParser parser) throws IOException {
        assertSame(XContentParser.Token.START_OBJECT, parser.nextToken());
        AggregatorFactories.Builder parsed = AggregatorFactories.parseAggregators(parser);
        assertThat(parsed.getAggregatorFactories(), hasSize(1));
        assertThat(parsed.getPipelineAggregatorFactories(), hasSize(0));
        WeightedAvgAggregationBuilder agg = (WeightedAvgAggregationBuilder) parsed.getAggregatorFactories().iterator().next();
        assertNull(parser.nextToken());
        assertNotNull(agg);
        return agg;
    }

    @Override
    protected WeightedAvgAggregationBuilder createTestInstance() {
        MultiValuesSourceFieldConfig valueConfig = new MultiValuesSourceFieldConfig.Builder().setFieldName("value_field").build();
        MultiValuesSourceFieldConfig weightConfig = new MultiValuesSourceFieldConfig.Builder().setFieldName("weight_field").build();
        WeightedAvgAggregationBuilder aggregationBuilder = new WeightedAvgAggregationBuilder(aggregationName).value(valueConfig)
            .weight(weightConfig);
        return aggregationBuilder;
    }

    @Override
    protected Writeable.Reader<WeightedAvgAggregationBuilder> instanceReader() {
        return WeightedAvgAggregationBuilder::new;
    }
}
