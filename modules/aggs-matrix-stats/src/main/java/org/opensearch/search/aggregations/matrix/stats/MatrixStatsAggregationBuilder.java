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

package org.opensearch.search.aggregations.matrix.stats;

import org.opensearch.Version;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.MultiValueMode;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.AggregatorFactory;
import org.opensearch.search.aggregations.support.ArrayValuesSourceAggregationBuilder;
import org.opensearch.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class MatrixStatsAggregationBuilder extends ArrayValuesSourceAggregationBuilder.LeafOnly<MatrixStatsAggregationBuilder> {
    public static final String NAME = "matrix_stats";

    private MultiValueMode multiValueMode = MultiValueMode.AVG;

    public MatrixStatsAggregationBuilder(String name) {
        super(name);
    }

    protected MatrixStatsAggregationBuilder(
        MatrixStatsAggregationBuilder clone,
        AggregatorFactories.Builder factoriesBuilder,
        Map<String, Object> metadata
    ) {
        super(clone, factoriesBuilder, metadata);
        this.multiValueMode = clone.multiValueMode;
    }

    @Override
    protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
        return new MatrixStatsAggregationBuilder(this, factoriesBuilder, metadata);
    }

    /**
     * Read from a stream.
     */
    public MatrixStatsAggregationBuilder(StreamInput in) throws IOException {
        super(in);
        if (in.getVersion().onOrAfter(Version.V_3_1_0)) {
            this.multiValueMode = in.readEnum(MultiValueMode.class);
        } else {
            this.multiValueMode = MultiValueMode.AVG;
        }
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        if (out.getVersion().onOrAfter(Version.V_3_1_0)) {
            out.writeEnum(multiValueMode);
        }
    }

    public MatrixStatsAggregationBuilder multiValueMode(MultiValueMode multiValueMode) {
        this.multiValueMode = multiValueMode;
        return this;
    }

    public MultiValueMode multiValueMode() {
        return this.multiValueMode;
    }

    @Override
    protected MatrixStatsAggregatorFactory innerBuild(
        QueryShardContext queryShardContext,
        Map<String, ValuesSourceConfig> configs,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder
    ) throws IOException {
        return new MatrixStatsAggregatorFactory(name, configs, multiValueMode, queryShardContext, parent, subFactoriesBuilder, metadata);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.field(MULTIVALUE_MODE_FIELD.getPreferredName(), multiValueMode);
        return builder;
    }

    @Override
    public String getType() {
        return NAME;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        MatrixStatsAggregationBuilder other = (MatrixStatsAggregationBuilder) obj;
        return multiValueMode == other.multiValueMode;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), multiValueMode);
    }
}
