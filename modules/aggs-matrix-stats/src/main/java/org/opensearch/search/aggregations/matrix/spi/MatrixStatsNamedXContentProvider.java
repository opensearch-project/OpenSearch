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

package org.opensearch.search.aggregations.matrix.spi;

import org.opensearch.core.ParseField;
import org.opensearch.core.xcontent.ContextParser;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.plugins.spi.NamedXContentProvider;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.matrix.stats.MatrixStatsAggregationBuilder;
import org.opensearch.search.aggregations.matrix.stats.ParsedMatrixStats;

import java.util.List;

import static java.util.Collections.singletonList;

public class MatrixStatsNamedXContentProvider implements NamedXContentProvider {

    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContentParsers() {
        ParseField parseField = new ParseField(MatrixStatsAggregationBuilder.NAME);
        ContextParser<Object, Aggregation> contextParser = (p, name) -> ParsedMatrixStats.fromXContent(p, (String) name);
        return singletonList(new NamedXContentRegistry.Entry(Aggregation.class, parseField, contextParser));
    }
}
