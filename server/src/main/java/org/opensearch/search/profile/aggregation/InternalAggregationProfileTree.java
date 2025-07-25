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

package org.opensearch.search.profile.aggregation;

import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.profile.AbstractInternalProfileTree;

/**
 * The profiling tree for different levels of agg profiling
 *
 * @opensearch.internal
 */
public class InternalAggregationProfileTree extends AbstractInternalProfileTree<AggregationProfileBreakdown, Aggregator> {

    @Override
    protected AggregationProfileBreakdown createProfileBreakdown(Aggregator aggregator) {
        return new AggregationProfileBreakdown();
    }

    @Override
    protected String getTypeFromElement(Aggregator element) {

        // Anonymous classes (such as NonCollectingAggregator in TermsAgg) won't have a name,
        // we need to get the super class
        if (element.getClass().getSimpleName().isEmpty()) {
            return element.getClass().getSuperclass().getSimpleName();
        }
        Class<?> enclosing = element.getClass().getEnclosingClass();
        if (enclosing != null) {
            return enclosing.getSimpleName() + "." + element.getClass().getSimpleName();
        }
        return element.getClass().getSimpleName();
    }

    /**
     * @return is used to group aggregations with same name across slices.
     * So the name returned here should be same across slices for an aggregation operator.
     */
    @Override
    protected String getDescriptionFromElement(Aggregator element) {
        return element.name();
    }

}
