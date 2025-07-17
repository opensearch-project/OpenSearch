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

package org.opensearch.search.profile.query;

import org.apache.lucene.search.Query;
import org.opensearch.search.profile.ContextualProfileBreakdown;
import org.opensearch.search.profile.ProfileMetric;
import org.opensearch.search.profile.ProfileMetricUtil;
import org.opensearch.search.profile.ProfileResult;

import java.util.Collection;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * This class returns a list of {@link ProfileResult} that can be serialized back to the client in the non-concurrent execution.
 *
 * @opensearch.internal
 */
public class InternalQueryProfileTree extends AbstractQueryProfileTree {

    private final Function<Query, Collection<Supplier<ProfileMetric>>> customProfileMetrics;

    public InternalQueryProfileTree(Function<Query, Collection<Supplier<ProfileMetric>>> customProfileMetrics) {
        this.customProfileMetrics = customProfileMetrics;
    }

    @Override
    protected ContextualProfileBreakdown createProfileBreakdown(Query query) {
        return new QueryProfileBreakdown(ProfileMetricUtil.getQueryProfileMetrics(customProfileMetrics.apply(query)));
    }
}
