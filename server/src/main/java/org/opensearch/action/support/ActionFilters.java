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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.action.support;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Set;

/**
 * Holds the action filters injected through plugins, properly sorted by {@link org.opensearch.action.support.ActionFilter#order()}
 *
 * @opensearch.internal
 */
public class ActionFilters {

    private final ActionFilter[] filters;

    public ActionFilters(Set<ActionFilter> actionFilters) {
        this.filters = actionFilters.toArray(new ActionFilter[0]);
        Arrays.sort(filters, Comparator.comparingInt(ActionFilter::order));
    }

    /**
     * Returns the action filters that have been injected
     */
    public ActionFilter[] filters() {
        return filters;
    }
}
