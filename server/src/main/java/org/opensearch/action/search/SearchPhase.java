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

package org.opensearch.action.search;

import org.opensearch.common.CheckedRunnable;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

/**
 * Base class for all individual search phases like collecting distributed frequencies, fetching documents, querying shards.
 *
 * @opensearch.internal
 */
abstract class SearchPhase implements CheckedRunnable<IOException> {
    private final String name;

    protected SearchPhase(String name) {
        this.name = Objects.requireNonNull(name, "name must not be null");
    }

    /**
     * Returns the phases name.
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the SearchPhase name as {@link SearchPhaseName}. Exception will come if SearchPhase name is not defined
     * in {@link SearchPhaseName}
     * @return {@link SearchPhaseName}
     */
    public SearchPhaseName getSearchPhaseName() {
        return SearchPhaseName.valueOf(name.toUpperCase(Locale.ROOT));
    }
}
