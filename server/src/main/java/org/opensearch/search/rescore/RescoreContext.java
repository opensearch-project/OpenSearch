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

package org.opensearch.search.rescore;

import org.apache.lucene.search.Query;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.index.query.ParsedQuery;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Context available to the rescore while it is running. Rescore
 * implementations should extend this with any additional resources that
 * they will need while rescoring.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class RescoreContext {
    private final int windowSize;
    private final Rescorer rescorer;
    private Set<Integer> rescoredDocs; // doc Ids for which rescoring was applied

    /**
     * Build the context.
     * @param rescorer the rescorer actually performing the rescore.
     */
    public RescoreContext(int windowSize, Rescorer rescorer) {
        this.windowSize = windowSize;
        this.rescorer = rescorer;
    }

    /**
     * The rescorer to actually apply.
     */
    public Rescorer rescorer() {
        return rescorer;
    }

    /**
     * Size of the window to rescore.
     */
    public int getWindowSize() {
        return windowSize;
    }

    public void setRescoredDocs(Set<Integer> docIds) {
        rescoredDocs = docIds;
    }

    public boolean isRescored(int docId) {
        return rescoredDocs != null && rescoredDocs.contains(docId);
    }

    public Set<Integer> getRescoredDocs() {
        return rescoredDocs;
    }

    /**
     * Returns queries associated with the rescorer
     */
    public List<Query> getQueries() {
        return Collections.emptyList();
    }

    /**
     * Returns parsed queries associated with the rescorer
     */
    public List<ParsedQuery> getParsedQueries() {
        return Collections.emptyList();
    }
}
