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

package org.opensearch.search.profile;

import org.opensearch.search.profile.query.ConcurrentQueryProfileTree;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Base class for a profiler
 *
 * @opensearch.internal
 */
public class AbstractProfiler<PB extends AbstractProfileBreakdown<?>, E> {

    protected final AbstractInternalProfileTree<PB, E> profileTree;
    protected Map<Long, ConcurrentQueryProfileTree> threadToProfileTree;

    public AbstractProfiler(AbstractInternalProfileTree<PB, E> profileTree) {
        this.profileTree = profileTree;
    }

    /**
     * Get the {@link AbstractProfileBreakdown} for the given element in the
     * tree, potentially creating it if it did not exist.
     */
    public PB getQueryBreakdown(E query) {
        return profileTree.getProfileBreakdown(query);
    }

    /**
     * Removes the last (e.g. most recent) element on the stack.
     */
    public void pollLastElement() {
        if (threadToProfileTree == null) {
            profileTree.pollLast();
        } else {
            long threadId = Thread.currentThread().getId();
            ConcurrentQueryProfileTree concurrentProfileTree = threadToProfileTree.get(threadId);
            concurrentProfileTree.pollLast();
        }
    }

    /**
     * @return a hierarchical representation of the profiled tree
     */
    public List<ProfileResult> getTree() {
        if (threadToProfileTree == null) {
            return profileTree.getTree();
        }
        List<ProfileResult> profileResults = new ArrayList<>();
        for (Map.Entry<Long, ConcurrentQueryProfileTree> profile : threadToProfileTree.entrySet()) {
            profileResults.addAll(profile.getValue().getTree());
        }
        return profileResults;
    }

}
