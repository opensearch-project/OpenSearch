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

package org.opensearch.search.aggregations.pipeline;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.ParseField;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregation.ReduceContext;
import org.opensearch.search.aggregations.PipelineAggregationBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

/**
 * Base aggregator for pipline aggs
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public abstract class PipelineAggregator {
    /**
     * Parse the {@link PipelineAggregationBuilder} from a {@link XContentParser}.
     *
     * @opensearch.internal
     */
    @FunctionalInterface
    public interface Parser {
        ParseField BUCKETS_PATH = new ParseField("buckets_path");
        ParseField FORMAT = new ParseField("format");
        ParseField GAP_POLICY = new ParseField("gap_policy");

        /**
         * Returns the pipeline aggregator factory with which this parser is
         * associated.
         *
         * @param pipelineAggregatorName
         *            The name of the pipeline aggregation
         * @param parser the parser
         * @return The resolved pipeline aggregator factory
         * @throws java.io.IOException
         *             When parsing fails
         */
        PipelineAggregationBuilder parse(String pipelineAggregatorName, XContentParser parser) throws IOException;
    }

    /**
     * Tree of {@link PipelineAggregator}s to modify a tree of aggregations
     * after their final reduction.
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public static class PipelineTree {
        /**
         * An empty tree of {@link PipelineAggregator}s.
         */
        public static final PipelineTree EMPTY = new PipelineTree(emptyMap(), emptyList());

        private final Map<String, PipelineTree> subTrees;
        private final List<PipelineAggregator> aggregators;

        public PipelineTree(Map<String, PipelineTree> subTrees, List<PipelineAggregator> aggregators) {
            this.subTrees = subTrees;
            this.aggregators = aggregators;
        }

        /**
         * The {@link PipelineAggregator}s for the aggregation at this
         * position in the tree.
         */
        public List<PipelineAggregator> aggregators() {
            return aggregators;
        }

        /**
         * Get the sub-tree at for the named sub-aggregation or {@link #EMPTY}
         * if there are no pipeline aggragations for that sub-aggregator.
         */
        public PipelineTree subTree(String name) {
            return subTrees.getOrDefault(name, EMPTY);
        }

        /**
         * Return {@code true} if this node in the tree has any subtrees.
         */
        public boolean hasSubTrees() {
            return false == subTrees.isEmpty();
        }

        @Override
        public String toString() {
            return "PipelineTree[" + aggregators + "," + subTrees + "]";
        }
    }

    private String name;
    private String[] bucketsPaths;
    private Map<String, Object> metadata;

    protected PipelineAggregator(String name, String[] bucketsPaths, Map<String, Object> metadata) {
        this.name = name;
        this.bucketsPaths = bucketsPaths;
        this.metadata = metadata;
    }

    public String name() {
        return name;
    }

    public String[] bucketsPaths() {
        return bucketsPaths;
    }

    public Map<String, Object> metadata() {
        return metadata;
    }

    public abstract InternalAggregation reduce(InternalAggregation aggregation, ReduceContext reduceContext);
}
