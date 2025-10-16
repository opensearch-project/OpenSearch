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

package org.opensearch.search.aggregations.support;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.Strings;
import org.opensearch.search.aggregations.AggregationExecutionException;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.Aggregator.BucketComparator;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.bucket.SingleBucketAggregator;
import org.opensearch.search.aggregations.metrics.NumericMetricsAggregator;
import org.opensearch.search.sort.SortOrder;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * A path that can be used to sort/order buckets (in some multi-bucket aggregations, e.g. terms &amp; histogram) based on
 * sub-aggregations. The path may point to either a single-bucket aggregation or a metrics aggregation. If the path
 * points to a single-bucket aggregation, the sort will be applied based on the {@code doc_count} of the bucket. If this
 * path points to a metrics aggregation, if it's a single-value metrics (eg. avg, max, min, etc..) the sort will be
 * applied on that single value. If it points to a multi-value metrics, the path should point out what metric should be
 * the sort-by value.
 * <p>
 * The path has the following form:
 * {@code <aggregation_name>['>'<aggregation_name>*]['.'<metric_name>]}
 * <p>
 * Examples:
 *
 * <ul>
 *     <li>
 *         {@code agg1>agg2>agg3} - where agg1, agg2 and agg3 are all single-bucket aggs (eg filter, nested, missing, etc..). In
 *                                  this case, the order will be based on the number of documents under {@code agg3}.
 *     </li>
 *     <li>
 *         {@code agg1>agg2>agg3} - where agg1 and agg2 are both single-bucket aggs and agg3 is a single-value metrics agg (eg avg, max,
 *                                  min, etc..). In this case, the order will be based on the value of {@code agg3}.
 *     </li>
 *     <li>
 *         {@code agg1>agg2>agg3.avg} - where agg1 and agg2 are both single-bucket aggs and agg3 is a multi-value metrics agg (eg stats,
 *                                  extended_stats, etc...). In this case, the order will be based on the avg value of {@code agg3}.
 *     </li>
 * </ul>
 *
 *
 * @opensearch.internal
 */
public class AggregationPath {

    private static final String AGG_DELIM = ">";

    public static AggregationPath parse(String path) {
        String[] elements = Strings.tokenizeToStringArray(path, AGG_DELIM);
        List<PathElement> tokens = new ArrayList<>(elements.length);
        String[] tuple = new String[2];
        for (int i = 0; i < elements.length; i++) {
            String element = elements[i];
            if (i == elements.length - 1) {
                int index = element.lastIndexOf('[');
                if (index >= 0) {
                    if (index == 0 || index > element.length() - 3) {
                        throw new AggregationExecutionException("Invalid path element [" + element + "] in path [" + path + "]");
                    }
                    if (element.charAt(element.length() - 1) != ']') {
                        throw new AggregationExecutionException("Invalid path element [" + element + "] in path [" + path + "]");
                    }
                    tokens.add(new PathElement(element, element.substring(0, index), element.substring(index + 1, element.length() - 1)));
                    continue;
                }
                index = element.lastIndexOf('.');
                if (index < 0) {
                    tokens.add(new PathElement(element, element, null));
                    continue;
                }
                if (index == 0 || index > element.length() - 2) {
                    throw new AggregationExecutionException("Invalid path element [" + element + "] in path [" + path + "]");
                }
                tuple = split(element, index, tuple);
                tokens.add(new PathElement(element, tuple[0], tuple[1]));

            } else {
                int index = element.lastIndexOf('[');
                if (index >= 0) {
                    if (index == 0 || index > element.length() - 3) {
                        throw new AggregationExecutionException("Invalid path element [" + element + "] in path [" + path + "]");
                    }
                    if (element.charAt(element.length() - 1) != ']') {
                        throw new AggregationExecutionException("Invalid path element [" + element + "] in path [" + path + "]");
                    }
                    tokens.add(new PathElement(element, element.substring(0, index), element.substring(index + 1, element.length() - 1)));
                    continue;
                }
                tokens.add(new PathElement(element, element, null));
            }
        }
        return new AggregationPath(tokens);
    }

    /**
     * Element in an agg path
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public static class PathElement {

        private final String fullName;
        public final String name;
        public final String key;

        public PathElement(String fullName, String name, String key) {
            this.fullName = fullName;
            this.name = name;
            this.key = key;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            PathElement token = (PathElement) o;

            if (key != null ? !key.equals(token.key) : token.key != null) return false;
            if (!name.equals(token.name)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = name.hashCode();
            result = 31 * result + (key != null ? key.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return fullName;
        }
    }

    private final List<PathElement> pathElements;

    public AggregationPath(List<PathElement> tokens) {
        this.pathElements = tokens;
        if (tokens == null || tokens.size() == 0) {
            throw new IllegalArgumentException("Invalid path [" + this + "]");
        }
    }

    @Override
    public String toString() {
        return Strings.arrayToDelimitedString(pathElements.toArray(), AGG_DELIM);
    }

    public PathElement lastPathElement() {
        return pathElements.get(pathElements.size() - 1);
    }

    public List<PathElement> getPathElements() {
        return this.pathElements;
    }

    public List<String> getPathElementsAsStringList() {
        List<String> stringPathElements = new ArrayList<>();
        for (PathElement pathElement : this.pathElements) {
            stringPathElements.add(pathElement.name);
            if (pathElement.key != null) {
                stringPathElements.add(pathElement.key);
            }
        }
        return stringPathElements;
    }

    /**
     * Looks up the value of this path against a set of aggregation results.
     */
    public double resolveValue(InternalAggregations aggregations) {
        try {
            Iterator<PathElement> path = pathElements.iterator();
            assert path.hasNext();
            return aggregations.sortValue(path.next(), path);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid aggregation order path [" + this + "]. " + e.getMessage(), e);
        }
    }

    /**
     * Resolves the {@linkplain Aggregator} pointed to by this path against
     * the given root {@linkplain Aggregator}.
     */
    public Aggregator resolveAggregator(Aggregator root) {
        Iterator<PathElement> path = pathElements.iterator();
        assert path.hasNext();
        return root.resolveSortPathOnValidAgg(path.next(), path);
    }

    /**
     * Resolves the {@linkplain Aggregator} pointed to by the first element
     * of this path against the given root {@linkplain Aggregator}.
     */
    public Aggregator resolveTopmostAggregator(Aggregator root) {
        AggregationPath.PathElement token = pathElements.get(0);
        // TODO both unwrap and subAggregator are only used here!
        Aggregator aggregator = root.subAggregator(token.name).unwrapAggregator();
        assert (aggregator instanceof SingleBucketAggregator) || (aggregator instanceof NumericMetricsAggregator)
            : "this should be picked up before aggregation execution - on validate";
        return aggregator;
    }

    public BucketComparator bucketComparator(Aggregator root, SortOrder order) {
        return resolveAggregator(root).bucketComparator(lastPathElement().key, order);
    }

    private static String[] split(String toSplit, int index, String[] result) {
        result[0] = toSplit.substring(0, index);
        result[1] = toSplit.substring(index + 1);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        AggregationPath other = (AggregationPath) obj;
        return pathElements.equals(other.pathElements);
    }

    @Override
    public int hashCode() {
        return pathElements.hashCode();
    }
}
