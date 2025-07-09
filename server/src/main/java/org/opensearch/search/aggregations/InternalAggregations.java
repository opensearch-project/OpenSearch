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

package org.opensearch.search.aggregations;

import org.opensearch.Version;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.search.aggregations.InternalAggregation.ReduceContext;
import org.opensearch.search.aggregations.pipeline.PipelineAggregator;
import org.opensearch.search.aggregations.pipeline.SiblingPipelineAggregator;
import org.opensearch.search.aggregations.support.AggregationPath;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * An internal implementation of {@link Aggregations}.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public final class InternalAggregations extends Aggregations implements Writeable {

    public static final InternalAggregations EMPTY = new InternalAggregations(Collections.emptyList());

    private static final Comparator<InternalAggregation> INTERNAL_AGG_COMPARATOR = (agg1, agg2) -> {
        if (agg1.isMapped() == agg2.isMapped()) {
            return 0;
        } else if (agg1.isMapped() && agg2.isMapped() == false) {
            return -1;
        } else {
            return 1;
        }
    };

    /**
     * Constructs a new aggregation.
     */
    public InternalAggregations(List<InternalAggregation> aggregations) {
        super(aggregations);
    }

    public static InternalAggregations from(List<InternalAggregation> aggregations) {
        if (aggregations.isEmpty()) {
            return EMPTY;
        }
        return new InternalAggregations(aggregations);
    }

    public static InternalAggregations readFrom(StreamInput in) throws IOException {
        final InternalAggregations res = from(in.readList(stream -> in.readNamedWriteable(InternalAggregation.class)));
        return res;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeNamedWriteableList(getInternalAggregations());
    }

    /**
     * Make a mutable copy of the aggregation results.
     * <p>
     * IMPORTANT: The copy doesn't include any pipeline aggregations, if there are any.
     */
    public List<InternalAggregation> copyResults() {
        return new ArrayList<>(getInternalAggregations());
    }

    @SuppressWarnings("unchecked")
    private List<InternalAggregation> getInternalAggregations() {
        return (List<InternalAggregation>) aggregations;
    }

    /**
     * Get value to use when sorting by a descendant of the aggregation containing this.
     */
    public double sortValue(AggregationPath.PathElement head, Iterator<AggregationPath.PathElement> tail) {
        InternalAggregation aggregation = get(head.name);
        if (aggregation == null) {
            throw new IllegalArgumentException("Cannot find aggregation named [" + head.name + "]");
        }
        if (tail.hasNext()) {
            return aggregation.sortValue(tail.next(), tail);
        }
        return aggregation.sortValue(head.key);
    }

    /**
     * Begin the reduction process.  This should be the entry point for the "first" reduction, e.g. called by
     * SearchPhaseController or anywhere else that wants to initiate a reduction.  It _should not_ be called
     * as an intermediate reduction step (e.g. in the middle of an aggregation tree).
     * <p>
     * This method first reduces the aggregations, and if it is the final reduce, then reduce the pipeline
     * aggregations (both embedded parent/sibling as well as top-level sibling pipelines)
     */
    public static InternalAggregations topLevelReduce(List<InternalAggregations> aggregationsList, ReduceContext context) {
        InternalAggregations reduced = reduce(aggregationsList, context);
        if (reduced == null) {
            return null;
        }

        if (context.isFinalReduce()) {
            List<InternalAggregation> reducedInternalAggs = reduced.getInternalAggregations();
            reducedInternalAggs = reducedInternalAggs.stream()
                .map(agg -> agg.reducePipelines(agg, context, context.pipelineTreeRoot().subTree(agg.getName())))
                .collect(Collectors.toList());

            for (PipelineAggregator pipelineAggregator : context.pipelineTreeRoot().aggregators()) {
                SiblingPipelineAggregator sib = (SiblingPipelineAggregator) pipelineAggregator;
                InternalAggregation newAgg = sib.doReduce(from(reducedInternalAggs), context);
                reducedInternalAggs.add(newAgg);
            }
            return from(reducedInternalAggs);
        }
        return reduced;
    }

    /**
     * Reduces the given list of aggregations as well as the top-level pipeline aggregators extracted from the first
     * {@link InternalAggregations} object found in the list.
     * Note that pipeline aggregations _are not_ reduced by this method.  Pipelines are handled
     * separately by {@link InternalAggregations#topLevelReduce(List, ReduceContext)}
     */
    public static InternalAggregations reduce(List<InternalAggregations> aggregationsList, ReduceContext context) {
        if (aggregationsList.isEmpty()) {
            return null;
        }

        // first we collect all aggregations of the same type and list them together
        Map<String, List<InternalAggregation>> aggByName = new HashMap<>();
        for (InternalAggregations aggregations : aggregationsList) {
            for (Aggregation aggregation : aggregations.aggregations) {
                context.checkCancelled();
                List<InternalAggregation> aggs = aggByName.computeIfAbsent(
                    aggregation.getName(),
                    k -> new ArrayList<>(aggregationsList.size())
                );
                aggs.add((InternalAggregation) aggregation);
            }
        }

        // now we can use the first aggregation of each list to handle the reduce of its list
        List<InternalAggregation> reducedAggregations = new ArrayList<>();
        for (Map.Entry<String, List<InternalAggregation>> entry : aggByName.entrySet()) {
            List<InternalAggregation> aggregations = entry.getValue();
            // Sort aggregations so that unmapped aggs come last in the list
            // If all aggs are unmapped, the agg that leads the reduction will just return itself
            aggregations.sort(INTERNAL_AGG_COMPARATOR);
            InternalAggregation first = aggregations.get(0); // the list can't be empty as it's created on demand
            if (first.mustReduceOnSingleInternalAgg() || aggregations.size() > 1) {
                reducedAggregations.add(first.reduce(aggregations, context));
            } else {
                // no need for reduce phase
                reducedAggregations.add(first);
            }
        }

        return new InternalAggregations(reducedAggregations);
    }

    /**
     * Returns the number of bytes required to serialize these aggregations in binary form.
     */
    public long getSerializedSize() {
        try (CountingStreamOutput out = new CountingStreamOutput()) {
            out.setVersion(Version.CURRENT);
            writeTo(out);
            return out.size;
        } catch (IOException exc) {
            // should never happen
            throw new RuntimeException(exc);
        }
    }

    public static InternalAggregations merge(InternalAggregations first, InternalAggregations second) {
        final List<InternalAggregation> fromFirst = first.getInternalAggregations();
        final List<InternalAggregation> fromSecond = second.getInternalAggregations();
        final List<InternalAggregation> mergedAggregation = new ArrayList<>(fromFirst.size() + fromSecond.size());
        mergedAggregation.addAll(fromFirst);
        mergedAggregation.addAll(fromSecond);
        return new InternalAggregations(mergedAggregation);
    }

    /**
     * A counting stream output
     *
     * @opensearch.internal
     */
    private static class CountingStreamOutput extends StreamOutput {
        long size = 0;

        @Override
        public void writeByte(byte b) throws IOException {
            ++size;
        }

        @Override
        public void writeBytes(byte[] b, int offset, int length) throws IOException {
            size += length;
        }

        @Override
        public void flush() throws IOException {}

        @Override
        public void close() throws IOException {}

        @Override
        public void reset() throws IOException {
            size = 0;
        }

        public long length() {
            return size;
        }
    }
}
