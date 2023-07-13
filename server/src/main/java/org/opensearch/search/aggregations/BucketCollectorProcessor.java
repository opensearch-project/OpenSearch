/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations;

import org.apache.lucene.search.Collector;
import org.apache.lucene.search.MultiCollector;
import org.opensearch.common.lucene.MinimumScoreCollector;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.profile.query.InternalProfileCollector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/**
 * Processor to perform collector level processing specific to {@link BucketCollector} in different stages like: a) PostCollection
 * after search on each leaf is completed and b) process the collectors to perform reduce after collection is completed
 */
public class BucketCollectorProcessor {

    /**
     * Performs {@link BucketCollector#postCollection()} on all the {@link BucketCollector} in the given {@link Collector} collector tree
     * after the collection of documents on a leaf is completed. This method will be called by different slice threads on its own collector
     * tree instance in case of concurrent segment search such that postCollection happens on the same slice thread which initialize and
     * perform collection of the documents for a leaf segment. For sequential search case, there is always a single search thread which
     * performs both collection and postCollection on {@link BucketCollector}.
     * <p>
     * This was originally done in {@link org.opensearch.search.aggregations.AggregationProcessor#postProcess(SearchContext)}. But with
     * concurrent segment search path this needs to be performed here. There are AssertingCodecs in lucene which validates that the
     * DocValues created for a field is always used by the same thread for a request. In concurrent segment search case, the DocValues
     * gets initialized on different threads for different segments (or slices). Whereas the postProcess happens as part of reduce phase
     * and is performed on the separate thread which is from search threadpool and not from slice threadpool. So two different threads
     * performs the access on the DocValues causing the AssertingCodec to fail. From functionality perspective, there is no issue as
     * DocValues for each segment is always accessed by a single thread at a time but those threads may be different (e.g. slice thread
     * during collection and then search thread during reduce)
     * </p>
     * <p>
     * NOTE: We can evaluate and deprecate this postCollection processing once lucene release the changes described in the
     * <a href="https://github.com/apache/lucene/issues/12375">issue-12375</a>. With this new change we should be able to implement
     * {@link BucketCollector#postCollection()} functionality using the lucene interface directly such that postCollection gets called
     * from the slice thread by lucene itself
     * </p>
     * @param collectorTree collector tree used by calling thread
     */
    public void processPostCollection(Collector collectorTree) throws IOException {
        final Queue<Collector> collectors = new LinkedList<>();
        collectors.offer(collectorTree);
        while (!collectors.isEmpty()) {
            Collector currentCollector = collectors.poll();
            if (currentCollector instanceof InternalProfileCollector) {
                collectors.offer(((InternalProfileCollector) currentCollector).getCollector());
            } else if (currentCollector instanceof MinimumScoreCollector) {
                collectors.offer(((MinimumScoreCollector) currentCollector).getCollector());
            } else if (currentCollector instanceof MultiCollector) {
                for (Collector innerCollector : ((MultiCollector) currentCollector).getCollectors()) {
                    collectors.offer(innerCollector);
                }
            } else if (currentCollector instanceof BucketCollector) {
                ((BucketCollector) currentCollector).postCollection();
            }
        }
    }

    /**
     * Unwraps the input collection of {@link Collector} to get the list of the {@link Aggregator} used by different slice threads. The
     * input is expected to contain the collectors related to Aggregations only as that is passed to {@link AggregationCollectorManager}
     * during the reduce phase. This list of {@link Aggregator} is used to create {@link InternalAggregation} and optionally perform
     * reduce at shard level before returning response to coordinator
     * @param collectors collection of aggregation collectors to reduce
     * @return list of unwrapped {@link Aggregator}
     */
    public List<Aggregator> toAggregators(Collection<Collector> collectors) {
        List<Aggregator> aggregators = new ArrayList<>();

        final Deque<Collector> allCollectors = new LinkedList<>(collectors);
        while (!allCollectors.isEmpty()) {
            final Collector currentCollector = allCollectors.pop();
            if (currentCollector instanceof Aggregator) {
                aggregators.add((Aggregator) currentCollector);
            } else if (currentCollector instanceof InternalProfileCollector) {
                if (((InternalProfileCollector) currentCollector).getCollector() instanceof Aggregator) {
                    aggregators.add((Aggregator) ((InternalProfileCollector) currentCollector).getCollector());
                } else if (((InternalProfileCollector) currentCollector).getCollector() instanceof MultiBucketCollector) {
                    allCollectors.addAll(
                        Arrays.asList(((MultiBucketCollector) ((InternalProfileCollector) currentCollector).getCollector()).getCollectors())
                    );
                }
            } else if (currentCollector instanceof MultiBucketCollector) {
                allCollectors.addAll(Arrays.asList(((MultiBucketCollector) currentCollector).getCollectors()));
            }
        }
        return aggregators;
    }
}
