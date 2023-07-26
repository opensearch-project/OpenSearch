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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.ScoreMode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * This class wraps a Lucene Collector and times the execution of:
 * - setScorer()
 * - collect()
 * - doSetNextReader()
 * - needsScores()
 *
 * InternalProfiler facilitates the linking of the Collector graph
 *
 * @opensearch.internal
 */
public class InternalProfileCollector implements Collector, InternalProfileComponent {

    /**
     * A more friendly representation of the Collector's class name
     */
    private final String collectorName;

    /**
     * A "hint" to help provide some context about this Collector
     */
    private final String reason;

    /** The wrapped collector */
    private final ProfileCollector collector;

    /**
     * A list of "embedded" children collectors
     */
    private final List<? extends InternalProfileComponent> children;

    public InternalProfileCollector(Collector collector, String reason, List<? extends InternalProfileComponent> children) {
        this.collector = new ProfileCollector(collector);
        this.reason = reason;
        this.collectorName = deriveCollectorName(collector);
        this.children = children;
    }

    /**
     * @return the profiled time for this collector (inclusive of children)
     */
    public long getTime() {
        return collector.getTime();
    }

    /**
     * @return the profiled start time for this collector (inclusive of children)
     */
    public long getSliceStartTime() {
        return collector.getSliceStartTime();
    }

    /**
     * @return a human readable "hint" about what this collector was used for
     */
    public String getReason() {
        return this.reason;
    }

    /**
     * @return the lucene class name of the collector
     */
    public String getName() {
        return this.collectorName;
    }

    /**
     * @return the underlying collector instance being profiled
     */
    public Collector getCollector() {
        return collector.getDelegate();
    }

    /**
     * Creates a human-friendly representation of the Collector name.
     *
     * InternalBucket Collectors use the aggregation name in their toString() method,
     * which makes the profiled output a bit nicer.
     *
     * @param c The Collector to derive a name from
     * @return  A (hopefully) prettier name
     */
    private String deriveCollectorName(Collector c) {
        String s = c.getClass().getSimpleName();

        // MutiCollector which wraps multiple BucketCollectors is generated
        // via an anonymous class, so this corrects the lack of a name by
        // asking the enclosingClass
        if (s.equals("")) {
            s = c.getClass().getEnclosingClass().getSimpleName();
        }

        // Aggregation collector toString()'s include the user-defined agg name
        if (reason.equals(CollectorResult.REASON_AGGREGATION) || reason.equals(CollectorResult.REASON_AGGREGATION_GLOBAL)) {
            s += ": [" + c.toString() + "]";
        }
        return s;
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
        return collector.getLeafCollector(context);
    }

    @Override
    public ScoreMode scoreMode() {
        return collector.scoreMode();
    }

    @Override
    public Collection<? extends InternalProfileComponent> children() {
        return children;
    }

    @Override
    public CollectorResult getCollectorTree() {
        return InternalProfileCollector.doGetCollectorTree(this);
    }

    static CollectorResult doGetCollectorTree(InternalProfileComponent collector) {
        List<CollectorResult> childResults = new ArrayList<>(collector.children().size());
        for (InternalProfileComponent child : collector.children()) {
            CollectorResult result = doGetCollectorTree(child);
            childResults.add(result);
        }
        return new CollectorResult(collector.getName(), collector.getReason(), collector.getTime(), childResults);
    }
}
