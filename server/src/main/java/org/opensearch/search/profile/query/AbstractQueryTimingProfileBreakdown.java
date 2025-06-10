/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.profile.query;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.search.profile.AbstractTimingProfileBreakdown;

import java.util.List;
import java.util.Map;

/**
 * A {@link AbstractTimingProfileBreakdown} for query timings with contexts.
 */
@PublicApi(since="3.0.0")
public abstract class AbstractQueryTimingProfileBreakdown extends AbstractTimingProfileBreakdown {

    public abstract AbstractQueryTimingProfileBreakdown context(Object context);

    public abstract AbstractTimingProfileBreakdown getPluginBreakdown(Object context);

    public void associateCollectorToLeaves(Collector collector, LeafReaderContext leaf) {}

    public void associateCollectorsToLeaves(Map<Collector, List<LeafReaderContext>> collectorToLeaves) {}
}
