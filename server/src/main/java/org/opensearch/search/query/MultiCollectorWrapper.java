/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.MultiCollector;
import org.apache.lucene.search.ScoreMode;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Wraps MultiCollector and provide access to underlying collectors.
 * Please check out https://github.com/apache/lucene/pull/455.
 *
 * @opensearch.internal
 */
public class MultiCollectorWrapper implements Collector {
    private final MultiCollector delegate;
    private final Collection<Collector> collectors;

    MultiCollectorWrapper(MultiCollector delegate, Collection<Collector> collectors) {
        this.delegate = delegate;
        this.collectors = collectors;
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
        return delegate.getLeafCollector(context);
    }

    @Override
    public ScoreMode scoreMode() {
        return delegate.scoreMode();
    }

    public Collection<Collector> getCollectors() {
        return collectors;
    }

    public static Collector wrap(Collector... collectors) {
        final List<Collector> collectorsList = Arrays.asList(collectors);
        final Collector collector = MultiCollector.wrap(collectorsList);
        if (collector instanceof MultiCollector) {
            return new MultiCollectorWrapper((MultiCollector) collector, collectorsList);
        } else {
            return collector;
        }
    }
}
