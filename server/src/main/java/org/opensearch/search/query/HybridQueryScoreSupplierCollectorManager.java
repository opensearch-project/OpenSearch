/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ScorerSupplier;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * HybridQueryScoreSupplierCollectorManager is responsible for creating {@link HybridQueryExecutorCollector} instances.
 * Useful to create {@link HybridQueryExecutorCollector} instances that build {@link ScorerSupplier} from
 * given weight.
 */
public final class HybridQueryScoreSupplierCollectorManager
    implements
        HybridQueryExecutorCollectorManager<HybridQueryExecutorCollector<LeafReaderContext, ScorerSupplier>> {

    private LeafReaderContext context;

    public HybridQueryScoreSupplierCollectorManager(LeafReaderContext context) {
        if (context == null) {
            throw new NullPointerException("context must not be null");
        }
        this.context = context;
    }

    /**
     * Creates new {@link HybridQueryExecutorCollector} instance everytime to facilitate parallel execution
     * by individual tasks
     * @return new instance of HybridQueryExecutorCollector
     */
    @Override
    public HybridQueryExecutorCollector<LeafReaderContext, ScorerSupplier> newCollector() {
        return HybridQueryExecutorCollector.newCollector(context);
    }

    /**
     * mergeScoreSuppliers will build list of scoreSupplier from given list of collectors.
     * This method should be called after HybridQueryExecutorCollector's collect method is called.
     * If collectors didn't have any result, null will be added to list.
     * This method must be called after collection is finished on all provided collectors.
     * @param collectors List of collectors which is used to perform collection in parallel
     * @return list of {@link ScorerSupplier}
     */
    public List<ScorerSupplier> mergeScoreSuppliers(List<HybridQueryExecutorCollector<LeafReaderContext, ScorerSupplier>> collectors) {
        List<ScorerSupplier> scorerSuppliers = new ArrayList<>();
        for (HybridQueryExecutorCollector<LeafReaderContext, ScorerSupplier> collector : collectors) {
            Optional<ScorerSupplier> result = collector.getResult();
            if (result.isPresent()) {
                scorerSuppliers.add(result.get());
            } else {
                scorerSuppliers.add(null);
            }
        }
        return scorerSuppliers;
    }
}
