/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.search.scan;

import org.opensearch.index.shard.SearchOperationListener;
import org.opensearch.search.internal.SearchContext;

/**
 * Stops a shard scan when one of its slices fails inside the searcher but outside the scan's collector, e.g. when the query
 * throws while it scores. The failed slice frees its compute slot at once and the other slices stop. Otherwise they would
 * wait for a slot that the failed slice holds until the whole query phase ends.
 */
public final class ArrowScanSliceListener implements SearchOperationListener {

    /** Creates the listener. */
    public ArrowScanSliceListener() {}

    @Override
    public void onFailedSliceExecution(SearchContext searchContext) {
        // Runs on the failed slice's thread, which identifies the slice's collector.
        if (searchContext.getSearchExt(ArrowScanExt.NAME) instanceof ArrowScanExt ext) {
            ArrowScanCollector.sliceFailed(ext.state());
        }
    }
}
