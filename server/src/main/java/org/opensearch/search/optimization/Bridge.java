
/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.optimization;

import java.io.IOException;
import java.util.function.BiConsumer;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;

public interface Bridge {
    void setOptimizationContext(Context context);

    boolean canOptimize();

    void prepare() throws IOException;
    void prepareFromSegment(LeafReaderContext leaf) throws IOException;

    void tryOptimize(PointValues values, BiConsumer<Long, Long> incrementDocCount) throws IOException;
}
