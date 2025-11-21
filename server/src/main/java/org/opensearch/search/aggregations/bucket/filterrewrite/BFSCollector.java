/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.filterrewrite;

import org.apache.lucene.index.LeafReaderContext;
import org.opensearch.search.aggregations.LeafBucketCollector;

import java.io.IOException;

/**
 * Workaround for collectors that cannot handle DFS travel, i.e. changing owningBucketOrd)
 *
 * @opensearch.internal
 */
public interface BFSCollector {

    LeafBucketCollector getBFSLeafCollector(LeafReaderContext ctx) throws IOException;
}
