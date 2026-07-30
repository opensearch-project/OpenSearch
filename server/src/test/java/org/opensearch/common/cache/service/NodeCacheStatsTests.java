/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.service;

import org.opensearch.action.admin.indices.stats.CommonStatsFlags;
import org.opensearch.common.cache.CacheType;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.TreeMap;

public class NodeCacheStatsTests extends OpenSearchTestCase {

    public void testSerializationRoundTrip() throws IOException {
        CommonStatsFlags flags = new CommonStatsFlags().clear();
        flags.includeCacheType(CacheType.INDICES_REQUEST_CACHE);
        NodeCacheStats original = new NodeCacheStats(new TreeMap<>(), flags);
        NodeCacheStats deserialized = copyWriteable(original, writableRegistry(), NodeCacheStats::new);
        assertEquals(
            original.getStatsByCache(CacheType.INDICES_REQUEST_CACHE),
            deserialized.getStatsByCache(CacheType.INDICES_REQUEST_CACHE)
        );
    }
}
