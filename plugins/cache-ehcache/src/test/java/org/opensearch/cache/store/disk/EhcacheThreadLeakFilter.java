/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cache.store.disk;

import com.carrotsearch.randomizedtesting.ThreadFilter;

/**
 * In Ehcache(as of 3.10.8), while calling remove/invalidate() on entries causes to start a daemon thread in the
 * background to clean up the stale offheap memory associated with the disk cache. And this thread is not closed even
 * after we try to close the cache or cache manager. Considering that it requires a node restart to switch between
 * different cache plugins, this shouldn't be a problem for now.
 *
 * See: https://github.com/ehcache/ehcache3/issues/3204
 */
public class EhcacheThreadLeakFilter implements ThreadFilter {

    private static final String OFFENDING_THREAD_NAME = "MappedByteBufferSource";

    @Override
    public boolean reject(Thread t) {
        return t.getName().startsWith(OFFENDING_THREAD_NAME);
    }
}
