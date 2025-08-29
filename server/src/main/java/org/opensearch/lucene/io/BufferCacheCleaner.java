/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lucene.io;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ConcurrentHashMap;

public class BufferCacheCleaner implements Runnable {


    private final ConcurrentHashMap<String, Page> cache;
    private static final Logger logger = LogManager.getLogger(BufferCacheCleaner.class);

    public BufferCacheCleaner(ConcurrentHashMap<String, Page> cache) {
        this.cache = cache;
    }

    public void run() {
        System.gc();;
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        this.cache.entrySet().removeIf(entry -> {
            Page page = entry.getValue();
            if (page.getRefCount() == 1) {
                logger.info("Page [{}] has been cleared for file {} ", page, entry.getKey());
                return true;
            }
            return false;
        });
    }
}
