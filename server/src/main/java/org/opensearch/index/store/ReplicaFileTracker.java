/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * This class is a version of Lucene's ReplicaFileDeleter class used to keep track of
 * segment files that should be preserved on replicas between replication events.
 * The difference is this component does not actually perform any deletions, it only handles refcounts.
 * Our deletions are made through Store.java.
 *
 * https://github.com/apache/lucene/blob/main/lucene/replicator/src/java/org/apache/lucene/replicator/nrt/ReplicaFileDeleter.java
 *
 * @opensearch.internal
 */
final class ReplicaFileTracker {

    private final Map<String, Integer> refCounts = new HashMap<>();

    public synchronized void incRef(Collection<String> fileNames) {
        for (String fileName : fileNames) {
            refCounts.merge(fileName, 1, Integer::sum);
        }
    }

    public synchronized void decRef(Collection<String> fileNames) {
        for (String fileName : fileNames) {
            Integer curCount = refCounts.get(fileName);
            assert curCount != null : "fileName=" + fileName;
            assert curCount > 0;
            if (curCount == 1) {
                refCounts.remove(fileName);
            } else {
                refCounts.put(fileName, curCount - 1);
            }
        }
    }

    public synchronized boolean canDelete(String fileName) {
        return refCounts.containsKey(fileName) == false;
    }
}
