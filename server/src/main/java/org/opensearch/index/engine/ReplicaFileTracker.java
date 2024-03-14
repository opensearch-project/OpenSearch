/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

/**
 * This class is heavily influenced by Lucene's ReplicaFileDeleter class used to keep track of
 * segment files that should be preserved on replicas between replication events.
 * <p>
 * https://github.com/apache/lucene/blob/main/lucene/replicator/src/java/org/apache/lucene/replicator/nrt/ReplicaFileDeleter.java
 *
 * @opensearch.internal
 */
final class ReplicaFileTracker {

    public static final Logger logger = LogManager.getLogger(ReplicaFileTracker.class);
    private final Map<String, Integer> refCounts = new HashMap<>();
    private final Consumer<String> fileDeleter;
    private final Set<String> EXCLUDE_FILES = Set.of("write.lock");

    public ReplicaFileTracker(Consumer<String> fileDeleter) {
        this.fileDeleter = fileDeleter;
    }

    public synchronized void incRef(Collection<String> fileNames) {
        for (String fileName : fileNames) {
            refCounts.merge(fileName, 1, Integer::sum);
        }
    }

    public synchronized int refCount(String file) {
        return Optional.ofNullable(refCounts.get(file)).orElse(0);
    }

    public synchronized void decRef(Collection<String> fileNames) {
        Set<String> toDelete = new HashSet<>();
        for (String fileName : fileNames) {
            Integer curCount = refCounts.get(fileName);
            assert curCount != null : "fileName=" + fileName;
            assert curCount > 0;
            if (curCount == 1) {
                refCounts.remove(fileName);
                toDelete.add(fileName);
            } else {
                refCounts.put(fileName, curCount - 1);
            }
        }
        if (toDelete.isEmpty() == false) {
            delete(toDelete);
        }
    }

    public void deleteUnreferencedFiles(String... toDelete) {
        for (String file : toDelete) {
            if (canDelete(file)) {
                delete(file);
            }
        }
    }

    private synchronized void delete(Collection<String> toDelete) {
        for (String fileName : toDelete) {
            delete(fileName);
        }
    }

    private synchronized void delete(String fileName) {
        assert canDelete(fileName);
        fileDeleter.accept(fileName);
    }

    private synchronized boolean canDelete(String fileName) {
        return EXCLUDE_FILES.contains(fileName) == false && refCounts.containsKey(fileName) == false;
    }

}
