/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.apache.lucene.index;

/**
 * Utility class which helps overriding index writer configuration which can be overridden by default package.
 *
 */
public class IndexWriterUtil {
    public static void suppressMergePolicyException(MergeScheduler mergeScheduler) {
        if (mergeScheduler instanceof ConcurrentMergeScheduler) {
            // This test intentionally produces exceptions
            // in the threads that CMS launches; we don't
            // want to pollute test output with these.
            ((ConcurrentMergeScheduler) mergeScheduler).setSuppressExceptions();
        }
    }
}
