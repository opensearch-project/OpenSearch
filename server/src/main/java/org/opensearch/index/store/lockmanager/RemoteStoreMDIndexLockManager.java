/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.lockmanager;

import org.apache.lucene.store.IndexOutput;

import java.io.IOException;

/**
 * An Interface that defines Index Level Remote Store MD Lock Manager.
 * @opensearch.internal
 */
public interface RemoteStoreMDIndexLockManager {
    /**
     * Method to acquire index level lock.
     */
    public void acquire(IndexLockInfo lockInfo) throws IOException;
    /**
     * Method to release index level lock.
     */
    public void release(String lockName) throws IOException;
    /**
     * Method to get Index Lock Info Builder.
     */
    IndexLockInfo.LockInfoBuilder getLockInfoBuilder();
    /**
     * Method to read index level lock data.
     */
    public IndexLockInfo readLockData(String lockName) throws IOException;
    /**
     * An Interface that defines the info needed to be present in a Index Level Lock.
     */
    public interface IndexLockInfo {
        /**
         * Index Level Lock Name.
         */
        public String getLockName();
        /**
         * Method to write data in lock.
         */
        public void writeLockContent(IndexOutput indexOutput) throws IOException;
        /**
         * An Interface that defines a Lock Info Builder
         */
        public static interface LockInfoBuilder {
            /**
             * Method to set TTL in Index Level Lock.
             */
            public LockInfoBuilder withTTL(String ttl);
            /**
             * Method to set Resource Id in Index Level Lock.
             */
            public LockInfoBuilder withResourceId(String resourceId);
            /**
             * Method to build Lock Info Instance.
             */
            public IndexLockInfo build();
        }

    }
}
