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
 * An Interface that defines Shard Level Remote Store MD Lock Manager.
 * @opensearch.internal
 */
public interface RemoteStoreMDShardLockManager {
    /**
     * Method to acquire Shard level lock.
     */
    public void acquire(ShardLockInfo lockInfo) throws IOException;
    /**
     * Method to release Shard level lock.
     */
    public void release(String lockName) throws IOException;
    /**
     * Method to get Shard Lock Info Builder.
     */
    ShardLockInfo.LockInfoBuilder getLockInfoBuilder();
    /**
     * Method to read Shard level lock data.
     */
    public ShardLockInfo readLockData(String lockName) throws IOException;
    /**
     * An Interface that defines the info needed to be present in a Shard Level Lock.
     */
    public interface ShardLockInfo {
        /**
         * Shard Level Lock Name.
         */
        String getLockName();
        /**
         * Method to write data in lock.
         */
        public void writeLockContent(IndexOutput indexOutput) throws IOException;
        /**
         * An Interface that defines a Lock Info Builder
         */
        public static interface LockInfoBuilder {
            /**
             * Method to set Metadata File in Shard Level Lock.
             */
            public LockInfoBuilder withMetadataFile(String metadataFile);
            /**
             * Method to set Resource Id in Shard Level Lock.
             */
            public LockInfoBuilder withResourceId(String resourceId);
            /**
             * Method to build Lock Info Instance.
             */
            public ShardLockInfo build();
        }

    }
}
