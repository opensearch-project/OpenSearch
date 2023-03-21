/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.lockmanager;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.index.store.RemoteDirectory;

import java.io.IOException;
import java.util.*;

/**
 * A Lock Manager Class for Shard Level Lock {@code RemoteLockDirectory}.
 * @opensearch.internal
 */
public class FileBasedMDShardLockManager implements RemoteStoreMDShardLockManager  {

    private static final Logger logger = LogManager.getLogger(FileBasedMDShardLockManager.class);
    protected final RemoteDirectory lockDirectory;

    public FileBasedMDShardLockManager(RemoteDirectory lockDirectory) {
        this.lockDirectory = lockDirectory;
    }

    @Override
    public void acquire(RemoteStoreMDShardLockManager.ShardLockInfo lockInfo) throws IOException {
        try (IndexOutput indexOutput = lockDirectory.createOutput(lockInfo.getLockName(), IOContext.DEFAULT)) {
            lockInfo.writeLockContent(indexOutput);
        }
    }

    @Override
    public void release(String fileName) throws IOException {
        lockDirectory.deleteFile(fileName);
    }

    @Override
    public ShardLockFileInfo readLockData(String lockName) throws IOException {
        try (IndexInput indexInput = lockDirectory.openInput(lockName, IOContext.DEFAULT)) {
            return ShardLockFileInfo.getLockFileInfoFromIndexInput(indexInput);
        }
    }

    @Override
    public ShardLockFileInfo.LockInfoBuilder getLockInfoBuilder() {
        return new ShardLockFileInfo.LockInfoBuilder();
    }

    static class ShardLockFileInfo implements RemoteStoreMDShardLockManager.ShardLockInfo {
        private String metadataFile;
        private String resourceId;

        public String getResourceId() {
            return resourceId;
        }

        public String getMetadataFile() {
            return metadataFile;
        }

        private void setMetadataFile(String metadataFile) {
            this.metadataFile = metadataFile;
        }

        private void setResourceId(String resourceId) {
            this.resourceId = resourceId;
        }

        static String getResourceIDfromLockFile(String lockFile) {
            return lockFile.split(RemoteStoreLockManagerUtils.SEPARATOR)[1].replace(RemoteStoreLockManagerUtils.LOCK_FILE_EXTENSION, "");
        }

        static Map<String, String> getLockData(String metadataFile, String resourceId) {
            Map<String, String> lockFileData = new HashMap<>();
            lockFileData.put(RemoteStoreLockManagerUtils.RESOURCE_ID, resourceId);
            lockFileData.put(RemoteStoreLockManagerUtils.METADATA_FILE_NAME, metadataFile);
            return lockFileData;
        }

        static ShardLockFileInfo getLockFileInfoFromIndexInput(IndexInput indexInput) throws IOException {
            Map<String, String> lockData = indexInput.readMapOfStrings();
            ShardLockFileInfo shardLockFileInfo = new ShardLockFileInfo();
            shardLockFileInfo.setMetadataFile(lockData.get(RemoteStoreLockManagerUtils.METADATA_FILE_NAME));
            shardLockFileInfo.setResourceId(lockData.get(RemoteStoreLockManagerUtils.RESOURCE_ID));
            return shardLockFileInfo;
        }

        static String generateLockFileName(String mdFile, String resourceID) {
            return String.join(RemoteStoreLockManagerUtils.SEPARATOR, mdFile, resourceID)
                + RemoteStoreLockManagerUtils.LOCK_FILE_EXTENSION;
        }

        @Override
        public String getLockName() {
            return generateLockFileName(metadataFile, resourceId);
        }

        public String getMetadataFileNameFromLockFile(String lockFile) {
            return lockFile.split(RemoteStoreLockManagerUtils.SEPARATOR)[0];
        }

        @Override
        public void writeLockContent(IndexOutput indexOutput) throws IOException {
            indexOutput.writeMapOfStrings(getLockData(this.metadataFile, this.resourceId));
        }

        static class LockInfoBuilder implements RemoteStoreMDShardLockManager.ShardLockInfo.LockInfoBuilder {
            private final ShardLockFileInfo lockFileInfo;
            LockInfoBuilder() {
                this.lockFileInfo = new ShardLockFileInfo();
            }

            @Override
            public LockInfoBuilder withMetadataFile(String metadataFile) {
                lockFileInfo.setMetadataFile(metadataFile);
                return this;
            }

            @Override
            public LockInfoBuilder withResourceId(String resourceId) {
                lockFileInfo.setResourceId(resourceId);
                return this;
            }

            public ShardLockFileInfo build() {
                return lockFileInfo;
            }

        }

    }
}
