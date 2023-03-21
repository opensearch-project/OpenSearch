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
import org.apache.lucene.store.Lock;
import org.opensearch.common.Nullable;
import org.opensearch.index.store.RemoteDirectory;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * A Lock Manager Class for Index Level Lock
 * @opensearch.internal
 */
public class FileBasedMDIndexLockManager implements RemoteStoreMDIndexLockManager {
    private static final Logger logger = LogManager.getLogger(RemoteStoreMDIndexLockManager.class);
    private final RemoteDirectory lockDirectory;
    public FileBasedMDIndexLockManager(RemoteDirectory lockDirectory) {
        this.lockDirectory = lockDirectory;
    }

    @Override
    public void acquire(RemoteStoreMDIndexLockManager.IndexLockInfo lockInfo) throws IOException {
        try (IndexOutput indexOutput = lockDirectory.createOutput(lockInfo.getLockName(), IOContext.DEFAULT)) {
            lockInfo.writeLockContent(indexOutput);
        }
    }

    @Override
    public void release(String fileName) throws IOException {
        lockDirectory.deleteFile(fileName);
    }

    @Override
    public IndexLockFileInfo readLockData(String lockName) throws IOException {
        try (IndexInput indexInput = lockDirectory.openInput(lockName, IOContext.DEFAULT)) {
            return IndexLockFileInfo.getLockFileInfoFromIndexInput(indexInput);
        }
    }

    @Override
    public IndexLockInfo.LockInfoBuilder getLockInfoBuilder() {
        return new IndexLockFileInfo.LockInfoBuilder();
    }

    public Boolean isLockExpiredForResource(String resourceId) throws IOException {
        IndexLockFileInfo lockFileData = readLockData(IndexLockFileInfo.generateLockFileName(resourceId));
        if (lockFileData.getExpiryTime() != null) {
            Instant currentInstantInUTC = Instant.now().atZone(ZoneOffset.UTC).toInstant();
            return Instant.parse(lockFileData.getExpiryTime()).compareTo(currentInstantInUTC) > 0;
        }
        return false;
    }

    static class IndexLockFileInfo implements RemoteStoreMDIndexLockManager.IndexLockInfo {
        private String resourceId;
        private String expiryTime = null;

        private void setResourceId(String resourceId) {
            this.resourceId = resourceId;
        }

        private void setExpiryTimeFromTTL(String ttl) {
            if (!Objects.equals(ttl, RemoteStoreLockManagerUtils.NO_TTL)) {
                setExpiryTime(Instant.now().atZone(ZoneOffset.UTC).plusDays(Long.parseLong(ttl)).toInstant().toString());
            }
        }

        private void setExpiryTime(String expiryTime) {
            this.expiryTime = expiryTime;
        }

        public String getExpiryTime() {
            return this.expiryTime;
        }

        public String getResourceId() {
            return resourceId;
        }

        static IndexLockFileInfo getLockFileInfoFromIndexInput(IndexInput indexInput) throws IOException {
            Map<String, String> lockData = indexInput.readMapOfStrings();
            IndexLockFileInfo indexLockFileInfo = new IndexLockFileInfo();
            indexLockFileInfo.setResourceId(lockData.get(RemoteStoreLockManagerUtils.RESOURCE_ID));
            indexLockFileInfo.setExpiryTime(lockData.get(RemoteStoreLockManagerUtils.LOCK_EXPIRY_TIME));
            return indexLockFileInfo;
        }

        static String generateLockFileName(String resourceID) {
            return resourceID + RemoteStoreLockManagerUtils.LOCK_FILE_EXTENSION;
        }

        static String getResourceIDFromLockFile(String lockFile) {
            return lockFile.replace(RemoteStoreLockManagerUtils.LOCK_FILE_EXTENSION, "");
        }
        static Map<String, String> getLockData(String resourceID, String expiry_time) {
            Map<String, String> lockFileData = new HashMap<>();
            lockFileData.put(RemoteStoreLockManagerUtils.RESOURCE_ID, resourceID);
            if (expiry_time != null) {
                lockFileData.put(RemoteStoreLockManagerUtils.LOCK_EXPIRY_TIME, expiry_time);
            }
            return lockFileData;
        }
        @Override
        public String getLockName() {
            return generateLockFileName(this.resourceId);
        }

        @Override
        public void writeLockContent(IndexOutput indexOutput) throws IOException {
            indexOutput.writeMapOfStrings(getLockData(resourceId, expiryTime));
        }
        static class LockInfoBuilder implements RemoteStoreMDIndexLockManager.IndexLockInfo.LockInfoBuilder {
            private final IndexLockFileInfo lockFileInfo;

            LockInfoBuilder() {
                this.lockFileInfo = new IndexLockFileInfo();
            }
            @Override
            public LockInfoBuilder withTTL(String ttl) {
                lockFileInfo.setExpiryTimeFromTTL(ttl);
                return this;
            }

            @Override
            public LockInfoBuilder withResourceId(String resourceId) {
                lockFileInfo.setResourceId(resourceId);
                return this;
            }

            @Override
            public IndexLockFileInfo build() {
                return lockFileInfo;
            }
        }
    }
}
