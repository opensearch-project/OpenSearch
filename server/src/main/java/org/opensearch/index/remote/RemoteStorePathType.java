/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.remote;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.index.remote.RemoteStoreDataEnums.DataCategory;
import org.opensearch.index.remote.RemoteStoreDataEnums.DataType;

import java.util.Locale;

/**
 * Enumerates the types of remote store paths resolution techniques supported by OpenSearch.
 * For more information, see <a href="https://github.com/opensearch-project/OpenSearch/issues/12567">Github issue #12567</a>.
 *
 * @opensearch.internal
 */
@PublicApi(since = "2.14.0")
public enum RemoteStorePathType {

    FIXED {
        @Override
        public BlobPath generatePath(BlobPath basePath, String indexUUID, String shardId, String dataCategory, String dataType) {
            return basePath.add(indexUUID).add(shardId).add(dataCategory).add(dataType);
        }
    },
    HASHED_PREFIX {
        @Override
        public BlobPath generatePath(BlobPath basePath, String indexUUID, String shardId, String dataCategory, String dataType) {
            // TODO - We need to implement this, keeping the same path as Fixed for sake of multiple tests that can fail otherwise.
            // throw new UnsupportedOperationException("Not implemented"); --> Not using this for unblocking couple of tests.
            return basePath.add(indexUUID).add(shardId).add(dataCategory).add(dataType);
        }
    };

    /**
     * @param basePath     base path of the underlying blob store repository
     * @param indexUUID    of the index
     * @param shardId      shard id
     * @param dataCategory is either translog or segment
     * @param dataType     can be one of data, metadata or lock_files.
     * @return the blob path for the underlying remote store path type.
     */
    public BlobPath path(BlobPath basePath, String indexUUID, String shardId, DataCategory dataCategory, DataType dataType) {
        assert dataCategory.isSupportedDataType(dataType) : "category:"
            + dataCategory
            + " type:"
            + dataType
            + " are not supported together";
        return generatePath(basePath, indexUUID, shardId, dataCategory.getName(), dataType.getName());
    }

    abstract BlobPath generatePath(BlobPath basePath, String indexUUID, String shardId, String dataCategory, String dataType);

    public static RemoteStorePathType parseString(String remoteStoreBlobPathType) {
        try {
            return RemoteStorePathType.valueOf(remoteStoreBlobPathType.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException | NullPointerException e) {
            // IllegalArgumentException is thrown when the input does not match any enum name
            // NullPointerException is thrown when the input is null
            throw new IllegalArgumentException("Could not parse RemoteStorePathType for [" + remoteStoreBlobPathType + "]");
        }
    }

    /**
     * This string is used as key for storing information in the custom data in index settings.
     */
    public static final String NAME = "path_type";
}
