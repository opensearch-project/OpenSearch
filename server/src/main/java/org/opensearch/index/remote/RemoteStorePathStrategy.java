/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.remote;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.index.remote.RemoteStoreEnums.DataCategory;
import org.opensearch.index.remote.RemoteStoreEnums.DataType;
import org.opensearch.index.remote.RemoteStoreEnums.PathHashAlgorithm;
import org.opensearch.index.remote.RemoteStoreEnums.PathType;

import java.util.Objects;

/**
 * This class wraps internal details on the remote store path for an index.
 *
 * @opensearch.internal
 */
@PublicApi(since = "2.14.0")
@ExperimentalApi
public class RemoteStorePathStrategy {

    private final PathType type;

    @Nullable
    private final PathHashAlgorithm hashAlgorithm;

    public RemoteStorePathStrategy(PathType type) {
        this(type, null);
    }

    public RemoteStorePathStrategy(PathType type, PathHashAlgorithm hashAlgorithm) {
        Objects.requireNonNull(type, "pathType can not be null");
        if (isCompatible(type, hashAlgorithm) == false) {
            throw new IllegalArgumentException(
                new ParameterizedMessage("pathType={} pathHashAlgorithm={} are incompatible", type, hashAlgorithm).getFormattedMessage()
            );
        }
        this.type = type;
        this.hashAlgorithm = hashAlgorithm;
    }

    public static boolean isCompatible(PathType type, PathHashAlgorithm hashAlgorithm) {
        return (type.requiresHashAlgorithm() == false && Objects.isNull(hashAlgorithm))
            || (type.requiresHashAlgorithm() && Objects.nonNull(hashAlgorithm));
    }

    public PathType getType() {
        return type;
    }

    public PathHashAlgorithm getHashAlgorithm() {
        return hashAlgorithm;
    }

    @Override
    public String toString() {
        return "RemoteStorePathStrategy{" + "type=" + type + ", hashAlgorithm=" + hashAlgorithm + '}';
    }

    public BlobPath generatePath(PathInput pathInput) {
        return type.path(pathInput, hashAlgorithm);
    }

    /**
     * Wrapper class for the input required to generate path for remote store uploads.
     * @opensearch.internal
     */
    @PublicApi(since = "2.14.0")
    @ExperimentalApi
    public static class PathInput {
        private final BlobPath basePath;
        private final String indexUUID;
        private final String shardId;
        private final DataCategory dataCategory;
        private final DataType dataType;

        public PathInput(BlobPath basePath, String indexUUID, String shardId, DataCategory dataCategory, DataType dataType) {
            this.basePath = Objects.requireNonNull(basePath);
            this.indexUUID = Objects.requireNonNull(indexUUID);
            this.shardId = Objects.requireNonNull(shardId);
            this.dataCategory = Objects.requireNonNull(dataCategory);
            this.dataType = Objects.requireNonNull(dataType);
        }

        BlobPath basePath() {
            return basePath;
        }

        String indexUUID() {
            return indexUUID;
        }

        String shardId() {
            return shardId;
        }

        DataCategory dataCategory() {
            return dataCategory;
        }

        DataType dataType() {
            return dataType;
        }

        /**
         * Returns a new builder for {@link PathInput}.
         */
        public static Builder builder() {
            return new Builder();
        }

        /**
         * Builder for {@link PathInput}.
         *
         * @opensearch.internal
         */
        @PublicApi(since = "2.14.0")
        @ExperimentalApi
        public static class Builder {
            private BlobPath basePath;
            private String indexUUID;
            private String shardId;
            private DataCategory dataCategory;
            private DataType dataType;

            public Builder basePath(BlobPath basePath) {
                this.basePath = basePath;
                return this;
            }

            public Builder indexUUID(String indexUUID) {
                this.indexUUID = indexUUID;
                return this;
            }

            public Builder shardId(String shardId) {
                this.shardId = shardId;
                return this;
            }

            public Builder dataCategory(DataCategory dataCategory) {
                this.dataCategory = dataCategory;
                return this;
            }

            public Builder dataType(DataType dataType) {
                this.dataType = dataType;
                return this;
            }

            public PathInput build() {
                return new PathInput(basePath, indexUUID, shardId, dataCategory, dataType);
            }
        }
    }

}
