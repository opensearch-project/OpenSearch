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
import org.opensearch.repositories.blobstore.BlobStoreRepository;

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
     * Wrapper class for the path input required to generate path for remote store uploads. This input is composed of
     * basePath and indexUUID.
     *
     * @opensearch.internal
     */
    @PublicApi(since = "2.14.0")
    @ExperimentalApi
    public static class PathInput {
        private final BlobPath basePath;
        private final String indexUUID;
        private final String fixedPrefix;

        public PathInput(Builder<?> builder) {
            this.basePath = Objects.requireNonNull(builder.basePath);
            this.indexUUID = Objects.requireNonNull(builder.indexUUID);
            this.fixedPrefix = Objects.isNull(builder.fixedPrefix) ? "" : builder.fixedPrefix;
        }

        BlobPath basePath() {
            return basePath;
        }

        String indexUUID() {
            return indexUUID;
        }

        String fixedPrefix() {
            return fixedPrefix;
        }

        BlobPath fixedSubPath() {
            return BlobPath.cleanPath().add(indexUUID);
        }

        BlobPath hashPath() {
            return fixedSubPath();
        }

        /**
         * Returns a new builder for {@link PathInput}.
         */
        public static Builder<?> builder() {
            return new Builder<>();
        }

        public void assertIsValid() {
            // Input is always valid here.
        }

        /**
         * Builder for {@link PathInput}.
         *
         * @opensearch.internal
         */
        @PublicApi(since = "2.14.0")
        @ExperimentalApi
        public static class Builder<T extends Builder<T>> {
            private BlobPath basePath;
            private String indexUUID;
            private String fixedPrefix;

            public T basePath(BlobPath basePath) {
                this.basePath = basePath;
                return self();
            }

            public T indexUUID(String indexUUID) {
                this.indexUUID = indexUUID;
                return self();
            }

            public T fixedPrefix(String fixedPrefix) {
                this.fixedPrefix = fixedPrefix;
                return self();
            }

            protected T self() {
                return (T) this;
            }

            public PathInput build() {
                return new PathInput(this);
            }
        }
    }

    /**
     * A subclass of {@link PathInput} that represents the input required to generate a path
     * for a shard in a snapshot. It includes the base path, index UUID, and shard ID.
     *
     * @opensearch.internal
     */
    public static class SnapshotShardPathInput extends PathInput {
        private final String shardId;

        public SnapshotShardPathInput(SnapshotShardPathInput.Builder builder) {
            super(builder);
            this.shardId = Objects.requireNonNull(builder.shardId);
        }

        @Override
        BlobPath fixedSubPath() {
            return BlobPath.cleanPath().add(BlobStoreRepository.INDICES_DIR).add(super.fixedSubPath()).add(shardId);
        }

        @Override
        BlobPath hashPath() {
            return BlobPath.cleanPath().add(shardId).add(indexUUID());
        }

        public String shardId() {
            return shardId;
        }

        /**
         * Returns a new builder for {@link SnapshotShardPathInput}.
         */
        public static SnapshotShardPathInput.Builder builder() {
            return new SnapshotShardPathInput.Builder();
        }

        /**
         * Builder for {@link SnapshotShardPathInput}.
         *
         * @opensearch.internal
         */
        public static class Builder extends PathInput.Builder<SnapshotShardPathInput.Builder> {
            private String shardId;

            public SnapshotShardPathInput.Builder shardId(String shardId) {
                this.shardId = shardId;
                return this;
            }

            @Override
            protected SnapshotShardPathInput.Builder self() {
                return this;
            }

            public SnapshotShardPathInput build() {
                return new SnapshotShardPathInput(this);
            }
        }
    }

    /**
     * Wrapper class for the data aware path input required to generate path for remote store uploads. This input is
     * composed of the parent inputs, shard id, data category and data type.
     *
     * @opensearch.internal
     */
    @PublicApi(since = "2.14.0")
    @ExperimentalApi
    public static class ShardDataPathInput extends PathInput {
        private final String shardId;
        private final DataCategory dataCategory;
        private final DataType dataType;

        public ShardDataPathInput(Builder builder) {
            super(builder);
            this.shardId = Objects.requireNonNull(builder.shardId);
            this.dataCategory = Objects.requireNonNull(builder.dataCategory);
            this.dataType = Objects.requireNonNull(builder.dataType);
            assert dataCategory.isSupportedDataType(dataType) : "category:"
                + dataCategory
                + " type:"
                + dataType
                + " are not supported together";

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

        @Override
        BlobPath fixedSubPath() {
            return super.fixedSubPath().add(shardId).add(dataCategory.getName()).add(dataType.getName());
        }

        /**
         * Returns a new builder for {@link ShardDataPathInput}.
         */
        public static Builder builder() {
            return new Builder();
        }

        /**
         * Builder for {@link ShardDataPathInput}.
         *
         * @opensearch.internal
         */
        @PublicApi(since = "2.14.0")
        @ExperimentalApi
        public static class Builder extends PathInput.Builder<Builder> {
            private String shardId;
            private DataCategory dataCategory;
            private DataType dataType;

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

            @Override
            protected Builder self() {
                return this;
            }

            public ShardDataPathInput build() {
                return new ShardDataPathInput(this);
            }
        }
    }

}
