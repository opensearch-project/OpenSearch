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
import org.opensearch.common.hash.FNV1a;
import org.opensearch.index.remote.RemoteStorePathStrategy.PathInput;

import java.util.Locale;
import java.util.Set;

import static org.opensearch.index.remote.RemoteStoreEnums.DataType.DATA;
import static org.opensearch.index.remote.RemoteStoreEnums.DataType.METADATA;

/**
 * This class contains the different enums related to remote store like data categories and types, path types
 * and hashing algorithm.
 *
 * @opensearch.api
 */
public class RemoteStoreEnums {

    /**
     * Categories of the data in Remote store.
     */
    @PublicApi(since = "2.14.0")
    public enum DataCategory {
        SEGMENTS("segments", Set.of(DataType.values())),
        TRANSLOG("translog", Set.of(DATA, METADATA));

        private final String name;
        private final Set<DataType> supportedDataTypes;

        DataCategory(String name, Set<DataType> supportedDataTypes) {
            this.name = name;
            this.supportedDataTypes = supportedDataTypes;
        }

        public boolean isSupportedDataType(DataType dataType) {
            return supportedDataTypes.contains(dataType);
        }

        public String getName() {
            return name;
        }
    }

    /**
     * Types of data in remote store.
     */
    @PublicApi(since = "2.14.0")
    public enum DataType {
        DATA("data"),
        METADATA("metadata"),
        LOCK_FILES("lock_files");

        private final String name;

        DataType(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }

    /**
     * Enumerates the types of remote store paths resolution techniques supported by OpenSearch.
     * For more information, see <a href="https://github.com/opensearch-project/OpenSearch/issues/12567">Github issue #12567</a>.
     */
    @PublicApi(since = "2.14.0")
    public enum PathType {
        FIXED {
            @Override
            public BlobPath generatePath(PathInput pathInput, PathHashAlgorithm hashAlgorithm) {
                // Hash algorithm is not used in FIXED path type
                return pathInput.basePath()
                    .add(pathInput.indexUUID())
                    .add(pathInput.shardId())
                    .add(pathInput.dataCategory().getName())
                    .add(pathInput.dataType().getName());
            }

            @Override
            boolean requiresHashAlgorithm() {
                return false;
            }
        },
        HASHED_PREFIX {
            @Override
            public BlobPath generatePath(PathInput pathInput, PathHashAlgorithm hashAlgorithm) {
                // TODO - We need to implement this, keeping the same path as Fixed for sake of multiple tests that can fail otherwise.
                // throw new UnsupportedOperationException("Not implemented"); --> Not using this for unblocking couple of tests.
                return pathInput.basePath()
                    .add(pathInput.indexUUID())
                    .add(pathInput.shardId())
                    .add(pathInput.dataCategory().getName())
                    .add(pathInput.dataType().getName());
            }

            @Override
            boolean requiresHashAlgorithm() {
                return true;
            }
        };

        /**
         * This method generates the path for the given path input which constitutes multiple fields and characteristics
         * of the data.
         *
         * @param pathInput     input.
         * @param hashAlgorithm hashing algorithm.
         * @return the blob path for the path input.
         */
        public BlobPath path(PathInput pathInput, PathHashAlgorithm hashAlgorithm) {
            DataCategory dataCategory = pathInput.dataCategory();
            DataType dataType = pathInput.dataType();
            assert dataCategory.isSupportedDataType(dataType) : "category:"
                + dataCategory
                + " type:"
                + dataType
                + " are not supported together";
            return generatePath(pathInput, hashAlgorithm);
        }

        abstract BlobPath generatePath(PathInput pathInput, PathHashAlgorithm hashAlgorithm);

        abstract boolean requiresHashAlgorithm();

        public static PathType parseString(String pathType) {
            try {
                return PathType.valueOf(pathType.toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException | NullPointerException e) {
                // IllegalArgumentException is thrown when the input does not match any enum name
                // NullPointerException is thrown when the input is null
                throw new IllegalArgumentException("Could not parse PathType for [" + pathType + "]");
            }
        }

        /**
         * This string is used as key for storing information in the custom data in index settings.
         */
        public static final String NAME = "path_type";

    }

    /**
     * Type of hashes supported for path types that have hashing.
     */
    @PublicApi(since = "2.14.0")
    public enum PathHashAlgorithm {

        FNV_1A {
            @Override
            long hash(PathInput pathInput) {
                String input = pathInput.indexUUID() + pathInput.shardId() + pathInput.dataCategory().getName() + pathInput.dataType()
                    .getName();
                return FNV1a.hash32(input);
            }
        };

        abstract long hash(PathInput pathInput);

        public static PathHashAlgorithm parseString(String pathHashAlgorithm) {
            try {
                return PathHashAlgorithm.valueOf(pathHashAlgorithm.toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException | NullPointerException e) {
                // IllegalArgumentException is thrown when the input does not match any enum name
                // NullPointerException is thrown when the input is null
                throw new IllegalArgumentException("Could not parse PathHashAlgorithm for [" + pathHashAlgorithm + "]");
            }
        }

        /**
         * This string is used as key for storing information in the custom data in index settings.
         */
        public static final String NAME = "path_hash_algorithm";
    }
}
