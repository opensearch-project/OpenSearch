/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.remote;

import org.opensearch.common.annotation.PublicApi;

import java.util.Set;

import static org.opensearch.index.remote.RemoteStoreDataEnums.DataType.DATA;
import static org.opensearch.index.remote.RemoteStoreDataEnums.DataType.METADATA;

/**
 * This class contains the different enums related to remote store data categories and types.
 *
 * @opensearch.api
 */
public class RemoteStoreDataEnums {

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
}
