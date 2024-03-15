/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.remote;

/**
 * Enumerates the types of remote store paths resolution techniques supported by OpenSearch.
 * For more information, see <a href="https://github.com/opensearch-project/OpenSearch/issues/12567">Github issue #12567</a>.
 *
 * @opensearch.internal
 */
public enum RemoteStoreBlobPathType {

    FIXED,
    HASHED_PREFIX;

    public static RemoteStoreBlobPathType parseString(String remoteStoreBlobPathType) {
        try {
            return RemoteStoreBlobPathType.valueOf(remoteStoreBlobPathType);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Could not parse RemoteStorePathType for [" + remoteStoreBlobPathType + "]");
        } catch (NullPointerException npe) {
            // return a default value for null input
            return FIXED;
        }
    }

    /**
     * This string is used as key for storing information in the custom data in index settings.
     */
    public static final String NAME = "path_type";
}
