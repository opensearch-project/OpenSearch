/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication.common;

/**
 * Enumerates the types of replication strategies supported by OpenSearch.
 * For more information, see https://github.com/opensearch-project/OpenSearch/issues/1694
 *
 * @opensearch.internal
 */
public enum ReplicationType {

    DOCUMENT,
    SEGMENT;

    public static ReplicationType parseString(String replicationType) {
        try {
            return ReplicationType.valueOf(replicationType);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Could not parse ReplicationStrategy for [" + replicationType + "]");
        } catch (NullPointerException npe) {
            // return a default value for null input
            return DOCUMENT;
        }
    }
}
