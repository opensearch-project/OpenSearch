/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication.common;

public enum ReplicationType {

    DOCUMENT("document"),

    SEGMENT("segment");

    private final String value;

    ReplicationType(String replicationType) {
        this.value = replicationType;
    }

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

    @Override
    public String toString() {
        return value;
    }
}
