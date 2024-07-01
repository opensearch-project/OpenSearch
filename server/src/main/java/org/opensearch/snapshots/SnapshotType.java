/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.snapshots;

import org.opensearch.common.annotation.PublicApi;

@PublicApi(since = "2.15.0")
public enum SnapshotType {
    FULL_COPY("full_copy"),
    SHALLOW_COPY("shallow_copy");

    private final String text;

    SnapshotType(String text) {
        this.text = text;
    }

    @Override
    public String toString() {
        return text;
    }

    public static SnapshotType fromString(String string) {
        for (SnapshotType type : values()) {
            if (type.text.equals(string)) {
                return type;
            }
        }
        throw new IllegalArgumentException("Invalid snapshot_type: " + string);
    }
}
