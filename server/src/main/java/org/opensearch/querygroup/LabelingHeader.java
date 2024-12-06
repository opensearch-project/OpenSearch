/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.querygroup;

/**
 * Main enum define various headers introduced by {@link org.opensearch.plugins.LabelingPlugin}s
 */
public enum LabelingHeader {
    QUERY_GROUP_ID("queryGroupId");

    private final String name;

    private LabelingHeader(final String name) {
        this.name = name;
    }

    public static LabelingHeader fromName(String name) {
        for (final LabelingHeader header : values()) {
            if (header.getName().equals(name)) {
                return header;
            }
        }
        throw new IllegalArgumentException(name + " is not a valid [LabelingHeader]");
    }

    private String getName() {
        return name;
    }
}
