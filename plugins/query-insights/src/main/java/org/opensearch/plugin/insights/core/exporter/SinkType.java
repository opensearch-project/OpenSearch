/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.exporter;

import java.util.Locale;

/**
 * Type of supported sinks
 */
public enum SinkType {
    /** debug exporter */
    DEBUG,
    /** local index exporter */
    LOCAL_INDEX;

    @Override
    public String toString() {
        return super.toString().toLowerCase(Locale.ROOT);
    }

    /**
     * Parse SinkType from String
     * @param type the String representation of the SinkType
     * @return SinkType
     */
    public static SinkType parse(final String type) {
        return valueOf(type.toUpperCase(Locale.ROOT));
    }
}
