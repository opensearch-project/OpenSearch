/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

import java.util.Arrays;

public enum SpanExporterType {

    LOGGING, OLTP_GRPC;

    public static SpanExporterType fromString(String name) {
        for (SpanExporterType level : values()) {
            if (level.name().equalsIgnoreCase(name)) {
                return level;
            }
        }
        throw new IllegalArgumentException(
            "invalid value for tracing level [" + name + "], " + "must be in " + Arrays.asList(SpanExporterType.values())
        );
    }
}
