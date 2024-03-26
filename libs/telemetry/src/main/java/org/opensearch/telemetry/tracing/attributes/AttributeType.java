/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing.attributes;

import org.opensearch.common.annotation.PublicApi;

/**
 * Type of Attribute.
 */
@PublicApi(since = "2.14.0")
public enum AttributeType {

    /**
     * Attribute type represents the Boolean attribute.
     */
    BOOLEAN,

    /**
     * Attribute type represents the Long attribute.
     */
    LONG,

    /**
     * Attribute type represents the String attribute.
     */
    STRING,

    /**
     * Attribute type represents the Double attribute.
     */
    DOUBLE
}
