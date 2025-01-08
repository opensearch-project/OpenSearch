/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube;

/**
 * Represents the types of dimensions supported in a data cube.
 * <p>
 * This enum defines the possible types of dimensions that can be used
 * in a data cube structure within the composite index.
 *
 * @opensearch.experimental
 */
public enum DimensionType {
    /**
     * Represents a numeric dimension type.
     * This is used for dimensions that contain numerical values.
     */
    NUMERIC,

    /**
     * Represents an unsigned long dimension type.
     * This is used for dimensions that contain numerical values of type unsigned long.
     */
    UNSIGNED_LONG,

    /**
     * Represents a date dimension type.
     * This is used for dimensions that contain date or timestamp values.
     */
    DATE,

    /**
     * Represents dimension types which uses ordinals.
     * This is used for dimensions that contain sortedSet ordinals.
     */
    ORDINAL,

    /**
     * Represents an IP dimension type.
     * This is used for dimensions that contain IP ordinals.
     */
    IP
}
