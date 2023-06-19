/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

import java.util.Arrays;

/**
 * The Level class defines a set of standard tracing levels that can be used to control tracing output.
 * The tracing Level objects are ordered and are specified by ordered integers.
 * Enabling tracing at a given level also enables tracing at all higher levels.
 *
 * Levels in descending order are
 * <ul>
 *  <li>ROOT(highest value)</li>
 *  <li>TERSE</li>
 *  <li>INFO</li>
 *  <li>DEBUG</li>
 *  <li>TRACE(lowest value)</li>
 * </ul>
 *
 */
public enum Level {
    /**
     * ROOT is a tracing level indicating top level/root spans.
     */
    ROOT((byte) 100),

    /**
     * TERSE is a tracing level for critical spans
     */
    TERSE((byte) 80),

    /**
     * INFO is a tracing level used of generic spans
     */
    INFO((byte) 60),

    /**
     * DEBUG is a tracing level used for low level spans
     */
    DEBUG((byte) 40),

    /**
     * TRACE is the lowest level span
     */
    TRACE((byte) 20);

    private final byte value;

    Level(byte value) {
        this.value = value;
    }

    /**
     * Returns a mirrored Level object that matches the given name. Throws {@link IllegalArgumentException} if no match is found
     * @param name string value
     * @return Level corresponding to the given name
     */
    public static Level fromString(String name) {
        for (Level level : values()) {
            if (level.name().equalsIgnoreCase(name)) {
                return level;
            }
        }
        throw new IllegalArgumentException(
            "invalid value for tracing level [" + name + "], " + "must be in " + Arrays.asList(Level.values())
        );
    }

    /**
     * Get the integer value for this level
     * @return integer value of the level
     */
    public int getValue() {
        return value;
    }

    /**
     * Checks if the current level's value is equal or higher than the given level
     * @param level to compare
     * @return <code>true</code> if the current level's value is equal or higher than given level's value, <code>false</code> otherwise
     */
    public boolean isHigherOrEqual(Level level) {
        if (level != null) {
            return this.value >= level.value;
        }
        return false;
    }

    /**
     * Checks if the current level's value is equal or lower than the given level
     * @param level to compare
     * @return <code>true</code> if the current level's value is equal or lower than given level's value, <code>false</code> otherwise
     */
    public boolean isLessOrEqual(Level level) {
        if (level != null) {
            return this.value <= level.value;
        }
        return false;
    }

}
