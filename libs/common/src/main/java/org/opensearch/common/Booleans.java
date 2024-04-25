/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.common;

/**
 * Extended Boolean functionality
 *
 * @opensearch.internal
 */
public final class Booleans {
    private Booleans() {
        throw new AssertionError("No instances intended");
    }

    /**
     * Parses a char[] representation of a boolean value to <code>boolean</code>.
     *
     * @return <code>true</code> iff the sequence of chars is "true", <code>false</code> iff the sequence of
     * chars is "false" or the provided default value iff either text is <code>null</code> or length == 0.
     * @throws IllegalArgumentException if the string cannot be parsed to boolean.
     */
    public static boolean parseBoolean(char[] text, int offset, int length, boolean defaultValue) {
        if (text == null) {
            return defaultValue;
        }

        switch (length) {
            case 0:
                return defaultValue;
            case 1:
            case 2:
            case 3:
            default:
                break;
            case 4:
                if (text[offset] == 't' && text[offset + 1] == 'r' && text[offset + 2] == 'u' && text[offset + 3] == 'e') {
                    return true;
                }
                break;
            case 5:
                if (text[offset] == 'f'
                    && text[offset + 1] == 'a'
                    && text[offset + 2] == 'l'
                    && text[offset + 3] == 's'
                    && text[offset + 4] == 'e') {
                    return false;
                }
                break;
        }

        throw new IllegalArgumentException(
            "Failed to parse value [" + new String(text, offset, length) + "] as only [true] or [false] are allowed."
        );
    }

    /**
     * Returns true iff the sequence of chars is one of "true", "false".
     *
     * @param text   sequence to check
     * @param offset offset to start
     * @param length length to check
     */
    public static boolean isBoolean(char[] text, int offset, int length) {
        if (text == null) {
            return false;
        }

        switch (length) {
            case 0:
            case 1:
            case 2:
            case 3:
            default:
                return false;
            case 4:
                return text[offset] == 't' && text[offset + 1] == 'r' && text[offset + 2] == 'u' && text[offset + 3] == 'e';
            case 5:
                return text[offset] == 'f'
                    && text[offset + 1] == 'a'
                    && text[offset + 2] == 'l'
                    && text[offset + 3] == 's'
                    && text[offset + 4] == 'e';
        }
    }

    public static boolean isBoolean(String value) {
        return isFalse(value) || isTrue(value);
    }

    /**
     * Parses a string representation of a boolean value to <code>boolean</code>.
     *
     * @return <code>true</code> iff the provided value is "true". <code>false</code> iff the provided value is "false".
     * @throws IllegalArgumentException if the string cannot be parsed to boolean.
     */
    public static boolean parseBoolean(String value) {
        if (isFalse(value)) {
            return false;
        }
        if (isTrue(value)) {
            return true;
        }
        throw new IllegalArgumentException("Failed to parse value [" + value + "] as only [true] or [false] are allowed.");
    }

    /**
     * Parses a string representation of a boolean value to <code>boolean</code>.
     * Note the subtle difference between this and {@link #parseBoolean(char[], int, int, boolean)}; this returns the
     * default value even when the value is non-zero length containing all whitespaces (possibly overlooked, but
     * preserving this behavior for compatibility reasons). Use {@link #parseBooleanStrict(String, boolean)} instead.
     *
     * @param value text to parse.
     * @param defaultValue The default value to return if the provided value is <code>null</code> or blank.
     * @return see {@link #parseBoolean(String)}
     */
    @Deprecated
    public static boolean parseBoolean(String value, boolean defaultValue) {
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        return parseBoolean(value);
    }

    @Deprecated
    public static Boolean parseBoolean(String value, Boolean defaultValue) {
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        return parseBoolean(value);
    }

    /**
     * Parses a string representation of a boolean value to <code>boolean</code>.
     * Analogous to {@link #parseBoolean(char[], int, int, boolean)}.
     *
     * @return <code>true</code> iff the sequence of chars is "true", <code>false</code> iff the sequence of
     * chars is "false", or the provided default value iff either text is <code>null</code> or length == 0.
     * @throws IllegalArgumentException if the string cannot be parsed to boolean.
     */
    public static boolean parseBooleanStrict(String value, boolean defaultValue) {
        if (value == null || value.length() == 0) {
            return defaultValue;
        }
        return parseBoolean(value);
    }

    /**
     * @return {@code true} iff the value is "false", otherwise {@code false}.
     */
    public static boolean isFalse(String value) {
        return "false".equals(value);
    }

    /**
     * @return {@code true} iff the value is "true", otherwise {@code false}.
     */
    public static boolean isTrue(String value) {
        return "true".equals(value);
    }
}
