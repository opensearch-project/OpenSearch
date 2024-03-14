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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

/*
 * This code is based on code from SFL4J 1.5.11
 * Copyright (c) 2004-2007 QOS.ch
 * All rights reserved.
 * SPDX-License-Identifier: MIT
 */

package org.opensearch.core.common.logging;

import java.util.HashSet;
import java.util.Set;

/**
 * Format string for OpenSearch log messages.
 * <p>
 * This class is almost a copy of {@code org.slf4j.helpers.MessageFormatter}<p>
 * The original code is licensed under the MIT License and is available at :
 * <a href="https://github.com/qos-ch/slf4j/blob/7c164fab8d54f823dd55c01a5a839c153f578297/slf4j-api/src/main/java/org/slf4j/helpers/MessageFormatter.java">MessageFormatter.java</a>
 *
 * @opensearch.internal
 */
public class LoggerMessageFormat {

    static final char DELIM_START = '{';
    static final char DELIM_STOP = '}';
    static final String DELIM_STR = "{}";
    private static final char ESCAPE_CHAR = '\\';

    public static String format(final String messagePattern, final Object... argArray) {
        return format(null, messagePattern, argArray);
    }

    /**
     * (this is almost a copy of {@code org.slf4j.helpers.MessageFormatter.arrayFormat})
     *
     * @param prefix the prefix to prepend to the formatted message (can be null)
     * @param messagePattern the message pattern which will be parsed and formatted
     * @param argArray an array of arguments to be substituted in place of formatting anchors
     * @return null if messagePattern is null <p>
     *         messagePattern if argArray is (null or empty) and prefix is null <p>
     *         prefix + messagePattern if argArray is (null or empty) and prefix is not null <p>
     *         formatted message otherwise (even if prefix is null)
     */
    public static String format(final String prefix, final String messagePattern, final Object... argArray) {
        if (messagePattern == null) {
            return null;
        }
        if (argArray == null || argArray.length == 0) {
            if (prefix == null) {
                return messagePattern;
            } else {
                return prefix + messagePattern;
            }
        }
        int i = 0;
        int j;
        final StringBuilder sbuf = new StringBuilder(messagePattern.length() + 50);
        if (prefix != null) {
            sbuf.append(prefix);
        }

        for (int L = 0; L < argArray.length; L++) {

            j = messagePattern.indexOf(DELIM_STR, i);

            if (j == -1) {
                // no more variables
                if (i == 0) { // this is a simple string
                    return messagePattern;
                } else { // add the tail string which contains no variables and return
                    // the result.
                    sbuf.append(messagePattern.substring(i));
                    return sbuf.toString();
                }
            } else {
                if (isEscapedDelimiter(messagePattern, j)) {
                    if (!isDoubleEscaped(messagePattern, j)) {
                        L--; // DELIM_START was escaped, thus should not be incremented
                        sbuf.append(messagePattern, i, j - 1);
                        sbuf.append(DELIM_START);
                        i = j + 1;
                    } else {
                        // The escape character preceding the delimiter start is
                        // itself escaped: "abc x:\\{}"
                        // we have to consume one backward slash
                        sbuf.append(messagePattern, i, j - 1);
                        deeplyAppendParameter(sbuf, argArray[L], new HashSet<>());
                        i = j + 2;
                    }
                } else {
                    // normal case
                    sbuf.append(messagePattern, i, j);
                    deeplyAppendParameter(sbuf, argArray[L], new HashSet<>());
                    i = j + 2;
                }
            }
        }
        // append the characters following the last {} pair.
        sbuf.append(messagePattern.substring(i));
        return sbuf.toString();
    }

    /**
     * Checks if (delimterStartIndex - 1) in messagePattern is an escape character.
     * @param messagePattern the message pattern
     * @param delimiterStartIndex the index of the character to check
     * @return true if there is an escape char before the character at delimiterStartIndex.<p>
     *         Always returns false if delimiterStartIndex == 0 (edge case)
     */
    static boolean isEscapedDelimiter(String messagePattern, int delimiterStartIndex) {

        if (delimiterStartIndex == 0) {
            return false;
        }
        char potentialEscape = messagePattern.charAt(delimiterStartIndex - 1);
        return potentialEscape == ESCAPE_CHAR;
    }

    /**
     * Checks if (delimterStartIndex - 2) in messagePattern is an escape character.
     * @param messagePattern the message pattern
     * @param delimiterStartIndex the index of the character to check
     * @return true if (delimterStartIndex - 2) in messagePattern is an escape character.
     *         Always returns false if delimiterStartIndex is less than 2 (edge case)
     */
    static boolean isDoubleEscaped(String messagePattern, int delimiterStartIndex) {
        return delimiterStartIndex >= 2 && messagePattern.charAt(delimiterStartIndex - 2) == ESCAPE_CHAR;
    }

    private static void deeplyAppendParameter(StringBuilder sbuf, Object o, Set<Object[]> seen) {
        if (o == null) {
            sbuf.append("null");
            return;
        }
        if (!o.getClass().isArray()) {
            safeObjectAppend(sbuf, o);
        } else {
            // check for primitive array types because they
            // unfortunately cannot be cast to Object[]
            if (o instanceof boolean[]) {
                booleanArrayAppend(sbuf, (boolean[]) o);
            } else if (o instanceof byte[]) {
                byteArrayAppend(sbuf, (byte[]) o);
            } else if (o instanceof char[]) {
                charArrayAppend(sbuf, (char[]) o);
            } else if (o instanceof short[]) {
                shortArrayAppend(sbuf, (short[]) o);
            } else if (o instanceof int[]) {
                intArrayAppend(sbuf, (int[]) o);
            } else if (o instanceof long[]) {
                longArrayAppend(sbuf, (long[]) o);
            } else if (o instanceof float[]) {
                floatArrayAppend(sbuf, (float[]) o);
            } else if (o instanceof double[]) {
                doubleArrayAppend(sbuf, (double[]) o);
            } else {
                objectArrayAppend(sbuf, (Object[]) o, seen);
            }
        }
    }

    private static void safeObjectAppend(StringBuilder sbuf, Object o) {
        try {
            String oAsString = o.toString();
            sbuf.append(oAsString);
        } catch (Exception e) {
            sbuf.append("[FAILED toString()]");
        }

    }

    private static void objectArrayAppend(StringBuilder sbuf, Object[] a, Set<Object[]> seen) {
        sbuf.append('[');
        if (!seen.contains(a)) {
            seen.add(a);
            final int len = a.length;
            for (int i = 0; i < len; i++) {
                deeplyAppendParameter(sbuf, a[i], seen);
                if (i != len - 1) sbuf.append(", ");
            }
            // allow repeats in siblings
            seen.remove(a);
        } else {
            sbuf.append("...");
        }
        sbuf.append(']');
    }

    private static void booleanArrayAppend(StringBuilder sbuf, boolean[] a) {
        sbuf.append('[');
        final int len = a.length;
        for (int i = 0; i < len; i++) {
            sbuf.append(a[i]);
            if (i != len - 1) sbuf.append(", ");
        }
        sbuf.append(']');
    }

    private static void byteArrayAppend(StringBuilder sbuf, byte[] a) {
        sbuf.append('[');
        final int len = a.length;
        for (int i = 0; i < len; i++) {
            sbuf.append(a[i]);
            if (i != len - 1) sbuf.append(", ");
        }
        sbuf.append(']');
    }

    private static void charArrayAppend(StringBuilder sbuf, char[] a) {
        sbuf.append('[');
        final int len = a.length;
        for (int i = 0; i < len; i++) {
            sbuf.append(a[i]);
            if (i != len - 1) sbuf.append(", ");
        }
        sbuf.append(']');
    }

    private static void shortArrayAppend(StringBuilder sbuf, short[] a) {
        sbuf.append('[');
        final int len = a.length;
        for (int i = 0; i < len; i++) {
            sbuf.append(a[i]);
            if (i != len - 1) sbuf.append(", ");
        }
        sbuf.append(']');
    }

    private static void intArrayAppend(StringBuilder sbuf, int[] a) {
        sbuf.append('[');
        final int len = a.length;
        for (int i = 0; i < len; i++) {
            sbuf.append(a[i]);
            if (i != len - 1) sbuf.append(", ");
        }
        sbuf.append(']');
    }

    private static void longArrayAppend(StringBuilder sbuf, long[] a) {
        sbuf.append('[');
        final int len = a.length;
        for (int i = 0; i < len; i++) {
            sbuf.append(a[i]);
            if (i != len - 1) sbuf.append(", ");
        }
        sbuf.append(']');
    }

    private static void floatArrayAppend(StringBuilder sbuf, float[] a) {
        sbuf.append('[');
        final int len = a.length;
        for (int i = 0; i < len; i++) {
            sbuf.append(a[i]);
            if (i != len - 1) sbuf.append(", ");
        }
        sbuf.append(']');
    }

    private static void doubleArrayAppend(StringBuilder sbuf, double[] a) {
        sbuf.append('[');
        final int len = a.length;
        for (int i = 0; i < len; i++) {
            sbuf.append(a[i]);
            if (i != len - 1) sbuf.append(", ");
        }
        sbuf.append(']');
    }
}
