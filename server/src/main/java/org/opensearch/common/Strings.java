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

package org.opensearch.common;

import org.apache.lucene.util.BytesRefBuilder;
import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchException;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.common.util.CollectionUtils;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Set;

import static java.util.Collections.unmodifiableSet;
import static org.opensearch.common.util.set.Sets.newHashSet;

/**
 * String utility class.
 *
 * @opensearch.internal
 */
public class Strings {

    public static final String[] EMPTY_ARRAY = org.opensearch.core.common.Strings.EMPTY_ARRAY;

    // ---------------------------------------------------------------------
    // General convenience methods for working with Strings
    // ---------------------------------------------------------------------

    /**
     * Check that the given BytesReference is neither <code>null</code> nor of length 0
     * Note: Will return <code>true</code> for a BytesReference that purely consists of whitespace.
     *
     * @param bytesReference the BytesReference to check (may be <code>null</code>)
     * @return <code>true</code> if the BytesReference is not null and has length
     * @see org.opensearch.core.common.Strings#hasLength(CharSequence)
     */
    public static boolean hasLength(BytesReference bytesReference) {
        return (bytesReference != null && bytesReference.length() > 0);
    }

    /**
     * Test whether the given string matches the given substring
     * at the given index.
     *
     * @param str       the original string (or StringBuilder)
     * @param index     the index in the original string to start matching against
     * @param substring the substring to match at the given index
     */
    public static boolean substringMatch(CharSequence str, int index, CharSequence substring) {
        for (int j = 0; j < substring.length(); j++) {
            int i = index + j;
            if (i >= str.length() || str.charAt(i) != substring.charAt(j)) {
                return false;
            }
        }
        return true;
    }

    // ---------------------------------------------------------------------
    // Convenience methods for working with formatted Strings
    // ---------------------------------------------------------------------

    /**
     * Quote the given String with single quotes.
     *
     * @param str the input String (e.g. "myString")
     * @return the quoted String (e.g. "'myString'"),
     *         or <code>null</code> if the input was <code>null</code>
     */
    public static String quote(String str) {
        return (str != null ? "'" + str + "'" : null);
    }

    public static final Set<Character> INVALID_FILENAME_CHARS = unmodifiableSet(
        newHashSet('\\', '/', '*', '?', '"', '<', '>', '|', ' ', ',')
    );

    public static boolean validFileName(String fileName) {
        for (int i = 0; i < fileName.length(); i++) {
            char c = fileName.charAt(i);
            if (INVALID_FILENAME_CHARS.contains(c)) {
                return false;
            }
        }
        return true;
    }

    public static boolean validFileNameExcludingAstrix(String fileName) {
        for (int i = 0; i < fileName.length(); i++) {
            char c = fileName.charAt(i);
            if (c != '*' && INVALID_FILENAME_CHARS.contains(c)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Split a String at the first occurrence of the delimiter.
     * Does not include the delimiter in the result.
     *
     * @param toSplit   the string to split
     * @param delimiter to split the string up with
     * @return a two element array with index 0 being before the delimiter, and
     *         index 1 being after the delimiter (neither element includes the delimiter);
     *         or <code>null</code> if the delimiter wasn't found in the given input String
     */
    public static String[] split(String toSplit, String delimiter) {
        if (org.opensearch.core.common.Strings.hasLength(toSplit) == false
            || org.opensearch.core.common.Strings.hasLength(delimiter) == false) {
            return null;
        }
        int offset = toSplit.indexOf(delimiter);
        if (offset < 0) {
            return null;
        }
        String beforeDelimiter = toSplit.substring(0, offset);
        String afterDelimiter = toSplit.substring(offset + delimiter.length());
        return new String[] { beforeDelimiter, afterDelimiter };
    }

    private Strings() {}

    public static byte[] toUTF8Bytes(CharSequence charSequence) {
        return toUTF8Bytes(charSequence, new BytesRefBuilder());
    }

    public static byte[] toUTF8Bytes(CharSequence charSequence, BytesRefBuilder spare) {
        spare.copyChars(charSequence);
        return Arrays.copyOf(spare.bytes(), spare.length());
    }

    /**
     * Return substring(beginIndex, endIndex) that is impervious to string length.
     */
    public static String substring(String s, int beginIndex, int endIndex) {
        if (s == null) {
            return s;
        }

        int realEndIndex = s.length() > 0 ? s.length() - 1 : 0;

        if (endIndex > realEndIndex) {
            return s.substring(beginIndex);
        } else {
            return s.substring(beginIndex, endIndex);
        }
    }

    /**
     * If an array only consists of zero or one element, which is "*" or "_all" return an empty array
     * which is usually used as everything
     */
    public static boolean isAllOrWildcard(String[] data) {
        return CollectionUtils.isEmpty(data) || data.length == 1 && isAllOrWildcard(data[0]);
    }

    /**
     * Returns `true` if the string is `_all` or `*`.
     */
    public static boolean isAllOrWildcard(String data) {
        return "_all".equals(data) || "*".equals(data);
    }

    /**
     * Return a {@link String} that is the json representation of the provided {@link ToXContent}.
     * Wraps the output into an anonymous object if needed. The content is not pretty-printed
     * nor human readable.
     */
    public static String toString(MediaType mediaType, ToXContent toXContent) {
        return toString(mediaType, toXContent, false, false);
    }

    /**
     * Return a {@link String} that is the json representation of the provided {@link ToXContent}.
     * Wraps the output into an anonymous object if needed.
     * Allows to configure the params.
     * The content is not pretty-printed nor human readable.
     */
    public static String toString(MediaType mediaType, ToXContent toXContent, ToXContent.Params params) {
        return toString(mediaType, toXContent, params, false, false);
    }

    /**
     * Returns a string representation of the builder (only applicable for text based xcontent).
     * @param xContentBuilder builder containing an object to converted to a string
     */
    public static String toString(XContentBuilder xContentBuilder) {
        return BytesReference.bytes(xContentBuilder).utf8ToString();
    }

    /**
     * Return a {@link String} that is the json representation of the provided {@link ToXContent}.
     * Wraps the output into an anonymous object if needed. Allows to control whether the outputted
     * json needs to be pretty printed and human readable.
     *
     */
    public static String toString(MediaType mediaType, ToXContent toXContent, boolean pretty, boolean human) {
        return toString(mediaType, toXContent, ToXContent.EMPTY_PARAMS, pretty, human);
    }

    /**
     * Return a {@link String} that is the json representation of the provided {@link ToXContent}.
     * Wraps the output into an anonymous object if needed.
     * Allows to configure the params.
     * Allows to control whether the outputted json needs to be pretty printed and human readable.
     */
    private static String toString(MediaType mediaType, ToXContent toXContent, ToXContent.Params params, boolean pretty, boolean human) {
        try {
            XContentBuilder builder = createBuilder(mediaType, pretty, human);
            if (toXContent.isFragment()) {
                builder.startObject();
            }
            toXContent.toXContent(builder, params);
            if (toXContent.isFragment()) {
                builder.endObject();
            }
            return toString(builder);
        } catch (IOException e) {
            try {
                XContentBuilder builder = createBuilder(mediaType, pretty, human);
                builder.startObject();
                builder.field("error", "error building toString out of XContent: " + e.getMessage());
                builder.field("stack_trace", ExceptionsHelper.stackTrace(e));
                builder.endObject();
                return toString(builder);
            } catch (IOException e2) {
                throw new OpenSearchException("cannot generate error message for deserialization", e);
            }
        }
    }

    private static XContentBuilder createBuilder(MediaType mediaType, boolean pretty, boolean human) throws IOException {
        XContentBuilder builder = XContentBuilder.builder(mediaType.xContent());
        if (pretty) {
            builder.prettyPrint();
        }
        if (human) {
            builder.humanReadable(true);
        }
        return builder;
    }

    /**
     * Truncates string to a length less than length. Backtracks to throw out
     * high surrogates.
     */
    public static String cleanTruncate(String s, int length) {
        if (s == null) {
            return s;
        }
        /*
         * Its pretty silly for you to truncate to 0 length but just in case
         * someone does this shouldn't break.
         */
        if (length == 0) {
            return "";
        }
        if (length >= s.length()) {
            return s;
        }
        if (Character.isHighSurrogate(s.charAt(length - 1))) {
            length--;
        }
        return s.substring(0, length);
    }

    public static String padStart(String s, int minimumLength, char c) {
        if (s == null) {
            throw new NullPointerException("s");
        }
        if (s.length() >= minimumLength) {
            return s;
        } else {
            StringBuilder sb = new StringBuilder(minimumLength);
            for (int i = s.length(); i < minimumLength; i++) {
                sb.append(c);
            }

            sb.append(s);
            return sb.toString();
        }
    }

    public static String toLowercaseAscii(String in) {
        StringBuilder out = new StringBuilder();
        Iterator<Integer> iter = in.codePoints().iterator();
        while (iter.hasNext()) {
            int codepoint = iter.next();
            if (codepoint > 128) {
                out.appendCodePoint(codepoint);
            } else {
                out.appendCodePoint(Character.toLowerCase(codepoint));
            }
        }
        return out.toString();
    }
}
