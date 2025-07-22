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

package org.opensearch.common.logging;

import org.opensearch.Build;
import org.opensearch.Version;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.common.logging.LoggerMessageFormat;
import org.opensearch.tasks.Task;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.BitSet;
import java.util.Iterator;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This is a simplistic logger that adds warning messages to HTTP headers.
 * Use <code>HeaderWarning.addWarning(message,params)</code>. Message will be formatted according to RFC7234.
 * The result will be returned as HTTP response headers.
 *
 * @opensearch.internal
 */
public class HeaderWarning {
    /**
     * Regular expression to test if a string matches the RFC7234 specification for warning headers. This pattern assumes that the warn code
     * is always 299. Further, this pattern assumes that the warn agent represents a version of OpenSearch including the build hash.
     */
    public static final Pattern WARNING_HEADER_PATTERN = Pattern.compile("299 OpenSearch-" + // warn code
        "\\d+\\.\\d+\\.\\d+(?:-(?:alpha|beta|rc)\\d+)?(?:-SNAPSHOT)?-" + // warn agent
        "(?:[a-f0-9]{7}(?:[a-f0-9]{33})?|unknown) " + // warn agent
        "\"((?:\t| |!|[\\x23-\\x5B]|[\\x5D-\\x7E]|[\\x80-\\xFF]|\\\\|\\\\\")*)\"( " + // quoted warning value, captured
        // quoted RFC 1123 date format
        "\"" + // opening quote
        "(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun), " + // weekday
        "\\d{2} " + // 2-digit day
        "(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) " + // month
        "\\d{4} " + // 4-digit year
        "\\d{2}:\\d{2}:\\d{2} " + // (two-digit hour):(two-digit minute):(two-digit second)
        "GMT" + // GMT
        "\")?"); // closing quote (optional, since an older version can still send a warn-date)
    public static final Pattern WARNING_XCONTENT_LOCATION_PATTERN = Pattern.compile("^\\[.*?]\\[-?\\d+:-?\\d+] ");

    /*
     * RFC7234 specifies the warning format as warn-code <space> warn-agent <space> "warn-text" [<space> "warn-date"]. Here, warn-code is a
     * three-digit number with various standard warn codes specified. The warn code 299 is apt for our purposes as it represents a
     * miscellaneous persistent warning (can be presented to a human, or logged, and must not be removed by a cache). The warn-agent is an
     * arbitrary token; here we use the Elasticsearch version and build hash. The warn text must be quoted. The warn-date is an optional
     * quoted field that can be in a variety of specified date formats; here we use RFC 1123 format.
     */
    private static final String WARNING_PREFIX = String.format(
        Locale.ROOT,
        "299 OpenSearch-%s%s-%s",
        Version.CURRENT.toString(),
        Build.CURRENT.isSnapshot() ? "-SNAPSHOT" : "",
        Build.CURRENT.hash()
    );

    private static BitSet doesNotNeedEncoding;

    static {
        doesNotNeedEncoding = new BitSet(1 + 0xFF);
        doesNotNeedEncoding.set('\t');
        doesNotNeedEncoding.set(' ');
        doesNotNeedEncoding.set('!');
        doesNotNeedEncoding.set('\\');
        doesNotNeedEncoding.set('"');
        // we have to skip '%' which is 0x25 so that it is percent-encoded too
        for (int i = 0x23; i <= 0x24; i++) {
            doesNotNeedEncoding.set(i);
        }
        for (int i = 0x26; i <= 0x5B; i++) {
            doesNotNeedEncoding.set(i);
        }
        for (int i = 0x5D; i <= 0x7E; i++) {
            doesNotNeedEncoding.set(i);
        }
        for (int i = 0x80; i <= 0xFF; i++) {
            doesNotNeedEncoding.set(i);
        }
        assert doesNotNeedEncoding.get('%') == false : doesNotNeedEncoding;
    }

    private static final Charset UTF_8 = StandardCharsets.UTF_8;

    /**
     * This is set once by the {@code Node} constructor, but it uses {@link CopyOnWriteArraySet} to ensure that tests can run in parallel.
     * <p>
     * Integration tests will create separate nodes within the same classloader, thus leading to a shared, {@code static} state.
     * In order for all tests to appropriately be handled, this must be able to remember <em>all</em> {@link ThreadContext}s that it is
     * given in a thread safe manner.
     * <p>
     * For actual usage, multiple nodes do not share the same JVM and therefore this will only be set once in practice.
     */
    static final CopyOnWriteArraySet<ThreadContext> THREAD_CONTEXT = new CopyOnWriteArraySet<>();

    /**
     * Set the {@link ThreadContext} used to add warning headers to network responses.
     * <p>
     * This is expected to <em>only</em> be invoked by the {@code Node}'s constructor (therefore once outside of tests).
     *
     * @param threadContext The thread context owned by the {@code ThreadPool} (and implicitly a {@code Node})
     * @throws IllegalStateException if this {@code threadContext} has already been set
     */
    public static void setThreadContext(ThreadContext threadContext) {
        Objects.requireNonNull(threadContext, "Cannot register a null ThreadContext");

        // add returning false means it _did_ have it already
        if (THREAD_CONTEXT.add(threadContext) == false) {
            throw new IllegalStateException("Double-setting ThreadContext not allowed!");
        }
    }

    /**
     * Remove the {@link ThreadContext} used to add warning headers to network responses.
     * <p>
     * This is expected to <em>only</em> be invoked by the {@code Node}'s {@code close} method (therefore once outside of tests).
     *
     * @param threadContext The thread context owned by the {@code ThreadPool} (and implicitly a {@code Node})
     * @throws IllegalStateException if this {@code threadContext} is unknown (and presumably already unset before)
     */
    public static void removeThreadContext(ThreadContext threadContext) {
        assert threadContext != null;

        // remove returning false means it did not have it already
        if (THREAD_CONTEXT.remove(threadContext) == false) {
            throw new IllegalStateException("Removing unknown ThreadContext not allowed!");
        }
    }

    /**
     * Extracts the warning value from the value of a warning header that is formatted according to RFC 7234. That is, given a string
     * {@code 299 OpenSearch-6.0.0 "warning value"}, the return value of this method would be {@code warning value}.
     *
     * @param s the value of a warning header formatted according to RFC 7234.
     * @return the extracted warning value
     */
    public static String extractWarningValueFromWarningHeader(final String s, boolean stripXContentPosition) {
        /*
         * We know the exact format of the warning header, so to extract the warning value we can skip forward from the front to the first
         * quote and we know the last quote is at the end of the string
         *
         *   299 Elasticsearch-6.0.0 "warning value"
         *                           ^             ^
         *                           firstQuote    lastQuote
         *
         * We parse this manually rather than using the capturing regular expression because the regular expression involves a lot of
         * backtracking and carries a performance penalty. However, when assertions are enabled, we still use the regular expression to
         * verify that we are maintaining the warning header format.
         */
        final int firstQuote = s.indexOf('\"');
        final int lastQuote = s.length() - 1;
        String warningValue = s.substring(firstQuote + 1, lastQuote);
        assert assertWarningValue(s, warningValue);
        if (stripXContentPosition) {
            Matcher matcher = WARNING_XCONTENT_LOCATION_PATTERN.matcher(warningValue);
            if (matcher.find()) {
                warningValue = warningValue.substring(matcher.end());
            }
        }
        return warningValue;
    }

    /**
     * Assert that the specified string has the warning value equal to the provided warning value.
     *
     * @param s            the string representing a full warning header
     * @param warningValue the expected warning header
     * @return {@code true} if the specified string has the expected warning value
     */
    private static boolean assertWarningValue(final String s, final String warningValue) {
        final Matcher matcher = WARNING_HEADER_PATTERN.matcher(s);
        final boolean matches = matcher.matches();
        assert matches;
        return matcher.group(1).equals(warningValue);
    }

    /**
     * Format a warning string in the proper warning format by prepending a warn code, warn agent, wrapping the warning string in quotes,
     * and appending the RFC 7231 date.
     *
     * @param s the warning string to format
     * @return a warning value formatted according to RFC 7234
     */
    public static String formatWarning(final String s) {
        // Assume that the common scenario won't have a string to escape and encode.
        int length = WARNING_PREFIX.length() + s.length() + 3;
        final StringBuilder sb = new StringBuilder(length);
        sb.append(WARNING_PREFIX).append(" \"").append(escapeAndEncode(s)).append("\"");
        return sb.toString();
    }

    /**
     * Escape and encode a string as a valid RFC 7230 quoted-string.
     *
     * @param s the string to escape and encode
     * @return the escaped and encoded string
     */
    public static String escapeAndEncode(final String s) {
        return encode(escapeBackslashesAndQuotes(s));
    }

    /**
     * Escape backslashes and quotes in the specified string.
     *
     * @param s the string to escape
     * @return the escaped string
     */
    static String escapeBackslashesAndQuotes(final String s) {
        /*
         * We want a fast path check to avoid creating the string builder and copying characters if needed. So we walk the string looking
         * for either of the characters that we need to escape. If we find a character that needs escaping, we start over and
         */
        boolean escapingNeeded = false;
        for (int i = 0; i < s.length(); i++) {
            final char c = s.charAt(i);
            if (c == '\\' || c == '"') {
                escapingNeeded = true;
                break;
            }
        }

        if (escapingNeeded) {
            final StringBuilder sb = new StringBuilder();
            for (final char c : s.toCharArray()) {
                if (c == '\\' || c == '"') {
                    sb.append("\\");
                }
                sb.append(c);
            }
            return sb.toString();
        } else {
            return s;
        }
    }

    /**
     * Encode a string containing characters outside of the legal characters for an RFC 7230 quoted-string.
     *
     * @param s the string to encode
     * @return the encoded string
     */
    static String encode(final String s) {
        // first check if the string needs any encoding; this is the fast path and we want to avoid creating a string builder and copying
        boolean encodingNeeded = false;
        for (int i = 0; i < s.length(); i++) {
            int current = s.charAt(i);
            if (doesNotNeedEncoding.get(current) == false) {
                encodingNeeded = true;
                break;
            }
        }

        if (encodingNeeded == false) {
            return s;
        }

        final StringBuilder sb = new StringBuilder(s.length());
        for (int i = 0; i < s.length();) {
            int current = s.charAt(i);
            /*
             * Either the character does not need encoding or it does; when the character does not need encoding we append the character to
             * a buffer and move to the next character and when the character does need encoding, we peel off as many characters as possible
             * which we encode using UTF-8 until we encounter another character that does not need encoding.
             */
            if (doesNotNeedEncoding.get(current)) {
                // append directly and move to the next character
                sb.append((char) current);
                i++;
            } else {
                int startIndex = i;
                do {
                    i++;
                } while (i < s.length() && doesNotNeedEncoding.get(s.charAt(i)) == false);

                final byte[] bytes = s.substring(startIndex, i).getBytes(UTF_8);
                // noinspection ForLoopReplaceableByForEach
                for (int j = 0; j < bytes.length; j++) {
                    sb.append('%').append(hex(bytes[j] >> 4)).append(hex(bytes[j]));
                }
            }
        }
        return sb.toString();
    }

    private static char hex(int b) {
        final char ch = Character.forDigit(b & 0xF, 16);
        if (Character.isLetter(ch)) {
            return Character.toUpperCase(ch);
        } else {
            return ch;
        }
    }

    public static String getXOpaqueId() {
        return THREAD_CONTEXT.stream()
            .filter(t -> t.getHeader(Task.X_OPAQUE_ID) != null)
            .findFirst()
            .map(t -> t.getHeader(Task.X_OPAQUE_ID))
            .orElse("");
    }

    public static void addWarning(String message, Object... params) {
        addWarning(THREAD_CONTEXT, message, params);
    }

    // package scope for testing
    static void addWarning(Set<ThreadContext> threadContexts, String message, Object... params) {
        final Iterator<ThreadContext> iterator = threadContexts.iterator();
        if (iterator.hasNext()) {
            final String formattedMessage = LoggerMessageFormat.format(message, params);
            final String warningHeaderValue = formatWarning(formattedMessage);
            assert WARNING_HEADER_PATTERN.matcher(warningHeaderValue).matches();
            assert extractWarningValueFromWarningHeader(warningHeaderValue, false).equals(escapeAndEncode(formattedMessage));
            while (iterator.hasNext()) {
                try {
                    final ThreadContext next = iterator.next();
                    next.addResponseHeader("Warning", warningHeaderValue);
                } catch (final IllegalStateException e) {
                    // ignored; it should be removed shortly
                }
            }
        }
    }
}
