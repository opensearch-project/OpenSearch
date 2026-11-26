/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.ingestion.objectstore;

import org.opensearch.core.common.Strings;

import java.util.Locale;

/**
 * Sanitizes untrusted strings (such as object keys and record content, which originate from object storage) before
 * they are embedded in log messages or exception messages, to prevent CRLF log injection. Carriage returns
 * and line feeds are replaced with a visible escape so a crafted key cannot forge additional log lines.
 */
final class LogSanitizer {

    private static final int MAX_LENGTH = 1024;

    private LogSanitizer() {}

    /**
     * Escapes every character a log viewer, SIEM parser or terminal may treat as a line break or as cursor control,
     * not only CR and LF. That includes the C0 and C1 control ranges (so VT, FF, NEL and ESC, the start of an ANSI
     * sequence) and the unicode LINE SEPARATOR and PARAGRAPH SEPARATOR. Anything escaped is rendered as its
     * {@code \\uXXXX} form so the original value stays readable and cannot forge a log line.
     *
     * @param value the bounded, untrusted value
     * @return the value with control and separator characters escaped
     */
    private static String escapeControlCharacters(String value) {
        StringBuilder out = new StringBuilder(value.length());
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c == '\r') {
                out.append("\\r");
            } else if (c == '\n') {
                out.append("\\n");
            } else if (c < 0x20 || c == 0x7F || (c >= 0x80 && c <= 0x9F) || c == '\u2028' || c == '\u2029') {
                out.append(String.format(Locale.ROOT, "\\u%04x", (int) c));
            } else {
                out.append(c);
            }
        }
        return out.toString();
    }

    /**
     * Returns a copy of the value with control and separator characters replaced by their escaped forms, truncated to a
     * bounded length so an oversized untrusted value cannot flood logs or exception messages. Truncation never splits a
     * surrogate pair, so the result is always well-formed UTF-16.
     *
     * @param value the untrusted value (may be {@code null})
     * @return the sanitized, length-bounded value, or {@code null} if the input was {@code null}
     */
    public static String sanitize(String value) {
        if (value == null) {
            return null;
        }
        String bounded = Strings.cleanTruncate(value, MAX_LENGTH);
        String escaped = escapeControlCharacters(bounded);
        String capped = Strings.cleanTruncate(escaped, MAX_LENGTH);
        return capped.length() < escaped.length() || bounded.length() < value.length() ? capped + "..." : capped;
    }
}
