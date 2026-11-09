/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.ingestion.objectstore;

import org.opensearch.test.OpenSearchTestCase;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

/**
 * Unit tests for {@link LogSanitizer}.
 */
public class LogSanitizerTests extends OpenSearchTestCase {

    public void testEscapesCarriageReturnAndLineFeed() {
        assertThat(LogSanitizer.sanitize("a\r\nb\nc\rd"), is("a\\r\\nb\\nc\\rd"));
    }

    public void testLeavesCleanStringUnchanged() {
        assertThat(LogSanitizer.sanitize("dump/events.ndjson"), is("dump/events.ndjson"));
    }

    public void testNullReturnsNull() {
        assertThat(LogSanitizer.sanitize(null), is(nullValue()));
    }

    public void testOversizedValueIsTruncated() {
        String sanitized = LogSanitizer.sanitize("a".repeat(5000));
        assertThat(sanitized.length(), is(1027));
        assertThat(sanitized.endsWith("..."), is(true));
    }

    public void testTruncationDoesNotSplitASurrogatePair() {
        String sanitized = LogSanitizer.sanitize("a".repeat(1023) + "\uD83D\uDE00" + "b".repeat(100));
        assertThat(sanitized.endsWith("..."), is(true));
        String body = sanitized.substring(0, sanitized.length() - 3);
        assertThat(Character.isHighSurrogate(body.charAt(body.length() - 1)), is(false));
        assertThat(body.length(), is(1023));
    }

    public void testOverlongValueIsTruncatedAndMarked() {
        String sanitized = LogSanitizer.sanitize("a".repeat(2000));
        assertThat(sanitized.length(), is(1027));
        assertThat(sanitized.endsWith("..."), is(true));
    }

    public void testOverlongValueWithNewlinesStaysBounded() {
        String sanitized = LogSanitizer.sanitize("a\n".repeat(2000));
        assertThat(sanitized.length(), is(1027));
        assertThat(sanitized.endsWith("..."), is(true));
        assertThat(sanitized.contains("\n"), is(false));
    }

    public void testValueExactlyAtTheCapIsNotMarked() {
        String sanitized = LogSanitizer.sanitize("a".repeat(1024));
        assertThat(sanitized.length(), is(1024));
        assertThat(sanitized.endsWith("..."), is(false));
    }

    public void testOtherLineTerminatorsAndControlCharactersAreEscaped() {
        String sanitized = LogSanitizer.sanitize("a\u2028b\u2029c\u0085d\u000bE\u001bF");
        assertThat(sanitized, is("a\\u2028b\\u2029c\\u0085d\\u000bE\\u001bF"));
    }

    public void testPlainTextIsUnchanged() {
        assertThat(LogSanitizer.sanitize("dump/part-0.ndjson"), is("dump/part-0.ndjson"));
    }

    public void testEscapingPastTheCapAlsoAvoidsSplittingASurrogatePair() {
        String value = "\n".repeat(500) + "a".repeat(23) + "\uD83D\uDE00" + "b".repeat(600);
        String sanitized = LogSanitizer.sanitize(value);
        assertThat(sanitized.endsWith("..."), is(true));
        String body = sanitized.substring(0, sanitized.length() - 3);
        assertThat(Character.isHighSurrogate(body.charAt(body.length() - 1)), is(false));
        assertThat(body.length(), is(1023));
    }
}
