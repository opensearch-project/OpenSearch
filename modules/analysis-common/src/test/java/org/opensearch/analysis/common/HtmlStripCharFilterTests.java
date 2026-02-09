/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analysis.common;

import org.apache.lucene.analysis.charfilter.HTMLStripCharFilter;
import org.apache.lucene.analysis.pattern.PatternReplaceCharFilter;
import org.opensearch.test.OpenSearchTestCase;

import java.io.StringReader;
import java.io.Reader;
import java.io.IOException;
import java.util.regex.Pattern;

public class HtmlStripCharFilterTests extends OpenSearchTestCase {
    private static final Pattern EQUALS_QUOTE_PATTERN = Pattern.compile("=(?=[\\\"\\s])");

    public void testHtmlStripWithEqualsAtEndOfAttribute() throws IOException {
        String input = "<a href=\"https://www.example.com/?test=\">example</a> this gets discarded<a href=\"https://www.example.com/\">example2</a> continues here";
        assertHtmlStrip(input, "example", "this gets discarded", "example2");
    }

    public void testHtmlStripWithEqualsNotAtEnd() throws IOException {
        String input = "<a href=\"https://www.example.com/?test=foo\">example</a> this should work<a href=\"https://www.example.com/\">example2</a>";
        assertHtmlStrip(input, "example", "this should work", "example2");
    }

    public void testHtmlStripWithSpaceAfterEquals() throws IOException {
        String input = "<a href=\"https://www.example.com/?test= \">example</a> this might work<a href=\"https://www.example.com/\">example2</a>";
        assertHtmlStrip(input, "example", "this might work", "example2");
    }

    public void testHtmlStripWithEntityEquals() throws IOException {
        String input = "<a href=\"https://www.example.com/?test&#61;\">example</a> this should work<a href=\"https://www.example.com/\">example2</a>";
        assertHtmlStrip(input, "example", "this should work", "example2");
    }

    private void assertHtmlStrip(String input, String... expectedSubstrings) throws IOException {
        Reader reader = new HTMLStripCharFilter(
            new PatternReplaceCharFilter(EQUALS_QUOTE_PATTERN, "&#61;", new StringReader(input)),
            null
        );
        StringBuilder sb = new StringBuilder();
        char[] buffer = new char[1024];
        int len;
        while ((len = reader.read(buffer)) != -1) {
            sb.append(buffer, 0, len);
        }
        String output = sb.toString();
        for (String expected : expectedSubstrings) {
            assertTrue("Output should contain '" + expected + "'. Output was: " + output, output.contains(expected));
        }
    }
}
