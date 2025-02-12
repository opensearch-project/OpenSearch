/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.client;

import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

final class ResponseWarningsExtractor {

    /**
     * Optimized regular expression to test if a string matches the RFC 1123 date
     * format (with quotes and leading space). Start/end of line characters and
     * atomic groups are used to prevent backtracking.
     */
    private static final Pattern WARNING_HEADER_DATE_PATTERN = Pattern.compile("^ " + // start of line, leading space
    // quoted RFC 1123 date format
        "\"" + // opening quote
        "(?>Mon|Tue|Wed|Thu|Fri|Sat|Sun), " + // day of week, atomic group to prevent backtracking
        "\\d{2} " + // 2-digit day
        "(?>Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) " + // month, atomic group to prevent backtracking
        "\\d{4} " + // 4-digit year
        "\\d{2}:\\d{2}:\\d{2} " + // (two-digit hour):(two-digit minute):(two-digit second)
        "GMT" + // GMT
        "\"$"); // closing quote (optional, since an older version can still send a warn-date), end of line

    /**
     * Length of RFC 1123 format (with quotes and leading space), used in
     * matchWarningHeaderPatternByPrefix(String).
     */
    private static final int WARNING_HEADER_DATE_LENGTH = 0 + 1 + 1 + 3 + 1 + 1 + 2 + 1 + 3 + 1 + 4 + 1 + 2 + 1 + 2 + 1 + 2 + 1 + 3 + 1;

    private ResponseWarningsExtractor() {}

    /**
     * Returns a list of all warning headers returned in the response.
     * @param response HTTP response
     */
    static List<String> getWarnings(final HttpResponse response) {
        List<String> warnings = new ArrayList<>();
        for (Header header : response.getHeaders("Warning")) {
            String warning = header.getValue();
            if (matchWarningHeaderPatternByPrefix(warning)) {
                warnings.add(extractWarningValueFromWarningHeader(warning));
            } else {
                warnings.add(warning);
            }
        }
        return warnings;
    }

    /**
     * Tests if a string matches the RFC 7234 specification for warning headers.
     * This assumes that the warn code is always 299 and the warn agent is always
     * OpenSearch.
     *
     * @param s the value of a warning header formatted according to RFC 7234
     * @return {@code true} if the input string matches the specification
     */
    private static boolean matchWarningHeaderPatternByPrefix(final String s) {
        return s.startsWith("299 OpenSearch-");
    }

    /**
     * Refer to org.opensearch.common.logging.DeprecationLogger
     */
    private static String extractWarningValueFromWarningHeader(final String s) {
        String warningHeader = s;

        /*
         * The following block tests for the existence of a RFC 1123 date in the warning header. If the date exists, it is removed for
         * extractWarningValueFromWarningHeader(String) to work properly (as it does not handle dates).
         */
        if (s.length() > WARNING_HEADER_DATE_LENGTH) {
            final String possibleDateString = s.substring(s.length() - WARNING_HEADER_DATE_LENGTH);
            final Matcher matcher = WARNING_HEADER_DATE_PATTERN.matcher(possibleDateString);

            if (matcher.matches()) {
                warningHeader = warningHeader.substring(0, s.length() - WARNING_HEADER_DATE_LENGTH);
            }
        }

        final int firstQuote = warningHeader.indexOf('\"');
        final int lastQuote = warningHeader.length() - 1;
        final String warningValue = warningHeader.substring(firstQuote + 1, lastQuote);
        return warningValue;
    }

}
