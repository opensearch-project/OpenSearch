/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package fixture.gcs;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ContentHttpHeadersParser {

    private static final Pattern PATTERN_CONTENT_RANGE = Pattern.compile("bytes=([0-9]+)-([0-9]+)");
    private static final Pattern PATTERN_CONTENT_RANGE_BYTES = Pattern.compile("bytes (?:(\\d+)-(\\d+)|\\*)/(?:(\\d+)|\\*)");


    public static Range parseRangeHeader(String rangeHeaderValue) {
        final Matcher matcher = PATTERN_CONTENT_RANGE.matcher(rangeHeaderValue);
        if (matcher.matches()) {
            try {
                return new Range(Integer.parseInt(matcher.group(1)), Integer.parseInt(matcher.group(2)));
            } catch (NumberFormatException e) {
                return null;
            }
        }
        return null;
    }

    public record Range(int start, int end) {

        public String header() {
            return String.format("bytes=%s-%s", start, end);
        }

    }

    public static ContentRange parseContentRangeHeader(String contentRangeHeaderValue) {
        final Matcher matcher = PATTERN_CONTENT_RANGE_BYTES.matcher(contentRangeHeaderValue);
        if (matcher.matches()) {
            try {
                if (matcher.groupCount() == 3) {
                    final Integer start = parseIntegerValue(matcher.group(1));
                    final Integer end = parseIntegerValue(matcher.group(2));
                    final Integer size = parseIntegerValue(matcher.group(3));
                    return new ContentRange(start, end, size);
                }
            } catch (NumberFormatException e) {
                return null;
            }
        }
        return null;
    }

    private static Integer parseIntegerValue(String value) {
        return value == null ? null : Integer.parseInt(value);
    }

    public record ContentRange(Integer start, Integer end, Integer size) {

        public ContentRange {
            assert (start == null) == (end == null) : "Must have either start and end or neither";
        }

        public boolean hasRange() {
            return start != null && end != null;
        }

        public boolean hasSize() {
            return size != null;
        }

        public String headerString() {
            final String rangeString = hasRange() ? start + "-" + end : "*";
            final String sizeString = hasSize() ? String.valueOf(size) : "*";
            return "bytes " + rangeString + "/" + sizeString;
        }
    }

}
