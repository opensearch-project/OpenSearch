/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.time;

import java.text.Format;
import java.text.ParsePosition;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQuery;
import java.util.Locale;

/**
* Wrapper class for DateTimeFormatter{@link java.time.format.DateTimeFormatter}
* to allow for custom implementations for datetime parsing/formatting
 */
class OpenSearchDateTimeFormatter implements OpenSearchDateTimePrinter {
    private final DateTimeFormatter formatter;

    public OpenSearchDateTimeFormatter(String pattern) {
        this.formatter = DateTimeFormatter.ofPattern(pattern, Locale.ROOT);
    }

    public OpenSearchDateTimeFormatter(String pattern, Locale locale) {
        this.formatter = DateTimeFormatter.ofPattern(pattern, locale);
    }

    public OpenSearchDateTimeFormatter(DateTimeFormatter formatter) {
        this.formatter = formatter;
    }

    public OpenSearchDateTimeFormatter withLocale(Locale locale) {
        return new OpenSearchDateTimeFormatter(getFormatter().withLocale(locale));
    }

    public OpenSearchDateTimeFormatter withZone(ZoneId zoneId) {
        return new OpenSearchDateTimeFormatter(getFormatter().withZone(zoneId));
    }

    public String format(TemporalAccessor temporal) {
        return this.getFormatter().format(temporal);
    }

    public TemporalAccessor parse(CharSequence text, ParsePosition position) {
        return this.getFormatter().parse(text, position);
    }

    public TemporalAccessor parse(CharSequence text) {
        return this.getFormatter().parse(text);
    }

    public <T> T parse(CharSequence text, TemporalQuery<T> query) {
        return this.getFormatter().parse(text, query);
    }

    public ZoneId getZone() {
        return this.getFormatter().getZone();
    }

    public Locale getLocale() {
        return this.getFormatter().getLocale();
    }

    public TemporalAccessor parse(String input) {
        return formatter.parse(input);
    }

    public DateTimeFormatter getFormatter() {
        return formatter;
    }

    public Format toFormat() {
        return getFormatter().toFormat();
    }

    public Object parseObject(String text, ParsePosition pos) {
        return getFormatter().toFormat().parseObject(text, pos);
    }
}
