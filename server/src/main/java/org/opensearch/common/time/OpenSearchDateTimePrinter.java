/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.time;

import java.time.ZoneId;
import java.time.temporal.TemporalAccessor;
import java.util.Locale;

/**
 * Interface for DateTimeFormatter{@link java.time.format.DateTimeFormatter}
 * to allow for custom implementations for datetime formatting
 */
interface OpenSearchDateTimePrinter {

    public OpenSearchDateTimePrinter withLocale(Locale locale);

    public OpenSearchDateTimePrinter withZone(ZoneId zoneId);

    public String format(TemporalAccessor temporal);

    public Locale getLocale();

    public ZoneId getZone();
}
