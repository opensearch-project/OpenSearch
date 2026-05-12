/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.opensearch.analytics.spi.AbstractNameMappingAdapter;

import java.util.List;

/**
 * Date-part extractor adapters — rewrite {@code FN(ts)} to {@code date_part('<unit>', ts)}.
 * Alias pairs (e.g. MONTH_OF_YEAR → MONTH) share an adapter instance at registration.
 *
 * @opensearch.internal
 */
final class DatePartAdapters extends AbstractNameMappingAdapter {

    DatePartAdapters(String unit) {
        super(SqlLibraryOperators.DATE_PART, List.of(unit), List.of());
    }

    static DatePartAdapters year() {
        return new DatePartAdapters("year");
    }

    static DatePartAdapters quarter() {
        return new DatePartAdapters("quarter");
    }

    static DatePartAdapters month() {
        return new DatePartAdapters("month");
    }

    static DatePartAdapters day() {
        return new DatePartAdapters("day");
    }

    static DatePartAdapters dayOfYear() {
        return new DatePartAdapters("doy");
    }

    static DatePartAdapters hour() {
        return new DatePartAdapters("hour");
    }

    static DatePartAdapters minute() {
        return new DatePartAdapters("minute");
    }

    static DatePartAdapters microsecond() {
        return new DatePartAdapters("microsecond");
    }

    static DatePartAdapters week() {
        return new DatePartAdapters("week");
    }
}
