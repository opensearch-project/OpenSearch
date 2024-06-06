/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex;

import org.opensearch.common.Rounding;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.settings.Settings;

import java.util.ArrayList;
import java.util.List;

/**
 * Date dimension class
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class DateDimension extends Dimension {
    private final List<Rounding.DateTimeUnit> calendarIntervals;

    public DateDimension(String name, Settings settings, CompositeIndexConfig compositeIndexConfig) {
        super(name);
        List<String> intervalStrings = settings.getAsList("calendar_interval");
        if (intervalStrings == null || intervalStrings.isEmpty()) {
            this.calendarIntervals = compositeIndexConfig.getDefaultDateIntervals();
        } else {
            this.calendarIntervals = new ArrayList<>();
            for (String interval : intervalStrings) {
                this.calendarIntervals.add(CompositeIndexConfig.getTimeUnit(interval));
            }
        }
    }

    public List<Rounding.DateTimeUnit> getIntervals() {
        return calendarIntervals;
    }
}
