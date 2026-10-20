/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.fielddomain;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.Map;
import java.util.Optional;

/**
 * Reader and writer for {@link DateRangeFieldDomain} custom metadata.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class DateRangeFieldDomainParser implements FieldDomainParser<DateRangeFieldDomain> {
    private static final String KEY_MIN = "min";
    private static final String KEY_MAX = "max";
    private static final String KEY_FINALIZED = "finalized";
    private static final String KEY_SOURCE = "source";
    private static final String KEY_FORMAT = "format";
    private static final String KEY_RESOLUTION = "resolution";

    /**
     * Returns {@link DateRangeFieldDomain#TYPE}.
     */
    @Override
    public String type() {
        return DateRangeFieldDomain.TYPE;
    }

    /**
     * Reads one field's date range bounds from a flat custom metadata map.
     */
    @Override
    public Optional<DateRangeFieldDomain> fromCustomData(String field, Map<String, String> customData, String prefix) {
        String min = customData.get(prefix + KEY_MIN);
        String max = customData.get(prefix + KEY_MAX);
        String finalized = customData.get(prefix + KEY_FINALIZED);
        String source = customData.get(prefix + KEY_SOURCE);
        String format = customData.get(prefix + KEY_FORMAT);
        String resolution = customData.get(prefix + KEY_RESOLUTION);

        if (min == null || max == null || finalized == null) {
            return Optional.empty();
        }

        Optional<Boolean> parsedFinalized = parseBoolean(finalized);
        if (parsedFinalized.isEmpty()) {
            return Optional.empty();
        }

        try {
            return Optional.of(new DateRangeFieldDomain(field, min, max, parsedFinalized.get(), source, format, resolution));
        } catch (RuntimeException e) {
            return Optional.empty();
        }
    }

    /**
     * Writes date range bounds into a flat custom metadata map.
     */
    @Override
    public void writeToCustomData(DateRangeFieldDomain domain, Map<String, String> targetCustomData, String prefix) {
        targetCustomData.put(prefix + KEY_MIN, domain.min());
        targetCustomData.put(prefix + KEY_MAX, domain.max());
        targetCustomData.put(prefix + KEY_FINALIZED, Boolean.toString(domain.finalized()));

        if (domain.source() != null) {
            targetCustomData.put(prefix + KEY_SOURCE, domain.source());
        }
        if (domain.format() != null) {
            targetCustomData.put(prefix + KEY_FORMAT, domain.format());
        }
        if (domain.resolution() != null) {
            targetCustomData.put(prefix + KEY_RESOLUTION, domain.resolution());
        }
    }

    /**
     * Removes all date range keys for a field prefix.
     */
    @Override
    public void removeFieldKeys(Map<String, String> target, String prefix) {
        target.remove(prefix + KEY_MIN);
        target.remove(prefix + KEY_MAX);
        target.remove(prefix + KEY_FINALIZED);
        target.remove(prefix + KEY_SOURCE);
        target.remove(prefix + KEY_FORMAT);
        target.remove(prefix + KEY_RESOLUTION);
    }

    private static Optional<Boolean> parseBoolean(String value) {
        if ("true".equals(value)) {
            return Optional.of(Boolean.TRUE);
        }
        if ("false".equals(value)) {
            return Optional.of(Boolean.FALSE);
        }
        return Optional.empty();
    }
}
