/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search.pruning;

import java.util.Map;
import java.util.Optional;

/**
 * Parser and encoder for {@link DateRangeFieldDomain} custom metadata.
 */
public final class DateRangeFieldDomainParser implements FieldDomainParser {
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
     * Parses one field's date range bounds from a flat custom metadata map.
     */
    @Override
    public Optional<FieldDomain> parse(String field, Map<String, String> customData, String prefix) {
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
     * Encodes date range bounds into a flat custom metadata map.
     */
    @Override
    public void encode(FieldDomain domain, Map<String, String> target, String prefix) {
        if ((domain instanceof DateRangeFieldDomain) == false) {
            throw new IllegalArgumentException("unsupported field domain type [" + domain.getClass().getName() + "]");
        }

        DateRangeFieldDomain dateRangeDomain = (DateRangeFieldDomain) domain;
        target.put(prefix + KEY_MIN, dateRangeDomain.min());
        target.put(prefix + KEY_MAX, dateRangeDomain.max());
        target.put(prefix + KEY_FINALIZED, Boolean.toString(dateRangeDomain.finalized()));

        if (dateRangeDomain.source() != null) {
            target.put(prefix + KEY_SOURCE, dateRangeDomain.source());
        }
        if (dateRangeDomain.format() != null) {
            target.put(prefix + KEY_FORMAT, dateRangeDomain.format());
        }
        if (dateRangeDomain.resolution() != null) {
            target.put(prefix + KEY_RESOLUTION, dateRangeDomain.resolution());
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
