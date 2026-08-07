/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.fielddomain;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.mapper.DateFieldMapper;

import java.util.Locale;
import java.util.Objects;

/**
 * Field domain implementation for date-like range metadata.
 *
 * The stored min/max values are serialized as strings so the cluster-state custom metadata remains a simple
 * {@code Map<String, String>}. The evaluator interprets them using the configured resolution.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class DateRangeFieldDomain implements FieldDomain {
    /**
     * Metadata type for date range field domains.
     */
    public static final String TYPE = "date_range";

    private static final String DEFAULT_RESOLUTION = DateFieldMapper.Resolution.MILLISECONDS.name().toLowerCase(Locale.ROOT);

    private final String field;
    private final String min;
    private final String max;
    private final boolean finalized;
    private final String source;
    private final String format;
    private final String resolution;

    /**
     * Creates millisecond-resolution date range bounds.
     *
     * @param field field name this domain describes
     * @param min inclusive lower index bound in epoch milliseconds
     * @param max inclusive upper index bound in epoch milliseconds
     * @param finalized whether this domain is trusted as complete for consumers that require finalized metadata
     * @param source optional producer identifier
     */
    public DateRangeFieldDomain(String field, long min, long max, boolean finalized, String source) {
        this(field, Long.toString(min), Long.toString(max), finalized, source, null, DEFAULT_RESOLUTION);
    }

    /**
     * Creates date range bounds using serialized bound values.
     *
     * @param field field name this domain describes
     * @param min inclusive lower index bound
     * @param max inclusive upper index bound
     * @param finalized whether this domain is trusted as complete for consumers that require finalized metadata
     * @param source optional producer identifier
     * @param format optional date format used by the bound producer
     * @param resolution date resolution, for example milliseconds or nanoseconds
     */
    public DateRangeFieldDomain(String field, String min, String max, boolean finalized, String source, String format, String resolution) {
        this.field = requireNonEmpty(field, "field");
        this.min = requireNonEmpty(min, "min");
        this.max = requireNonEmpty(max, "max");
        validateBounds(this.min, this.max);
        this.finalized = finalized;
        this.source = source;
        this.format = format;
        this.resolution = normalizeResolution(resolution);
    }

    @Override
    public String field() {
        return field;
    }

    /**
     * Returns {@link #TYPE}.
     */
    @Override
    public String type() {
        return TYPE;
    }

    /**
     * Inclusive lower index bound, serialized as metadata.
     */
    public String min() {
        return min;
    }

    /**
     * Inclusive upper index bound, serialized as metadata.
     */
    public String max() {
        return max;
    }

    @Override
    public boolean finalized() {
        return finalized;
    }

    /**
     * Optional identifier for the component that produced these bounds.
     */
    public String source() {
        return source;
    }

    /**
     * Optional date format used to interpret query and index bounds.
     */
    public String format() {
        return format;
    }

    /**
     * Optional date resolution used to interpret numeric bounds.
     */
    public String resolution() {
        return resolution;
    }

    private static String requireNonEmpty(String value, String name) {
        Objects.requireNonNull(value, name + " must not be null");
        if (value.isEmpty()) {
            throw new IllegalArgumentException(name + " must not be empty");
        }
        return value;
    }

    private static void validateBounds(String min, String max) {
        long parsedMin;
        long parsedMax;
        try {
            parsedMin = Long.parseLong(min);
            parsedMax = Long.parseLong(max);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("date range field domain bounds must be numeric long values", e);
        }
        if (parsedMin > parsedMax) {
            throw new IllegalArgumentException("date range field domain min must be less than or equal to max");
        }
    }

    private static String normalizeResolution(String resolution) {
        if (resolution == null || resolution.isBlank()) {
            throw new IllegalArgumentException("resolution must not be null or empty");
        }

        String configured = resolution.toLowerCase(Locale.ROOT);
        for (DateFieldMapper.Resolution dateResolution : DateFieldMapper.Resolution.values()) {
            if (dateResolution.name().toLowerCase(Locale.ROOT).equals(configured) || dateResolution.type().equals(configured)) {
                return dateResolution.name().toLowerCase(Locale.ROOT);
            }
        }
        throw new IllegalArgumentException("unsupported date range field domain resolution [" + resolution + "]");
    }
}
