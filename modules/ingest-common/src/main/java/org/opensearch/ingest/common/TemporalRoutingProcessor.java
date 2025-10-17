/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ingest.common;

import org.opensearch.common.Nullable;
import org.opensearch.common.hash.MurmurHash3;
import org.opensearch.common.time.DateFormatter;
import org.opensearch.common.time.DateFormatters;
import org.opensearch.core.common.Strings;
import org.opensearch.ingest.AbstractProcessor;
import org.opensearch.ingest.ConfigurationUtils;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.ingest.Processor;

import java.nio.charset.StandardCharsets;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAccessor;
import java.util.Locale;
import java.util.Map;

import static org.opensearch.ingest.ConfigurationUtils.newConfigurationException;

/**
 * Processor that sets document routing based on temporal structure.
 *
 * This processor extracts a timestamp from a specified field, truncates it
 * to a configurable granularity (hour/day/week/month), and uses the resulting
 * temporal bucket to compute a routing value for improved temporal locality.
 *
 * Introduced in OpenSearch 3.2.0 to enable intelligent document co-location
 * based on time-based patterns for log and metrics workloads.
 */
public final class TemporalRoutingProcessor extends AbstractProcessor {

    public static final String TYPE = "temporal_routing";
    private static final String DEFAULT_FORMAT = "strict_date_optional_time";

    private final String timestampField;
    private final Granularity granularity;
    private final DateFormatter dateFormatter;
    private final boolean ignoreMissing;
    private final boolean overrideExisting;
    private final boolean hashBucket;

    /**
     * Supported temporal granularities
     */
    public enum Granularity {
        /** Hour granularity for hourly bucketing */
        HOUR(ChronoUnit.HOURS),
        /** Day granularity for daily bucketing */
        DAY(ChronoUnit.DAYS),
        /** Week granularity for weekly bucketing (ISO week) */
        WEEK(ChronoUnit.WEEKS),
        /** Month granularity for monthly bucketing */
        MONTH(ChronoUnit.MONTHS);

        private final ChronoUnit chronoUnit;

        Granularity(ChronoUnit chronoUnit) {
            this.chronoUnit = chronoUnit;
        }

        /**
         * Gets the ChronoUnit associated with this granularity
         * @return the ChronoUnit
         */
        public ChronoUnit getChronoUnit() {
            return chronoUnit;
        }

        /**
         * Parses a string value to a Granularity enum
         * @param value the string representation of the granularity
         * @return the corresponding Granularity enum value
         * @throws IllegalArgumentException if the value is not valid
         */
        public static Granularity fromString(String value) {
            try {
                return valueOf(value.toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Invalid granularity: " + value + ". Supported values are: hour, day, week, month");
            }
        }
    }

    TemporalRoutingProcessor(
        String tag,
        @Nullable String description,
        String timestampField,
        Granularity granularity,
        String format,
        boolean ignoreMissing,
        boolean overrideExisting,
        boolean hashBucket
    ) {
        super(tag, description);
        this.timestampField = timestampField;
        this.granularity = granularity;
        this.dateFormatter = DateFormatter.forPattern(format);
        this.ignoreMissing = ignoreMissing;
        this.overrideExisting = overrideExisting;
        this.hashBucket = hashBucket;
    }

    @Override
    public IngestDocument execute(IngestDocument document) throws Exception {
        // Check if routing already exists and we shouldn't override
        if (!overrideExisting) {
            try {
                Object existingRouting = document.getFieldValue("_routing", Object.class, true);
                if (existingRouting != null) {
                    return document;
                }
            } catch (Exception e) {
                // Field doesn't exist, continue with processing
            }
        }

        Object timestampValue = document.getFieldValue(timestampField, Object.class, ignoreMissing);

        if (timestampValue == null && ignoreMissing) {
            return document;
        }

        if (timestampValue == null) {
            throw new IllegalArgumentException("field [" + timestampField + "] not present as part of path [" + timestampField + "]");
        }

        String routingValue = computeRoutingValue(timestampValue.toString());
        document.setFieldValue("_routing", routingValue);

        return document;
    }

    /**
     * Computes routing value from timestamp by truncating to granularity
     * and optionally hashing for distribution
     */
    private String computeRoutingValue(String timestamp) {
        // Parse timestamp using DateFormatter and convert to ZonedDateTime
        TemporalAccessor accessor = dateFormatter.parse(timestamp);
        ZonedDateTime dateTime = DateFormatters.from(accessor, Locale.ROOT, ZoneOffset.UTC);

        // Truncate to granularity
        ZonedDateTime truncated = truncateToGranularity(dateTime);

        // Create temporal bucket key
        String temporalBucket = createTemporalBucketKey(truncated);

        // Optionally hash for distribution
        if (hashBucket) {
            byte[] bucketBytes = temporalBucket.getBytes(StandardCharsets.UTF_8);
            long hash = MurmurHash3.hash128(bucketBytes, 0, bucketBytes.length, 0, new MurmurHash3.Hash128()).h1;
            return String.valueOf(hash == Long.MIN_VALUE ? 0L : (hash < 0 ? -hash : hash));
        }

        return temporalBucket;
    }

    /**
     * Truncates datetime to the specified granularity
     *
     * IMPORTANT: This logic MUST be kept in sync with TemporalRoutingSearchProcessor.truncateToGranularity()
     * in the search-pipeline-common module to ensure consistent temporal bucketing.
     */
    private ZonedDateTime truncateToGranularity(ZonedDateTime dateTime) {
        switch (granularity) {
            case HOUR:
                return dateTime.withMinute(0).withSecond(0).withNano(0);
            case DAY:
                return dateTime.withHour(0).withMinute(0).withSecond(0).withNano(0);
            case WEEK:
                // Truncate to start of week (Monday)
                ZonedDateTime dayTruncated = dateTime.withHour(0).withMinute(0).withSecond(0).withNano(0);
                return dayTruncated.with(java.time.temporal.TemporalAdjusters.previousOrSame(java.time.DayOfWeek.MONDAY));
            case MONTH:
                return dateTime.withDayOfMonth(1).withHour(0).withMinute(0).withSecond(0).withNano(0);
            default:
                throw new IllegalArgumentException("Unsupported granularity: " + granularity);
        }
    }

    /**
     * Creates a string key for the temporal bucket
     *
     * IMPORTANT: This logic MUST be kept in sync with TemporalRoutingSearchProcessor.createTemporalBucket()
     * in the search-pipeline-common module. Both processors must generate identical bucket keys for the
     * same input to ensure documents are routed to the same shards during ingest and search.
     *
     * TODO: Consider moving this shared logic to a common module when search and ingest pipelines
     * can share code more easily.
     */
    private String createTemporalBucketKey(ZonedDateTime truncated) {
        switch (granularity) {
            case HOUR:
                return truncated.getYear()
                    + "-"
                    + String.format(Locale.ROOT, "%02d", truncated.getMonthValue())
                    + "-"
                    + String.format(Locale.ROOT, "%02d", truncated.getDayOfMonth())
                    + "T"
                    + String.format(Locale.ROOT, "%02d", truncated.getHour());
            case DAY:
                return truncated.getYear()
                    + "-"
                    + String.format(Locale.ROOT, "%02d", truncated.getMonthValue())
                    + "-"
                    + String.format(Locale.ROOT, "%02d", truncated.getDayOfMonth());
            case WEEK:
                // Use ISO week format: YYYY-WNN
                int weekOfYear = truncated.get(java.time.temporal.WeekFields.ISO.weekOfWeekBasedYear());
                int weekYear = truncated.get(java.time.temporal.WeekFields.ISO.weekBasedYear());
                return weekYear + "-W" + String.format(Locale.ROOT, "%02d", weekOfYear);
            case MONTH:
                return truncated.getYear() + "-" + String.format(Locale.ROOT, "%02d", truncated.getMonthValue());
            default:
                throw new IllegalArgumentException("Unsupported granularity: " + granularity);
        }
    }

    @Override
    public String getType() {
        return TYPE;
    }

    String getTimestampField() {
        return timestampField;
    }

    Granularity getGranularity() {
        return granularity;
    }

    DateFormatter getDateFormatter() {
        return dateFormatter;
    }

    boolean isIgnoreMissing() {
        return ignoreMissing;
    }

    boolean isOverrideExisting() {
        return overrideExisting;
    }

    boolean isHashBucket() {
        return hashBucket;
    }

    /**
     * Factory for creating TemporalRoutingProcessor instances
     */
    public static final class Factory implements Processor.Factory {

        @Override
        public TemporalRoutingProcessor create(
            Map<String, Processor.Factory> processorFactories,
            String tag,
            @Nullable String description,
            Map<String, Object> config
        ) throws Exception {

            String timestampField = ConfigurationUtils.readStringProperty(TYPE, tag, config, "timestamp_field");
            String granularityStr = ConfigurationUtils.readStringProperty(TYPE, tag, config, "granularity");
            String format = ConfigurationUtils.readOptionalStringProperty(TYPE, tag, config, "format");
            boolean ignoreMissing = ConfigurationUtils.readBooleanProperty(TYPE, tag, config, "ignore_missing", false);
            boolean overrideExisting = ConfigurationUtils.readBooleanProperty(TYPE, tag, config, "override_existing", true);
            boolean hashBucket = ConfigurationUtils.readBooleanProperty(TYPE, tag, config, "hash_bucket", false);

            // Set default format if not provided
            if (format == null) {
                format = DEFAULT_FORMAT;
            }

            // Validation
            if (Strings.isNullOrEmpty(timestampField)) {
                throw newConfigurationException(TYPE, tag, "timestamp_field", "cannot be null or empty");
            }

            if (Strings.isNullOrEmpty(granularityStr)) {
                throw newConfigurationException(TYPE, tag, "granularity", "cannot be null or empty");
            }

            Granularity granularity;
            try {
                granularity = Granularity.fromString(granularityStr);
            } catch (IllegalArgumentException e) {
                throw newConfigurationException(TYPE, tag, "granularity", e.getMessage());
            }

            // Validate date format
            try {
                DateFormatter.forPattern(format);
            } catch (Exception e) {
                throw newConfigurationException(TYPE, tag, "format", "invalid date format: " + e.getMessage());
            }

            return new TemporalRoutingProcessor(
                tag,
                description,
                timestampField,
                granularity,
                format,
                ignoreMissing,
                overrideExisting,
                hashBucket
            );
        }
    }
}
