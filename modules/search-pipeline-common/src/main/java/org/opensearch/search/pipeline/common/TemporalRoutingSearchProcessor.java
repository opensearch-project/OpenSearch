/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline.common;

import org.apache.lucene.search.BooleanClause;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.common.hash.MurmurHash3;
import org.opensearch.common.time.DateFormatter;
import org.opensearch.common.time.DateFormatters;
import org.opensearch.core.common.Strings;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilderVisitor;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.ingest.ConfigurationUtils;
import org.opensearch.search.pipeline.AbstractProcessor;
import org.opensearch.search.pipeline.Processor;
import org.opensearch.search.pipeline.SearchRequestProcessor;

import java.nio.charset.StandardCharsets;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAccessor;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.opensearch.ingest.ConfigurationUtils.newConfigurationException;

/**
 * A search request processor that automatically adds routing to search requests
 * based on temporal range information found in queries.
 *
 * This processor works in conjunction with the TemporalRoutingProcessor
 * (ingest pipeline) to optimize searches by routing them only to shards
 * that contain documents within the specified time ranges.
 *
 * Example: A query with range filter on timestamp field will only
 * search shards containing documents for those temporal buckets.
 */
public class TemporalRoutingSearchProcessor extends AbstractProcessor implements SearchRequestProcessor {

    /** The processor type identifier */
    public static final String TYPE = "temporal_routing_search";
    private static final String DEFAULT_FORMAT = "strict_date_optional_time";

    private final String timestampField;
    private final Granularity granularity;
    private final DateFormatter dateFormatter;
    private final boolean enableAutoDetection;
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

    TemporalRoutingSearchProcessor(
        String tag,
        String description,
        boolean ignoreFailure,
        String timestampField,
        Granularity granularity,
        String format,
        boolean enableAutoDetection,
        boolean hashBucket
    ) {
        super(tag, description, ignoreFailure);
        this.timestampField = timestampField;
        this.granularity = granularity;
        this.dateFormatter = DateFormatter.forPattern(format);
        this.enableAutoDetection = enableAutoDetection;
        this.hashBucket = hashBucket;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public SearchRequest processRequest(SearchRequest request) throws Exception {
        // Skip if routing is already explicitly set
        if (request.routing() != null && !request.routing().isEmpty()) {
            return request;
        }

        Set<String> routingValues = new HashSet<>();

        // Extract temporal range information from the search request using visitor pattern
        if (request.source() != null && request.source().query() != null) {
            TemporalRangeExtractionVisitor visitor = new TemporalRangeExtractionVisitor(routingValues);
            request.source().query().visit(visitor);
        }

        // If we found temporal range information, compute routing and apply it
        if (!routingValues.isEmpty()) {
            Set<String> computedRouting = new HashSet<>();
            for (String temporalBucket : routingValues) {
                if (hashBucket) {
                    String routingValue = hashTemporalBucket(temporalBucket);
                    computedRouting.add(routingValue);
                } else {
                    computedRouting.add(temporalBucket);
                }
            }

            if (!computedRouting.isEmpty()) {
                // Join multiple routing values with comma
                String routing = String.join(",", computedRouting);
                request.routing(routing);
            }
        }

        return request;
    }

    /**
     * Visitor implementation for extracting temporal ranges from queries
     */
    private class TemporalRangeExtractionVisitor implements QueryBuilderVisitor {
        private final Set<String> temporalBuckets;

        TemporalRangeExtractionVisitor(Set<String> temporalBuckets) {
            this.temporalBuckets = temporalBuckets;
        }

        @Override
        public void accept(QueryBuilder qb) {
            if (qb instanceof RangeQueryBuilder rangeQuery && timestampField.equals(rangeQuery.fieldName())) {
                extractTemporalBucketsFromRange(rangeQuery);
            }
            // The visitor pattern will automatically handle other query types
        }

        @Override
        public QueryBuilderVisitor getChildVisitor(BooleanClause.Occur occur) {
            // Only process MUST and FILTER clauses as they restrict results
            // SHOULD and MUST_NOT don't guarantee document presence on specific shards
            if (occur == BooleanClause.Occur.MUST || occur == BooleanClause.Occur.FILTER) {
                return this;
            }
            // Return a no-op visitor for SHOULD and MUST_NOT clauses
            return QueryBuilderVisitor.NO_OP_VISITOR;
        }

        /**
         * Extracts temporal buckets from a range query
         */
        private void extractTemporalBucketsFromRange(RangeQueryBuilder rangeQuery) {
            try {
                Object from = rangeQuery.from();
                Object to = rangeQuery.to();

                if (from != null && to != null) {
                    ZonedDateTime fromDate = parseTimestamp(from.toString());
                    ZonedDateTime toDate = parseTimestamp(to.toString());

                    // Generate all temporal buckets in the range
                    generateTemporalBucketsInRange(fromDate, toDate);
                } else if (from != null) {
                    // Only lower bound
                    ZonedDateTime fromDate = parseTimestamp(from.toString());
                    String bucket = createTemporalBucket(fromDate);
                    temporalBuckets.add(bucket);
                } else if (to != null) {
                    // Only upper bound
                    ZonedDateTime toDate = parseTimestamp(to.toString());
                    String bucket = createTemporalBucket(toDate);
                    temporalBuckets.add(bucket);
                }
            } catch (Exception e) {
                // If we can't parse the dates, skip temporal routing
                // This allows the query to fall back to searching all shards
            }
        }

        /**
         * Generates all temporal buckets within a date range
         */
        private void generateTemporalBucketsInRange(ZonedDateTime from, ZonedDateTime to) {
            ZonedDateTime current = truncateToGranularity(from);
            ZonedDateTime end = truncateToGranularity(to);

            // Add buckets up to a reasonable limit to avoid too many routing values
            // TODO: Make maxBuckets configurable via processor configuration
            int maxBuckets = 100; // Hard-coded limit for now
            int bucketCount = 0;

            while (!current.isAfter(end) && bucketCount < maxBuckets) {
                String bucket = createTemporalBucket(current);
                temporalBuckets.add(bucket);

                current = incrementByGranularity(current);
                bucketCount++;
            }
        }
    }

    /**
     * Parses timestamp string to ZonedDateTime
     */
    private ZonedDateTime parseTimestamp(String timestamp) {
        TemporalAccessor accessor = dateFormatter.parse(timestamp);
        return DateFormatters.from(accessor, Locale.ROOT, ZoneOffset.UTC);
    }

    /**
     * Truncates datetime to the specified granularity
     *
     * IMPORTANT: This logic MUST be kept in sync with TemporalRoutingProcessor.truncateToGranularity()
     * in the ingest-common module to ensure consistent temporal bucketing.
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
     * Increments datetime by the specified granularity
     */
    private ZonedDateTime incrementByGranularity(ZonedDateTime dateTime) {
        switch (granularity) {
            case HOUR:
                return dateTime.plusHours(1);
            case DAY:
                return dateTime.plusDays(1);
            case WEEK:
                return dateTime.plusWeeks(1);
            case MONTH:
                return dateTime.plusMonths(1);
            default:
                throw new IllegalArgumentException("Unsupported granularity: " + granularity);
        }
    }

    /**
     * Creates a temporal bucket key from a datetime
     *
     * IMPORTANT: This logic MUST be kept in sync with TemporalRoutingProcessor.createTemporalBucketKey()
     * in the ingest-common module. Both processors must generate identical bucket keys for the same
     * input to ensure documents are routed to the same shards during ingest and search.
     *
     * TODO: Consider moving this shared logic to a common module when search and ingest pipelines
     * can share code more easily.
     */
    private String createTemporalBucket(ZonedDateTime dateTime) {
        ZonedDateTime truncated = truncateToGranularity(dateTime);

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

    /**
     * Hashes temporal bucket for distribution
     */
    private String hashTemporalBucket(String temporalBucket) {
        byte[] bucketBytes = temporalBucket.getBytes(StandardCharsets.UTF_8);
        long hash = MurmurHash3.hash128(bucketBytes, 0, bucketBytes.length, 0, new MurmurHash3.Hash128()).h1;
        return String.valueOf(hash == Long.MIN_VALUE ? 0L : (hash < 0 ? -hash : hash));
    }

    /**
     * Factory for creating TemporalRoutingSearchProcessor instances
     */
    public static final class Factory implements Processor.Factory<SearchRequestProcessor> {

        /**
         * Creates a new Factory instance
         */
        public Factory() {}

        /**
         * Creates a new TemporalRoutingSearchProcessor instance
         *
         * @param processorFactories available processor factories
         * @param tag processor tag
         * @param description processor description
         * @param ignoreFailure whether to ignore failures
         * @param config processor configuration
         * @param pipelineContext pipeline context
         * @return new TemporalRoutingSearchProcessor instance
         * @throws Exception if configuration is invalid
         */
        @Override
        public TemporalRoutingSearchProcessor create(
            Map<String, Processor.Factory<SearchRequestProcessor>> processorFactories,
            String tag,
            String description,
            boolean ignoreFailure,
            Map<String, Object> config,
            Processor.PipelineContext pipelineContext
        ) throws Exception {

            String timestampField = ConfigurationUtils.readStringProperty(TYPE, tag, config, "timestamp_field");
            String granularityStr = ConfigurationUtils.readStringProperty(TYPE, tag, config, "granularity");
            String format = ConfigurationUtils.readOptionalStringProperty(TYPE, tag, config, "format");
            boolean enableAutoDetection = ConfigurationUtils.readBooleanProperty(TYPE, tag, config, "enable_auto_detection", true);
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

            return new TemporalRoutingSearchProcessor(
                tag,
                description,
                ignoreFailure,
                timestampField,
                granularity,
                format,
                enableAutoDetection,
                hashBucket
            );
        }
    }
}
