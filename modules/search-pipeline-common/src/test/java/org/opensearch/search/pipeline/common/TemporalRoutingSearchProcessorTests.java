/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline.common;

import org.opensearch.OpenSearchParseException;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.AbstractBuilderTestCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class TemporalRoutingSearchProcessorTests extends AbstractBuilderTestCase {

    public void testRangeQueryRouting() throws Exception {
        TemporalRoutingSearchProcessor processor = createProcessor("@timestamp", "day", "strict_date_optional_time", true, false);

        QueryBuilder query = new RangeQueryBuilder("@timestamp").from("2023-12-15T00:00:00Z").to("2023-12-15T23:59:59Z");
        SearchSourceBuilder source = new SearchSourceBuilder().query(query);
        SearchRequest request = new SearchRequest().source(source);

        SearchRequest transformedRequest = processor.processRequest(request);

        assertThat(transformedRequest.routing(), notNullValue());
        assertThat(transformedRequest.routing(), equalTo("2023-12-15"));
    }

    public void testMultiDayRangeQueryRouting() throws Exception {
        TemporalRoutingSearchProcessor processor = createProcessor("timestamp", "day", "strict_date_optional_time", true, false);

        QueryBuilder query = new RangeQueryBuilder("timestamp").from("2023-12-15T00:00:00Z").to("2023-12-17T23:59:59Z");
        SearchSourceBuilder source = new SearchSourceBuilder().query(query);
        SearchRequest request = new SearchRequest().source(source);

        SearchRequest transformedRequest = processor.processRequest(request);

        assertThat(transformedRequest.routing(), notNullValue());
        String routing = transformedRequest.routing();
        assertTrue("Should contain multiple routing values", routing.contains(","));
        assertTrue("Should contain 2023-12-15", routing.contains("2023-12-15"));
        assertTrue("Should contain 2023-12-16", routing.contains("2023-12-16"));
        assertTrue("Should contain 2023-12-17", routing.contains("2023-12-17"));
    }

    public void testHourlyGranularityRouting() throws Exception {
        TemporalRoutingSearchProcessor processor = createProcessor("timestamp", "hour", "strict_date_optional_time", true, false);

        QueryBuilder query = new RangeQueryBuilder("timestamp").from("2023-12-15T14:30:00Z").to("2023-12-15T16:45:00Z");
        SearchSourceBuilder source = new SearchSourceBuilder().query(query);
        SearchRequest request = new SearchRequest().source(source);

        SearchRequest transformedRequest = processor.processRequest(request);

        assertThat(transformedRequest.routing(), notNullValue());
        String routing = transformedRequest.routing();
        assertTrue("Should contain hourly buckets", routing.contains("2023-12-15T14"));
        assertTrue("Should contain hourly buckets", routing.contains("2023-12-15T15"));
        assertTrue("Should contain hourly buckets", routing.contains("2023-12-15T16"));
    }

    public void testWeeklyGranularityRouting() throws Exception {
        TemporalRoutingSearchProcessor processor = createProcessor("timestamp", "week", "strict_date_optional_time", true, false);

        QueryBuilder query = new RangeQueryBuilder("timestamp").from("2023-12-15T00:00:00Z")  // Week 50
            .to("2023-12-22T23:59:59Z");   // Week 51
        SearchSourceBuilder source = new SearchSourceBuilder().query(query);
        SearchRequest request = new SearchRequest().source(source);

        SearchRequest transformedRequest = processor.processRequest(request);

        assertThat(transformedRequest.routing(), notNullValue());
        String routing = transformedRequest.routing();
        assertTrue("Should contain weekly buckets", routing.contains("2023-W50"));
        assertTrue("Should contain weekly buckets", routing.contains("2023-W51"));
    }

    public void testMonthlyGranularityRouting() throws Exception {
        TemporalRoutingSearchProcessor processor = createProcessor("timestamp", "month", "strict_date_optional_time", true, false);

        QueryBuilder query = new RangeQueryBuilder("timestamp").from("2023-11-15T00:00:00Z").to("2024-01-15T23:59:59Z");
        SearchSourceBuilder source = new SearchSourceBuilder().query(query);
        SearchRequest request = new SearchRequest().source(source);

        SearchRequest transformedRequest = processor.processRequest(request);

        assertThat(transformedRequest.routing(), notNullValue());
        String routing = transformedRequest.routing();
        assertTrue("Should contain monthly buckets", routing.contains("2023-11"));
        assertTrue("Should contain monthly buckets", routing.contains("2023-12"));
        assertTrue("Should contain monthly buckets", routing.contains("2024-01"));
    }

    public void testHashBucketRouting() throws Exception {
        TemporalRoutingSearchProcessor processor = createProcessor("timestamp", "day", "strict_date_optional_time", true, true);

        QueryBuilder query = new RangeQueryBuilder("timestamp").from("2023-12-15T00:00:00Z").to("2023-12-15T23:59:59Z");
        SearchSourceBuilder source = new SearchSourceBuilder().query(query);
        SearchRequest request = new SearchRequest().source(source);

        SearchRequest transformedRequest = processor.processRequest(request);

        assertThat(transformedRequest.routing(), notNullValue());
        String routing = transformedRequest.routing();
        // Should be a numeric hash, not the date string
        assertThat(routing.matches("\\d+"), equalTo(true));
    }

    public void testBoolQueryWithRangeFilter() throws Exception {
        TemporalRoutingSearchProcessor processor = createProcessor("timestamp", "day", "strict_date_optional_time", true, false);

        QueryBuilder rangeFilter = new RangeQueryBuilder("timestamp").from("2023-12-15T00:00:00Z").to("2023-12-15T23:59:59Z");
        QueryBuilder textQuery = new TermQueryBuilder("content", "log message");
        QueryBuilder boolQuery = new BoolQueryBuilder().must(textQuery).filter(rangeFilter);

        SearchSourceBuilder source = new SearchSourceBuilder().query(boolQuery);
        SearchRequest request = new SearchRequest().source(source);

        SearchRequest transformedRequest = processor.processRequest(request);

        assertThat(transformedRequest.routing(), notNullValue());
        assertThat(transformedRequest.routing(), equalTo("2023-12-15"));
    }

    public void testShouldClauseIgnored() throws Exception {
        TemporalRoutingSearchProcessor processor = createProcessor("timestamp", "day", "strict_date_optional_time", true, false);

        // Query with range only in should clause
        QueryBuilder query = new BoolQueryBuilder().should(
            new RangeQueryBuilder("timestamp").from("2023-12-15T00:00:00Z").to("2023-12-15T23:59:59Z")
        );

        SearchSourceBuilder source = new SearchSourceBuilder().query(query);
        SearchRequest request = new SearchRequest().source(source);

        SearchRequest transformedRequest = processor.processRequest(request);

        // Should not add routing since range is only in should clause
        assertNull("Should not add routing for should clauses", transformedRequest.routing());
    }

    public void testNonTimestampFieldIgnored() throws Exception {
        TemporalRoutingSearchProcessor processor = createProcessor("timestamp", "day", "strict_date_optional_time", true, false);

        QueryBuilder query = new RangeQueryBuilder("other_field").from("2023-12-15T00:00:00Z").to("2023-12-15T23:59:59Z");
        SearchSourceBuilder source = new SearchSourceBuilder().query(query);
        SearchRequest request = new SearchRequest().source(source);

        SearchRequest transformedRequest = processor.processRequest(request);

        // Should not add routing for different field
        assertThat(transformedRequest.routing(), nullValue());
    }

    public void testExistingRoutingPreserved() throws Exception {
        TemporalRoutingSearchProcessor processor = createProcessor("timestamp", "day", "strict_date_optional_time", true, false);

        QueryBuilder query = new RangeQueryBuilder("timestamp").from("2023-12-15T00:00:00Z").to("2023-12-15T23:59:59Z");
        SearchSourceBuilder source = new SearchSourceBuilder().query(query);
        SearchRequest request = new SearchRequest().source(source).routing("existing_routing");

        SearchRequest transformedRequest = processor.processRequest(request);

        // Existing routing should be preserved
        assertThat(transformedRequest.routing(), equalTo("existing_routing"));
    }

    public void testEmptySearchRequest() throws Exception {
        TemporalRoutingSearchProcessor processor = createProcessor("timestamp", "day", "strict_date_optional_time", true, false);

        SearchRequest request = new SearchRequest();
        SearchRequest transformedRequest = processor.processRequest(request);

        // No routing should be added for empty request
        assertThat(transformedRequest.routing(), nullValue());
    }

    public void testInvalidDateFormatIgnored() throws Exception {
        TemporalRoutingSearchProcessor processor = createProcessor("timestamp", "day", "strict_date_optional_time", true, false);

        QueryBuilder query = new RangeQueryBuilder("timestamp").from("invalid-date").to("another-invalid-date");
        SearchSourceBuilder source = new SearchSourceBuilder().query(query);
        SearchRequest request = new SearchRequest().source(source);

        // Should not throw exception, should fallback to no routing
        SearchRequest transformedRequest = processor.processRequest(request);
        assertThat(transformedRequest.routing(), nullValue());
    }

    public void testCustomDateFormat() throws Exception {
        TemporalRoutingSearchProcessor processor = createProcessor("timestamp", "day", "yyyy-MM-dd HH:mm:ss", true, false);

        QueryBuilder query = new RangeQueryBuilder("timestamp").from("2023-12-15 00:00:00").to("2023-12-15 23:59:59");
        SearchSourceBuilder source = new SearchSourceBuilder().query(query);
        SearchRequest request = new SearchRequest().source(source);

        SearchRequest transformedRequest = processor.processRequest(request);

        assertThat(transformedRequest.routing(), notNullValue());
        assertThat(transformedRequest.routing(), equalTo("2023-12-15"));
    }

    public void testOpenEndedRanges() throws Exception {
        TemporalRoutingSearchProcessor processor = createProcessor("timestamp", "day", "strict_date_optional_time", true, false);

        // Test range with only lower bound
        QueryBuilder queryFrom = new RangeQueryBuilder("timestamp").from("2023-12-15T00:00:00Z");
        SearchSourceBuilder sourceFrom = new SearchSourceBuilder().query(queryFrom);
        SearchRequest requestFrom = new SearchRequest().source(sourceFrom);

        SearchRequest transformedRequestFrom = processor.processRequest(requestFrom);
        assertThat(transformedRequestFrom.routing(), equalTo("2023-12-15"));

        // Test range with only upper bound
        QueryBuilder queryTo = new RangeQueryBuilder("timestamp").to("2023-12-15T23:59:59Z");
        SearchSourceBuilder sourceTo = new SearchSourceBuilder().query(queryTo);
        SearchRequest requestTo = new SearchRequest().source(sourceTo);

        SearchRequest transformedRequestTo = processor.processRequest(requestTo);
        assertThat(transformedRequestTo.routing(), equalTo("2023-12-15"));
    }

    public void testConsistentRoutingForSameTimeWindow() throws Exception {
        TemporalRoutingSearchProcessor processor = createProcessor("timestamp", "day", "strict_date_optional_time", true, true);

        // Different ranges within same day should produce same routing
        QueryBuilder query1 = new RangeQueryBuilder("timestamp").from("2023-12-15T08:00:00Z").to("2023-12-15T12:00:00Z");

        QueryBuilder query2 = new RangeQueryBuilder("timestamp").from("2023-12-15T14:00:00Z").to("2023-12-15T18:00:00Z");

        SearchSourceBuilder source1 = new SearchSourceBuilder().query(query1);
        SearchSourceBuilder source2 = new SearchSourceBuilder().query(query2);
        SearchRequest request1 = new SearchRequest().source(source1);
        SearchRequest request2 = new SearchRequest().source(source2);

        SearchRequest result1 = processor.processRequest(request1);
        SearchRequest result2 = processor.processRequest(request2);

        // Should have same routing since they're on the same day
        assertThat(result1.routing(), equalTo(result2.routing()));
    }

    public void testFactory() throws Exception {
        TemporalRoutingSearchProcessor.Factory factory = new TemporalRoutingSearchProcessor.Factory();

        Map<String, Object> config = new HashMap<>();
        config.put("timestamp_field", "@timestamp");
        config.put("granularity", "day");
        config.put("format", "strict_date_optional_time");
        config.put("enable_auto_detection", true);
        config.put("hash_bucket", false);

        TemporalRoutingSearchProcessor processor = factory.create(Collections.emptyMap(), "test", "test processor", false, config, null);

        assertThat(processor.getType(), equalTo(TemporalRoutingSearchProcessor.TYPE));
    }

    public void testFactoryValidation() throws Exception {
        TemporalRoutingSearchProcessor.Factory factory = new TemporalRoutingSearchProcessor.Factory();

        // Test missing timestamp_field
        Map<String, Object> config1 = new HashMap<>();
        config1.put("granularity", "day");
        OpenSearchParseException exception = expectThrows(
            OpenSearchParseException.class,
            () -> factory.create(Collections.emptyMap(), "test", null, false, config1, null)
        );
        assertThat(exception.getMessage(), containsString("timestamp_field"));

        // Test missing granularity
        Map<String, Object> config2 = new HashMap<>();
        config2.put("timestamp_field", "@timestamp");
        exception = expectThrows(
            OpenSearchParseException.class,
            () -> factory.create(Collections.emptyMap(), "test", null, false, config2, null)
        );
        assertThat(exception.getMessage(), containsString("granularity"));

        // Test invalid granularity
        Map<String, Object> config3 = new HashMap<>();
        config3.put("timestamp_field", "@timestamp");
        config3.put("granularity", "invalid");
        exception = expectThrows(
            OpenSearchParseException.class,
            () -> factory.create(Collections.emptyMap(), "test", null, false, config3, null)
        );
        assertThat(exception.getMessage(), containsString("Invalid granularity"));

        // Test invalid format
        Map<String, Object> config4 = new HashMap<>();
        config4.put("timestamp_field", "@timestamp");
        config4.put("granularity", "day");
        config4.put("format", "invalid_format");
        exception = expectThrows(
            OpenSearchParseException.class,
            () -> factory.create(Collections.emptyMap(), "test", null, false, config4, null)
        );
        assertThat(exception.getMessage(), containsString("invalid date format"));
    }

    // Helper method to create processor
    private TemporalRoutingSearchProcessor createProcessor(
        String timestampField,
        String granularity,
        String format,
        boolean enableAutoDetection,
        boolean hashBucket
    ) {
        return new TemporalRoutingSearchProcessor(
            "test",
            "test processor",
            false,
            timestampField,
            TemporalRoutingSearchProcessor.Granularity.fromString(granularity),
            format,
            enableAutoDetection,
            hashBucket
        );
    }
}
