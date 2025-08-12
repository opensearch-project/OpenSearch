/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ingest.common;

import org.opensearch.OpenSearchParseException;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class TemporalRoutingProcessorTests extends OpenSearchTestCase {

    public void testExecuteHourlyGranularity() throws Exception {
        TemporalRoutingProcessor processor = createProcessor("@timestamp", "hour", "strict_date_optional_time", false, true, false);

        Map<String, Object> document = new HashMap<>();
        document.put("@timestamp", "2023-12-15T14:30:45.123Z");
        IngestDocument ingestDocument = new IngestDocument("index", "id", null, null, null, document);

        IngestDocument result = processor.execute(ingestDocument);

        assertThat(result.getFieldValue("_routing", String.class), equalTo("2023-12-15T14"));
    }

    public void testExecuteDailyGranularity() throws Exception {
        TemporalRoutingProcessor processor = createProcessor("timestamp", "day", "strict_date_optional_time", false, true, false);

        Map<String, Object> document = new HashMap<>();
        document.put("timestamp", "2023-12-15T14:30:45.123Z");
        IngestDocument ingestDocument = new IngestDocument("index", "id", null, null, null, document);

        IngestDocument result = processor.execute(ingestDocument);

        assertThat(result.getFieldValue("_routing", String.class), equalTo("2023-12-15"));
    }

    public void testExecuteWeeklyGranularity() throws Exception {
        TemporalRoutingProcessor processor = createProcessor("timestamp", "week", "strict_date_optional_time", false, true, false);

        Map<String, Object> document = new HashMap<>();
        // Friday, December 15, 2023 - should be week 50 of 2023
        document.put("timestamp", "2023-12-15T14:30:45.123Z");
        IngestDocument ingestDocument = new IngestDocument("index", "id", null, null, null, document);

        IngestDocument result = processor.execute(ingestDocument);

        assertThat(result.getFieldValue("_routing", String.class), equalTo("2023-W50"));
    }

    public void testExecuteMonthlyGranularity() throws Exception {
        TemporalRoutingProcessor processor = createProcessor("timestamp", "month", "strict_date_optional_time", false, true, false);

        Map<String, Object> document = new HashMap<>();
        document.put("timestamp", "2023-12-15T14:30:45.123Z");
        IngestDocument ingestDocument = new IngestDocument("index", "id", null, null, null, document);

        IngestDocument result = processor.execute(ingestDocument);

        assertThat(result.getFieldValue("_routing", String.class), equalTo("2023-12"));
    }

    public void testExecuteWithHashBucket() throws Exception {
        TemporalRoutingProcessor processor = createProcessor("timestamp", "day", "strict_date_optional_time", false, true, true);

        Map<String, Object> document = new HashMap<>();
        document.put("timestamp", "2023-12-15T14:30:45.123Z");
        IngestDocument ingestDocument = new IngestDocument("index", "id", null, null, null, document);

        IngestDocument result = processor.execute(ingestDocument);

        String routing = result.getFieldValue("_routing", String.class);
        assertThat(routing, notNullValue());
        // Should be a numeric hash, not the date string
        assertThat(routing.matches("\\d+"), equalTo(true));
    }

    public void testIgnoreMissingField() throws Exception {
        TemporalRoutingProcessor processor = createProcessor("missing_field", "day", "strict_date_optional_time", true, true, false);

        Map<String, Object> document = new HashMap<>();
        document.put("other_field", "value");
        IngestDocument ingestDocument = new IngestDocument("index", "id", null, null, null, document);

        IngestDocument result = processor.execute(ingestDocument);

        // Should not have added routing
        assertFalse(result.hasField("_routing"));
    }

    public void testMissingFieldThrowsException() throws Exception {
        TemporalRoutingProcessor processor = createProcessor("missing_field", "day", "strict_date_optional_time", false, true, false);

        Map<String, Object> document = new HashMap<>();
        document.put("other_field", "value");
        IngestDocument ingestDocument = new IngestDocument("index", "id", null, null, null, document);

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> processor.execute(ingestDocument));
        assertThat(exception.getMessage(), containsString("field [missing_field] not present as part of path [missing_field]"));
    }

    public void testOverrideExistingRouting() throws Exception {
        TemporalRoutingProcessor processor = createProcessor("timestamp", "day", "strict_date_optional_time", false, true, false);

        Map<String, Object> document = new HashMap<>();
        document.put("timestamp", "2023-12-15T14:30:45.123Z");
        document.put("_routing", "existing_routing");
        IngestDocument ingestDocument = new IngestDocument("index", "id", null, null, null, document);

        IngestDocument result = processor.execute(ingestDocument);

        // Should override existing routing
        assertThat(result.getFieldValue("_routing", String.class), equalTo("2023-12-15"));
    }

    public void testPreserveExistingRouting() throws Exception {
        TemporalRoutingProcessor processor = createProcessor("timestamp", "day", "strict_date_optional_time", false, false, false);

        Map<String, Object> document = new HashMap<>();
        document.put("timestamp", "2023-12-15T14:30:45.123Z");
        document.put("_routing", "existing_routing");
        IngestDocument ingestDocument = new IngestDocument("index", "id", null, null, null, document);

        IngestDocument result = processor.execute(ingestDocument);

        // Should preserve existing routing
        assertThat(result.getFieldValue("_routing", String.class), equalTo("existing_routing"));
    }

    public void testInvalidDateFormat() throws Exception {
        TemporalRoutingProcessor processor = createProcessor("timestamp", "day", "strict_date_optional_time", false, true, false);

        Map<String, Object> document = new HashMap<>();
        document.put("timestamp", "invalid-date");
        IngestDocument ingestDocument = new IngestDocument("index", "id", null, null, null, document);

        expectThrows(Exception.class, () -> processor.execute(ingestDocument));
    }

    public void testCustomDateFormat() throws Exception {
        TemporalRoutingProcessor processor = createProcessor("timestamp", "day", "yyyy-MM-dd HH:mm:ss", false, true, false);

        Map<String, Object> document = new HashMap<>();
        document.put("timestamp", "2023-12-15 14:30:45");
        IngestDocument ingestDocument = new IngestDocument("index", "id", null, null, null, document);

        IngestDocument result = processor.execute(ingestDocument);

        assertThat(result.getFieldValue("_routing", String.class), equalTo("2023-12-15"));
    }

    public void testDifferentTimestampFields() throws Exception {
        TemporalRoutingProcessor processor1 = createProcessor("created_at", "day", "strict_date_optional_time", false, true, false);
        TemporalRoutingProcessor processor2 = createProcessor("updated_at", "day", "strict_date_optional_time", false, true, false);

        Map<String, Object> document = new HashMap<>();
        document.put("created_at", "2023-12-15T14:30:45.123Z");
        document.put("updated_at", "2023-12-20T10:15:30.456Z");

        IngestDocument ingestDocument1 = new IngestDocument("index", "id", null, null, null, new HashMap<>(document));
        IngestDocument result1 = processor1.execute(ingestDocument1);
        assertThat(result1.getFieldValue("_routing", String.class), equalTo("2023-12-15"));

        IngestDocument ingestDocument2 = new IngestDocument("index", "id", null, null, null, new HashMap<>(document));
        IngestDocument result2 = processor2.execute(ingestDocument2);
        assertThat(result2.getFieldValue("_routing", String.class), equalTo("2023-12-20"));
    }

    public void testFactory() throws Exception {
        TemporalRoutingProcessor.Factory factory = new TemporalRoutingProcessor.Factory();

        Map<String, Object> config = new HashMap<>();
        config.put("timestamp_field", "@timestamp");
        config.put("granularity", "day");
        config.put("format", "strict_date_optional_time");
        config.put("ignore_missing", false);
        config.put("override_existing", true);
        config.put("hash_bucket", false);

        TemporalRoutingProcessor processor = factory.create(Collections.emptyMap(), "test", "test processor", config);

        assertThat(processor.getType(), equalTo(TemporalRoutingProcessor.TYPE));
        assertThat(processor.getTimestampField(), equalTo("@timestamp"));
        assertThat(processor.getGranularity(), equalTo(TemporalRoutingProcessor.Granularity.DAY));
        assertThat(processor.isIgnoreMissing(), equalTo(false));
        assertThat(processor.isOverrideExisting(), equalTo(true));
        assertThat(processor.isHashBucket(), equalTo(false));
    }

    public void testFactoryValidation() throws Exception {
        TemporalRoutingProcessor.Factory factory = new TemporalRoutingProcessor.Factory();

        // Test missing timestamp_field
        Map<String, Object> config1 = new HashMap<>();
        config1.put("granularity", "day");
        OpenSearchParseException exception = expectThrows(
            OpenSearchParseException.class,
            () -> factory.create(Collections.emptyMap(), "test", null, config1)
        );
        assertThat(exception.getMessage(), containsString("timestamp_field"));

        // Test missing granularity
        Map<String, Object> config2 = new HashMap<>();
        config2.put("timestamp_field", "@timestamp");
        exception = expectThrows(OpenSearchParseException.class, () -> factory.create(Collections.emptyMap(), "test", null, config2));
        assertThat(exception.getMessage(), containsString("granularity"));

        // Test invalid granularity
        Map<String, Object> config3 = new HashMap<>();
        config3.put("timestamp_field", "@timestamp");
        config3.put("granularity", "invalid");
        exception = expectThrows(OpenSearchParseException.class, () -> factory.create(Collections.emptyMap(), "test", null, config3));
        assertThat(exception.getMessage(), containsString("Invalid granularity"));

        // Test invalid format
        Map<String, Object> config4 = new HashMap<>();
        config4.put("timestamp_field", "@timestamp");
        config4.put("granularity", "day");
        config4.put("format", "invalid_format");
        exception = expectThrows(OpenSearchParseException.class, () -> factory.create(Collections.emptyMap(), "test", null, config4));
        assertThat(exception.getMessage(), containsString("invalid date format"));
    }

    public void testAllGranularityTypes() throws Exception {
        String timestamp = "2023-12-15T14:30:45.123Z";

        // Test all granularity types
        TemporalRoutingProcessor hourProcessor = createProcessor("timestamp", "hour", "strict_date_optional_time", false, true, false);
        TemporalRoutingProcessor dayProcessor = createProcessor("timestamp", "day", "strict_date_optional_time", false, true, false);
        TemporalRoutingProcessor weekProcessor = createProcessor("timestamp", "week", "strict_date_optional_time", false, true, false);
        TemporalRoutingProcessor monthProcessor = createProcessor("timestamp", "month", "strict_date_optional_time", false, true, false);

        Map<String, Object> document = new HashMap<>();
        document.put("timestamp", timestamp);

        IngestDocument hourDoc = new IngestDocument("index", "id", null, null, null, new HashMap<>(document));
        IngestDocument dayDoc = new IngestDocument("index", "id", null, null, null, new HashMap<>(document));
        IngestDocument weekDoc = new IngestDocument("index", "id", null, null, null, new HashMap<>(document));
        IngestDocument monthDoc = new IngestDocument("index", "id", null, null, null, new HashMap<>(document));

        assertThat(hourProcessor.execute(hourDoc).getFieldValue("_routing", String.class), equalTo("2023-12-15T14"));
        assertThat(dayProcessor.execute(dayDoc).getFieldValue("_routing", String.class), equalTo("2023-12-15"));
        assertThat(weekProcessor.execute(weekDoc).getFieldValue("_routing", String.class), equalTo("2023-W50"));
        assertThat(monthProcessor.execute(monthDoc).getFieldValue("_routing", String.class), equalTo("2023-12"));
    }

    public void testConsistentHashingForSameTimeWindow() throws Exception {
        TemporalRoutingProcessor processor = createProcessor("timestamp", "day", "strict_date_optional_time", false, true, true);

        // Different timestamps within same day should produce same routing
        Map<String, Object> document1 = new HashMap<>();
        document1.put("timestamp", "2023-12-15T08:00:00Z");

        Map<String, Object> document2 = new HashMap<>();
        document2.put("timestamp", "2023-12-15T16:30:45Z");

        IngestDocument ingestDoc1 = new IngestDocument("index", "id1", null, null, null, document1);
        IngestDocument ingestDoc2 = new IngestDocument("index", "id2", null, null, null, document2);

        IngestDocument result1 = processor.execute(ingestDoc1);
        IngestDocument result2 = processor.execute(ingestDoc2);

        // Should have same routing since they're on the same day
        assertThat(result1.getFieldValue("_routing", String.class), equalTo(result2.getFieldValue("_routing", String.class)));
    }

    // Helper method to create processor
    private TemporalRoutingProcessor createProcessor(
        String timestampField,
        String granularity,
        String format,
        boolean ignoreMissing,
        boolean overrideExisting,
        boolean hashBucket
    ) {
        return new TemporalRoutingProcessor(
            "test",
            "test processor",
            timestampField,
            TemporalRoutingProcessor.Granularity.fromString(granularity),
            format,
            ignoreMissing,
            overrideExisting,
            hashBucket
        );
    }
}
