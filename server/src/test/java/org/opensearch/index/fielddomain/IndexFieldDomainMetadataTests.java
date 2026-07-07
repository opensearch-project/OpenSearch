/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.fielddomain;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class IndexFieldDomainMetadataTests extends OpenSearchTestCase {
    private static final IndexFieldDomainMetadata METADATA = IndexFieldDomainMetadata.getInstance();

    public void testCustomMetadataKeyIsStable() {
        assertThat(IndexFieldDomainMetadata.CUSTOM_KEY, equalTo("index_field_domains"));
    }

    public void testFromCustomDataReadsDateRangeDomain() {
        Map<String, String> customData = Map.of(
            "fields.event.ingested.type",
            "date_range",
            "fields.event.ingested.min",
            "1714521600000",
            "fields.event.ingested.max",
            "1717200000000",
            "fields.event.ingested.finalized",
            "true",
            "fields.event.ingested.source",
            "test_producer",
            "fields.event.ingested.format",
            "strict_date_optional_time",
            "fields.event.ingested.resolution",
            "milliseconds"
        );

        Optional<FieldDomain> maybeDomain = METADATA.fromCustomData(customData, "event.ingested");

        assertTrue(maybeDomain.isPresent());
        assertThat(maybeDomain.get(), instanceOf(DateRangeFieldDomain.class));

        DateRangeFieldDomain domain = (DateRangeFieldDomain) maybeDomain.get();
        assertThat(domain.field(), equalTo("event.ingested"));
        assertThat(domain.type(), equalTo(DateRangeFieldDomain.TYPE));
        assertThat(domain.min(), equalTo("1714521600000"));
        assertThat(domain.max(), equalTo("1717200000000"));
        assertTrue(domain.finalized());
        assertThat(domain.source(), equalTo("test_producer"));
        assertThat(domain.format(), equalTo("strict_date_optional_time"));
        assertThat(domain.resolution(), equalTo("milliseconds"));
    }

    public void testFromCustomDataReturnsEmptyForMissingUnknownOrMalformedMetadata() {
        assertTrue(METADATA.fromCustomData(null, "@timestamp").isEmpty());
        assertTrue(METADATA.fromCustomData(Map.of(), "@timestamp").isEmpty());
        assertTrue(METADATA.fromCustomData(Map.of("fields.@timestamp.type", "unknown"), "@timestamp").isEmpty());
        assertTrue(
            METADATA.fromCustomData(
                Map.of(
                    "fields.@timestamp.type",
                    "date_range",
                    "fields.@timestamp.min",
                    "100",
                    "fields.@timestamp.max",
                    "200",
                    "fields.@timestamp.finalized",
                    "not-a-boolean",
                    "fields.@timestamp.resolution",
                    "milliseconds"
                ),
                "@timestamp"
            ).isEmpty()
        );
    }

    public void testFromCustomDataReturnsEmptyWhenDateRangeRequiredKeysAreMissing() {
        assertTrue(
            METADATA.fromCustomData(
                Map.of("fields.@timestamp.type", "date_range", "fields.@timestamp.max", "200", "fields.@timestamp.finalized", "true"),
                "@timestamp"
            ).isEmpty()
        );
        assertTrue(
            METADATA.fromCustomData(
                Map.of("fields.@timestamp.type", "date_range", "fields.@timestamp.min", "100", "fields.@timestamp.finalized", "true"),
                "@timestamp"
            ).isEmpty()
        );
        assertTrue(
            METADATA.fromCustomData(
                Map.of("fields.@timestamp.type", "date_range", "fields.@timestamp.min", "100", "fields.@timestamp.max", "200"),
                "@timestamp"
            ).isEmpty()
        );
    }

    public void testFromCustomDataReturnsEmptyWhenRequestedFieldIsMissing() {
        Map<String, String> customData = Map.of(
            "fields.event.created.type",
            "date_range",
            "fields.event.created.min",
            "100",
            "fields.event.created.max",
            "200",
            "fields.event.created.finalized",
            "true"
        );

        assertTrue(METADATA.fromCustomData(customData, "event.ingested").isEmpty());
    }

    public void testFromCustomDataReadsDateRangeDomainWithOptionalSourceAndFormatOmitted() {
        Optional<FieldDomain> maybeDomain = METADATA.fromCustomData(
            Map.of(
                "fields.@timestamp.type",
                "date_range",
                "fields.@timestamp.min",
                "100",
                "fields.@timestamp.max",
                "200",
                "fields.@timestamp.finalized",
                "true",
                "fields.@timestamp.resolution",
                "milliseconds"
            ),
            "@timestamp"
        );

        assertTrue(maybeDomain.isPresent());
        DateRangeFieldDomain domain = (DateRangeFieldDomain) maybeDomain.get();
        assertNull(domain.source());
        assertNull(domain.format());
        assertThat(domain.resolution(), equalTo("milliseconds"));
    }

    public void testFromCustomDataReturnsEmptyForInvalidDateRangeValues() {
        assertTrue(
            METADATA.fromCustomData(
                Map.of(
                    "fields.@timestamp.type",
                    "date_range",
                    "fields.@timestamp.min",
                    "bad",
                    "fields.@timestamp.max",
                    "200",
                    "fields.@timestamp.finalized",
                    "true",
                    "fields.@timestamp.resolution",
                    "milliseconds"
                ),
                "@timestamp"
            ).isEmpty()
        );
        assertTrue(
            METADATA.fromCustomData(
                Map.of(
                    "fields.@timestamp.type",
                    "date_range",
                    "fields.@timestamp.min",
                    "200",
                    "fields.@timestamp.max",
                    "100",
                    "fields.@timestamp.finalized",
                    "true",
                    "fields.@timestamp.resolution",
                    "milliseconds"
                ),
                "@timestamp"
            ).isEmpty()
        );
        assertTrue(
            METADATA.fromCustomData(
                Map.of(
                    "fields.@timestamp.type",
                    "date_range",
                    "fields.@timestamp.min",
                    "100",
                    "fields.@timestamp.max",
                    "200",
                    "fields.@timestamp.finalized",
                    "true",
                    "fields.@timestamp.resolution",
                    "unsupported"
                ),
                "@timestamp"
            ).isEmpty()
        );
        assertTrue(
            METADATA.fromCustomData(
                Map.of(
                    "fields.@timestamp.type",
                    "date_range",
                    "fields.@timestamp.min",
                    "100",
                    "fields.@timestamp.max",
                    "200",
                    "fields.@timestamp.finalized",
                    "true"
                ),
                "@timestamp"
            ).isEmpty()
        );
    }

    public void testToCustomDataWritesDateRangeDomain() {
        Map<String, String> customData = METADATA.toCustomData(
            new DateRangeFieldDomain("@timestamp", "100", "200", true, "test", null, "milliseconds")
        );

        assertThat(customData.get("fields.@timestamp.type"), equalTo("date_range"));
        assertThat(customData.get("fields.@timestamp.min"), equalTo("100"));
        assertThat(customData.get("fields.@timestamp.max"), equalTo("200"));
        assertThat(customData.get("fields.@timestamp.finalized"), equalTo("true"));
        assertThat(customData.get("fields.@timestamp.source"), equalTo("test"));
        assertFalse(customData.containsKey("fields.@timestamp.format"));
        assertThat(customData.get("fields.@timestamp.resolution"), equalTo("milliseconds"));
    }

    public void testToCustomDataReturnsImmutableMap() {
        Map<String, String> customData = METADATA.toCustomData(new DateRangeFieldDomain("@timestamp", 100L, 200L, true, "test"));

        expectThrows(UnsupportedOperationException.class, () -> customData.put("fields.@timestamp.min", "300"));
    }

    public void testToCustomDataRejectsUnsupportedDomainType() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> METADATA.toCustomData(new UnsupportedFieldDomain("host.name"))
        );

        assertThat(exception.getMessage(), equalTo("unsupported field domain type [unsupported]"));
    }

    public void testPutFieldDomainCreatesCustomDataWhenMissing() {
        IndexMetadata metadata = indexMetadataBuilder("logs-000001").build();

        IndexMetadata updated = METADATA.putFieldDomain(metadata, new DateRangeFieldDomain("@timestamp", 100L, 200L, true, "test"));

        Map<String, String> customData = updated.getCustomData(IndexFieldDomainMetadata.CUSTOM_KEY);
        assertThat(customData.get("fields.@timestamp.type"), equalTo("date_range"));
        assertThat(customData.get("fields.@timestamp.min"), equalTo("100"));
        assertThat(customData.get("fields.@timestamp.max"), equalTo("200"));
        assertThat(customData.get("fields.@timestamp.finalized"), equalTo("true"));
        assertThat(customData.get("fields.@timestamp.resolution"), equalTo("milliseconds"));
    }

    public void testPutFieldDomainReplacesOnlyTargetFieldMetadata() {
        Map<String, String> existing = new HashMap<>();
        existing.put("fields.@timestamp.type", "date_range");
        existing.put("fields.@timestamp.min", "1");
        existing.put("fields.@timestamp.max", "2");
        existing.put("fields.@timestamp.finalized", "true");
        existing.put("fields.host.name.type", "term_set");
        existing.put("fields.host.name.value", "server-a");
        existing.put("producer", "existing");

        IndexMetadata metadata = indexMetadataBuilder("logs-000001").putCustom(IndexFieldDomainMetadata.CUSTOM_KEY, existing).build();

        IndexMetadata updated = METADATA.putFieldDomain(metadata, new DateRangeFieldDomain("@timestamp", 100L, 200L, true, "ism_rollover"));

        Map<String, String> customData = updated.getCustomData(IndexFieldDomainMetadata.CUSTOM_KEY);
        assertThat(customData.get("fields.@timestamp.type"), equalTo("date_range"));
        assertThat(customData.get("fields.@timestamp.min"), equalTo("100"));
        assertThat(customData.get("fields.@timestamp.max"), equalTo("200"));
        assertThat(customData.get("fields.@timestamp.finalized"), equalTo("true"));
        assertThat(customData.get("fields.@timestamp.source"), equalTo("ism_rollover"));
        assertThat(customData.get("fields.host.name.type"), equalTo("term_set"));
        assertThat(customData.get("fields.host.name.value"), equalTo("server-a"));
        assertThat(customData.get("producer"), equalTo("existing"));
    }

    public void testPutFieldDomainPreservesFieldsWithSharedPrefixes() {
        Map<String, String> existing = new HashMap<>();
        existing.put("fields.event.type", "keyword_set");
        existing.put("fields.event.value", "audit");
        existing.put("fields.event.ingested.type", "date_range");
        existing.put("fields.event.ingested.min", "1");
        existing.put("fields.event.ingested.max", "2");
        existing.put("fields.event.ingested.finalized", "true");
        existing.put("fields.event.ingested.raw.type", "keyword_set");
        existing.put("fields.event.ingested.raw.value", "2024-01-01");

        IndexMetadata metadata = indexMetadataBuilder("logs-000001").putCustom(IndexFieldDomainMetadata.CUSTOM_KEY, existing).build();

        IndexMetadata updated = METADATA.putFieldDomain(metadata, new DateRangeFieldDomain("event.ingested", 100L, 200L, true, "test"));

        Map<String, String> customData = updated.getCustomData(IndexFieldDomainMetadata.CUSTOM_KEY);
        assertThat(customData.get("fields.event.type"), equalTo("keyword_set"));
        assertThat(customData.get("fields.event.value"), equalTo("audit"));
        assertThat(customData.get("fields.event.ingested.type"), equalTo("date_range"));
        assertThat(customData.get("fields.event.ingested.min"), equalTo("100"));
        assertThat(customData.get("fields.event.ingested.max"), equalTo("200"));
        assertThat(customData.get("fields.event.ingested.raw.type"), equalTo("keyword_set"));
        assertThat(customData.get("fields.event.ingested.raw.value"), equalTo("2024-01-01"));
    }

    public void testPutFieldDomainRemovesStaleKnownKeysForTargetField() {
        Map<String, String> existing = new HashMap<>();
        existing.put("fields.@timestamp.type", "date_range");
        existing.put("fields.@timestamp.min", "1");
        existing.put("fields.@timestamp.max", "2");
        existing.put("fields.@timestamp.finalized", "true");
        existing.put("fields.@timestamp.source", "old_source");
        existing.put("fields.@timestamp.format", "strict_date_optional_time");
        existing.put("fields.@timestamp.resolution", "nanoseconds");
        existing.put("fields.@timestamp.custom", "preserved");

        IndexMetadata metadata = indexMetadataBuilder("logs-000001").putCustom(IndexFieldDomainMetadata.CUSTOM_KEY, existing).build();

        IndexMetadata updated = METADATA.putFieldDomain(
            metadata,
            new DateRangeFieldDomain("@timestamp", "100", "200", true, null, null, "milliseconds")
        );

        Map<String, String> customData = updated.getCustomData(IndexFieldDomainMetadata.CUSTOM_KEY);
        assertThat(customData.get("fields.@timestamp.min"), equalTo("100"));
        assertThat(customData.get("fields.@timestamp.max"), equalTo("200"));
        assertFalse(customData.containsKey("fields.@timestamp.source"));
        assertFalse(customData.containsKey("fields.@timestamp.format"));
        assertThat(customData.get("fields.@timestamp.resolution"), equalTo("milliseconds"));
        assertThat(customData.get("fields.@timestamp.custom"), equalTo("preserved"));
    }

    public void testPutFieldDomainRejectsNullInputs() {
        IndexMetadata metadata = indexMetadataBuilder("logs-000001").build();

        expectThrows(
            NullPointerException.class,
            () -> METADATA.putFieldDomain(null, new DateRangeFieldDomain("@timestamp", 1L, 2L, true, "test"))
        );
        expectThrows(NullPointerException.class, () -> METADATA.putFieldDomain(metadata, null));
    }

    private static IndexMetadata.Builder indexMetadataBuilder(String index) {
        return IndexMetadata.builder(index)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, "_na_")
                    .build()
            )
            .numberOfShards(1)
            .numberOfReplicas(0);
    }

    private static final class UnsupportedFieldDomain implements FieldDomain {
        private final String field;

        private UnsupportedFieldDomain(String field) {
            this.field = field;
        }

        @Override
        public String field() {
            return field;
        }

        @Override
        public String type() {
            return "unsupported";
        }

        @Override
        public boolean finalized() {
            return true;
        }
    }
}
