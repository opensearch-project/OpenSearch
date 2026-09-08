/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.fielddomain;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.index.Index;
import org.opensearch.index.fielddomain.DateRangeFieldDomain;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class PutIndexFieldDomainsRequestTests extends OpenSearchTestCase {
    public void testValidationRequiresIndexAndMetadata() {
        PutIndexFieldDomainsRequest request = new PutIndexFieldDomainsRequest((Index) null);

        ActionRequestValidationException validationException = request.validate();

        assertNotNull(validationException);
        assertTrue(validationException.validationErrors().contains("target index is required"));
        assertTrue(validationException.validationErrors().contains("field domain metadata is required"));
    }

    public void testValidationRequiresTargetIndexUUID() {
        PutIndexFieldDomainsRequest request = new PutIndexFieldDomainsRequest(new Index("logs-000001", Strings.UNKNOWN_UUID_VALUE))
            .fieldDomain(new DateRangeFieldDomain("@timestamp", 100L, 200L, true, "test"));

        ActionRequestValidationException validationException = request.validate();

        assertNotNull(validationException);
        assertTrue(validationException.validationErrors().contains("target index UUID is required"));
    }

    public void testFieldDomainSetterEncodesMetadata() {
        PutIndexFieldDomainsRequest request = new PutIndexFieldDomainsRequest(new Index("logs-000001", "index-uuid")).fieldDomain(
            new DateRangeFieldDomain("@timestamp", 100L, 200L, true, "test")
        );

        Map<String, String> customData = request.fieldDomainCustomData();
        assertThat(customData.get("fields.@timestamp.type"), equalTo("date_range"));
        assertThat(customData.get("fields.@timestamp.min"), equalTo("100"));
        assertThat(customData.get("fields.@timestamp.max"), equalTo("200"));
        assertThat(customData.get("fields.@timestamp.finalized"), equalTo("true"));
        assertThat(customData.get("fields.@timestamp.resolution"), equalTo("milliseconds"));
    }

    public void testFieldDomainSetterAddsDomains() {
        PutIndexFieldDomainsRequest request = new PutIndexFieldDomainsRequest(new Index("logs-000001", "index-uuid")).fieldDomain(
            new DateRangeFieldDomain("@timestamp", 100L, 200L, true, "test")
        ).fieldDomain(new DateRangeFieldDomain("event.ingested", 300L, 400L, true, "test"));

        Map<String, String> customData = request.fieldDomainCustomData();
        assertThat(customData.get("fields.@timestamp.max"), equalTo("200"));
        assertThat(customData.get("fields.event.ingested.max"), equalTo("400"));
    }

    public void testFieldDomainSetterRejectsDuplicateField() {
        PutIndexFieldDomainsRequest request = new PutIndexFieldDomainsRequest(new Index("logs-000001", "index-uuid")).fieldDomain(
            new DateRangeFieldDomain("@timestamp", 100L, 200L, true, "test")
        );

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> request.fieldDomain(new DateRangeFieldDomain("@timestamp", 300L, 400L, true, "test"))
        );

        assertThat(exception.getMessage(), equalTo("duplicate field domain for field [@timestamp]"));
    }

    public void testFieldDomainsSetterEncodesMultipleDomains() {
        PutIndexFieldDomainsRequest request = new PutIndexFieldDomainsRequest(new Index("logs-000001", "index-uuid")).fieldDomains(
            List.of(
                new DateRangeFieldDomain("@timestamp", 100L, 200L, true, "test"),
                new DateRangeFieldDomain("event.ingested", 300L, 400L, true, "test")
            )
        );

        Map<String, String> customData = request.fieldDomainCustomData();
        assertThat(customData.get("fields.@timestamp.max"), equalTo("200"));
        assertThat(customData.get("fields.event.ingested.max"), equalTo("400"));
    }

    public void testSerialization() throws IOException {
        PutIndexFieldDomainsRequest request = new PutIndexFieldDomainsRequest(new Index("logs-000001", "index-uuid")).fieldDomains(
            List.of(
                new DateRangeFieldDomain("@timestamp", 100L, 200L, true, "test"),
                new DateRangeFieldDomain("event.ingested", 300L, 400L, true, "test")
            )
        );
        request.timeout(randomPositiveTimeValue());
        request.clusterManagerNodeTimeout(randomPositiveTimeValue());
        request.setParentTask(randomAlphaOfLength(5), randomNonNegativeLong());

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            request.writeTo(out);

            PutIndexFieldDomainsRequest deserialized;
            try (StreamInput in = out.bytes().streamInput()) {
                deserialized = new PutIndexFieldDomainsRequest(in);
            }
            assertThat(deserialized.targetIndex(), equalTo(request.targetIndex()));
            assertThat(deserialized.fieldDomainCustomData(), equalTo(request.fieldDomainCustomData()));
            assertThat(deserialized.timeout(), equalTo(request.timeout()));
            assertThat(deserialized.clusterManagerNodeTimeout(), equalTo(request.clusterManagerNodeTimeout()));
            assertThat(deserialized.getParentTask(), equalTo(request.getParentTask()));
        }
    }

    public void testIndicesRequestProperties() {
        PutIndexFieldDomainsRequest request = new PutIndexFieldDomainsRequest(new Index("logs-000001", "index-uuid"));

        assertArrayEquals(new String[] { "logs-000001" }, request.indices());
        assertThat(request.indicesOptions(), equalTo(IndicesOptions.strictSingleIndexNoExpandForbidClosed()));
        assertFalse(request.includeDataStreams());
    }
}
