/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.exporter;

import org.opensearch.action.bulk.BulkAction;
import org.opensearch.action.bulk.BulkRequestBuilder;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.client.Client;
import org.opensearch.plugin.insights.QueryInsightsTestUtils;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.test.OpenSearchTestCase;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Before;

import java.util.List;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Granular tests for the {@link LocalIndexExporterTests} class.
 */
public class LocalIndexExporterTests extends OpenSearchTestCase {
    private final DateTimeFormatter format = DateTimeFormat.forPattern("YYYY.MM.dd");
    private final Client client = mock(Client.class);
    private LocalIndexExporter localIndexExporter;

    @Before
    public void setup() {
        localIndexExporter = new LocalIndexExporter(client, format);
    }

    public void testExportEmptyRecords() {
        List<SearchQueryRecord> records = List.of();
        assertTrue(localIndexExporter.export(records));
    }

    @SuppressWarnings("unchecked")
    public void testExportRecords() {
        BulkRequestBuilder bulkRequestBuilder = spy(new BulkRequestBuilder(client, BulkAction.INSTANCE));
        final PlainActionFuture<BulkResponse> future = mock(PlainActionFuture.class);
        when(future.actionGet()).thenReturn(null);
        doAnswer(invocation -> future).when(bulkRequestBuilder).execute();
        when(client.prepareBulk()).thenReturn(bulkRequestBuilder);

        List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(2);
        assertTrue(localIndexExporter.export(records));
        assertEquals(2, bulkRequestBuilder.numberOfActions());
    }

    @SuppressWarnings("unchecked")
    public void testExportRecordsWithError() {
        BulkRequestBuilder bulkRequestBuilder = spy(new BulkRequestBuilder(client, BulkAction.INSTANCE));
        final PlainActionFuture<BulkResponse> future = mock(PlainActionFuture.class);
        when(future.actionGet()).thenReturn(null);
        doThrow(new RuntimeException()).when(bulkRequestBuilder).execute();
        when(client.prepareBulk()).thenReturn(bulkRequestBuilder);

        List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(2);
        assertFalse(localIndexExporter.export(records));
    }
}
