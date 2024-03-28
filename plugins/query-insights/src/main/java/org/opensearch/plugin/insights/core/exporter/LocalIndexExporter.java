/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.exporter;

import org.opensearch.action.bulk.BulkRequestBuilder;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.client.Client;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;

import java.util.List;

/**
 * Local index exporter for exporting query insights data to local OpenSearch indices.
 */
public final class LocalIndexExporter extends AbstractExporter {
    final private Client client;
    final private DateTimeFormatter indexPattern;

    /**
     * Constructor of LocalIndexExporter
     *
     * @param client OS client
     * @param indexPattern the pattern of index to export to
     */
    public LocalIndexExporter(final Client client, final DateTimeFormatter indexPattern) {
        this.indexPattern = indexPattern;
        this.client = client;
    }

    /**
     * Export a list of SearchQueryRecord to a local index
     *
     * @param records list of {@link SearchQueryRecord}
     * @return True if export succeed, false otherwise
     */
    @Override
    public boolean export(final List<SearchQueryRecord> records) {
        if (records == null || records.size() == 0) {
            return true;
        }
        try {
            final String index = getDateTimeFromFormat();
            final BulkRequestBuilder bulkRequestBuilder = client.prepareBulk()
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .setTimeout(TimeValue.timeValueMinutes(1));
            for (SearchQueryRecord record : records) {
                bulkRequestBuilder.add(
                    new IndexRequest(index).source(record.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS))
                );
            }
            bulkRequestBuilder.execute().actionGet();
            return true;
        } catch (final Exception e) {
            logger.error("Unable to index query insights data: ", e);
            return false;
        }
    }

    private String getDateTimeFromFormat() {
        return indexPattern.print(DateTime.now(DateTimeZone.UTC));
    }
}
