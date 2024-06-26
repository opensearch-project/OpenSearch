/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.exporter;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.bulk.BulkRequestBuilder;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.Client;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;

import java.util.List;

/**
 * Local index exporter for exporting query insights data to local OpenSearch indices.
 */
public final class LocalIndexExporter implements QueryInsightsExporter {
    /**
     * Logger of the local index exporter
     */
    private final Logger logger = LogManager.getLogger();
    private final Client client;
    private DateTimeFormatter indexPattern;

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
     * Getter of indexPattern
     *
     * @return indexPattern
     */
    public DateTimeFormatter getIndexPattern() {
        return indexPattern;
    }

    /**
     * Setter of indexPattern
     *
     * @param indexPattern index pattern
     * @return the current LocalIndexExporter
     */
    public LocalIndexExporter setIndexPattern(DateTimeFormatter indexPattern) {
        this.indexPattern = indexPattern;
        return this;
    }

    /**
     * Export a list of SearchQueryRecord to a local index
     *
     * @param records list of {@link SearchQueryRecord}
     */
    @Override
    public void export(final List<SearchQueryRecord> records) {
        if (records == null || records.size() == 0) {
            return;
        }
        try {
            final String index = getDateTimeFromFormat();
            final BulkRequestBuilder bulkRequestBuilder = client.prepareBulk().setTimeout(TimeValue.timeValueMinutes(1));
            for (SearchQueryRecord record : records) {
                bulkRequestBuilder.add(
                    new IndexRequest(index).source(record.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS))
                );
            }
            bulkRequestBuilder.execute(new ActionListener<BulkResponse>() {
                @Override
                public void onResponse(BulkResponse bulkItemResponses) {}

                @Override
                public void onFailure(Exception e) {
                    logger.error("Failed to execute bulk operation for query insights data: ", e);
                }
            });
        } catch (final Exception e) {
            logger.error("Unable to index query insights data: ", e);
        }
    }

    /**
     * Close the exporter sink
     */
    @Override
    public void close() {
        logger.debug("Closing the LocalIndexExporter..");
    }

    private String getDateTimeFromFormat() {
        return indexPattern.print(DateTime.now(DateTimeZone.UTC));
    }
}
