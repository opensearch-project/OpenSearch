/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.fielddomain;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.core.index.Index;
import org.opensearch.index.fielddomain.DateRangeFieldDomain;
import org.opensearch.index.fielddomain.IndexFieldDomainMetadata;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.List;
import java.util.Map;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class PutIndexFieldDomainsIT extends OpenSearchIntegTestCase {
    public void testPutIndexFieldDomainsActionPublishesIndexMetadata() throws Exception {
        String indexName = "logs-000001";
        assertAcked(prepareCreate(indexName).setMapping("@timestamp", "type=date", "event.ingested", "type=date"));
        ensureGreen(indexName);

        Index targetIndex = indexMetadata(indexName).getIndex();
        PutIndexFieldDomainsRequest request = new PutIndexFieldDomainsRequest(targetIndex).fieldDomains(
            List.of(
                new DateRangeFieldDomain("@timestamp", 100L, 200L, true, "test_producer"),
                new DateRangeFieldDomain("event.ingested", 300L, 400L, true, "test_producer")
            )
        );

        assertAcked(client().execute(PutIndexFieldDomainsAction.INSTANCE, request).actionGet());

        Map<String, String> customData = indexMetadata(indexName).getCustomData(IndexFieldDomainMetadata.CUSTOM_KEY);
        assertThat(customData, notNullValue());
        assertThat(customData.get("fields.@timestamp.type"), equalTo("date_range"));
        assertThat(customData.get("fields.@timestamp.min"), equalTo("100"));
        assertThat(customData.get("fields.@timestamp.max"), equalTo("200"));
        assertThat(customData.get("fields.@timestamp.finalized"), equalTo("true"));
        assertThat(customData.get("fields.@timestamp.source"), equalTo("test_producer"));
        assertThat(customData.get("fields.event.ingested.type"), equalTo("date_range"));
        assertThat(customData.get("fields.event.ingested.min"), equalTo("300"));
        assertThat(customData.get("fields.event.ingested.max"), equalTo("400"));
        assertThat(customData.get("fields.event.ingested.finalized"), equalTo("true"));
        assertThat(customData.get("fields.event.ingested.source"), equalTo("test_producer"));
    }

    private IndexMetadata indexMetadata(String indexName) {
        return client().admin().cluster().prepareState().clear().setMetadata(true).get().getState().metadata().index(indexName);
    }
}
