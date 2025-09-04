/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.admin.indices.refresh.RefreshResponse;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;

import static org.opensearch.cluster.metadata.IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING;
import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;

public class ScaledFloatDerivedSourceIT extends OpenSearchIntegTestCase {

    private static final String INDEX_NAME = "test";

    public void testScaledFloatDerivedSource() throws Exception {
        Settings.Builder settings = Settings.builder();
        settings.put(indexSettings());
        settings.put("index.derived_source.enabled", "true");

        prepareCreate(INDEX_NAME).setSettings(settings)
            .setMapping(
                jsonBuilder().startObject()
                    .startObject("properties")
                    .startObject("foo")
                    .field("type", "scaled_float")
                    .field("scaling_factor", "100")
                    .endObject()
                    .endObject()
                    .endObject()
            )
            .get();

        ensureGreen(INDEX_NAME);

        String docId = "one_doc";
        assertEquals(DocWriteResponse.Result.CREATED, prepareIndex(docId, 1.2123422f).get().getResult());

        RefreshResponse refreshResponse = refresh(INDEX_NAME);
        assertEquals(RestStatus.OK, refreshResponse.getStatus());
        assertEquals(0, refreshResponse.getFailedShards());
        assertEquals(INDEX_NUMBER_OF_SHARDS_SETTING.get(settings.build()).intValue(), refreshResponse.getSuccessfulShards());

        GetResponse getResponse = client().prepareGet()
            .setFetchSource(true)
            .setId(docId)
            .setIndex(INDEX_NAME)
            .get(TimeValue.timeValueMinutes(1));
        assertTrue(getResponse.isExists());
        assertEquals(1.21d, getResponse.getSourceAsMap().get("foo"));
    }

    private IndexRequestBuilder prepareIndex(String id, float number) throws IOException {
        return client().prepareIndex(INDEX_NAME)
            .setId(id)
            .setSource(jsonBuilder().startObject().field("foo", number).endObject().toString(), XContentType.JSON);
    }
}
