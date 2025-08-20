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

    public void testScaledFloatDerivedSource() throws Exception {
        Settings.Builder settings = Settings.builder();
        settings.put(indexSettings());
        settings.put("index.derived_source.enabled", "true");

        prepareCreate("test").setSettings(settings)
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

        ensureGreen("test");

        assertEquals(DocWriteResponse.Result.CREATED, prepareIndex("one_doc", 1.2123422f).get().getResult());

        RefreshResponse refreshResponse = refresh("test");
        assertEquals(RestStatus.OK, refreshResponse.getStatus());
        assertEquals(0, refreshResponse.getFailedShards());
        assertEquals(INDEX_NUMBER_OF_SHARDS_SETTING.get(settings.build()).intValue(), refreshResponse.getSuccessfulShards());

        GetResponse getResponse = client().prepareGet().setFetchSource(true).setId("one_doc").setIndex("test").get(TimeValue.timeValueMinutes(1));
        assertTrue(getResponse.isExists());
        assertEquals(1.21d, getResponse.getSourceAsMap().get("foo"));
    }

    private IndexRequestBuilder prepareIndex(String id, float number) throws IOException {
        return client().prepareIndex("test").setId(id).setSource(jsonBuilder().startObject().field("foo", number).endObject().toString(), XContentType.JSON);
    }
}
