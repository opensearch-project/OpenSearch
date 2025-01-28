/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.index.mapper.size;

import org.opensearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.plugin.mapper.MapperSizePlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Locale;
import java.util.Map;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class SizeMappingIT extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MapperSizePlugin.class);
    }

    // issue 5053
    public void testThatUpdatingMappingShouldNotRemoveSizeMappingConfiguration() throws Exception {
        String index = "foo";

        XContentBuilder builder = jsonBuilder().startObject().startObject("_size").field("enabled", true).endObject().endObject();
        assertAcked(client().admin().indices().prepareCreate(index).setMapping(builder));

        // check mapping again
        assertSizeMappingEnabled(index, true);

        // update some field in the mapping
        XContentBuilder updateMappingBuilder = jsonBuilder().startObject()
            .startObject("properties")
            .startObject("otherField")
            .field("type", "text")
            .endObject()
            .endObject()
            .endObject();
        AcknowledgedResponse putMappingResponse = client().admin().indices().preparePutMapping(index).setSource(updateMappingBuilder).get();
        assertAcked(putMappingResponse);

        // make sure size field is still in mapping
        assertSizeMappingEnabled(index, true);
    }

    public void testThatSizeCanBeSwitchedOnAndOff() throws Exception {
        String index = "foo";

        XContentBuilder builder = jsonBuilder().startObject().startObject("_size").field("enabled", true).endObject().endObject();
        assertAcked(client().admin().indices().prepareCreate(index).setMapping(builder));

        // check mapping again
        assertSizeMappingEnabled(index, true);

        // update some field in the mapping
        XContentBuilder updateMappingBuilder = jsonBuilder().startObject()
            .startObject("_size")
            .field("enabled", false)
            .endObject()
            .endObject();
        AcknowledgedResponse putMappingResponse = client().admin().indices().preparePutMapping(index).setSource(updateMappingBuilder).get();
        assertAcked(putMappingResponse);

        // make sure size field is still in mapping
        assertSizeMappingEnabled(index, false);
    }

    private void assertSizeMappingEnabled(String index, boolean enabled) throws IOException {
        String errMsg = String.format(
            Locale.ROOT,
            "Expected size field mapping to be " + (enabled ? "enabled" : "disabled") + " for %s",
            index
        );
        GetMappingsResponse getMappingsResponse = client().admin().indices().prepareGetMappings(index).get();
        Map<String, Object> mappingSource = getMappingsResponse.getMappings().get(index).getSourceAsMap();
        assertThat(errMsg, mappingSource, hasKey("_size"));
        String sizeAsString = mappingSource.get("_size").toString();
        assertThat(sizeAsString, is(notNullValue()));
        assertThat(errMsg, sizeAsString, is("{enabled=" + (enabled) + "}"));
    }

    public void testBasic() throws Exception {
        assertAcked(prepareCreate("test").setMapping("_size", "enabled=true"));
        final String source = "{\"f\":10}";
        indexRandom(true, client().prepareIndex("test").setId("1").setSource(source, MediaTypeRegistry.JSON));
        GetResponse getResponse = client().prepareGet("test", "1").setStoredFields("_size").get();
        assertNotNull(getResponse.getField("_size"));
        assertEquals(source.length(), (int) getResponse.getField("_size").getValue());
    }
}
