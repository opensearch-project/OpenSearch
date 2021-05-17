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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.index.mapper;

import org.opensearch.LegacyESVersion;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.Strings;
import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.index.IndexService;
import org.opensearch.index.mapper.MapperService.MergeReason;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.opensearch.test.VersionUtils;

import static org.hamcrest.CoreMatchers.containsString;

public class AllFieldMapperTests extends OpenSearchSingleNodeTestCase {

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    public void testAllDisabled() throws Exception {
        {
            final Version version = VersionUtils.randomVersionBetween(random(),
                LegacyESVersion.V_6_0_0, LegacyESVersion.V_7_0_0.minimumCompatibilityVersion());
            IndexService indexService = createIndex("test_6x",
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, version)
                    .build()
            );
            String mappingDisabled = Strings.toString(XContentFactory.jsonBuilder().startObject()
                .startObject("_all")
                    .field("enabled", false)
                .endObject().endObject()
            );
            indexService.mapperService().merge("_doc", new CompressedXContent(mappingDisabled), MergeReason.MAPPING_UPDATE);
            assertEquals("{\"_doc\":{\"_all\":{\"enabled\":false}}}",
                Strings.toString(indexService.mapperService().documentMapper()));

            String mappingEnabled = Strings.toString(XContentFactory.jsonBuilder().startObject()
                .startObject("_all")
                    .field("enabled", true)
                .endObject().endObject()
            );
            MapperParsingException exc = expectThrows(MapperParsingException.class,
                () -> indexService.mapperService().merge("_doc", new CompressedXContent(mappingEnabled), MergeReason.MAPPING_UPDATE));
            assertThat(exc.getMessage(), containsString("[_all] is disabled in this version."));
        }
        {
            IndexService indexService = createIndex("test");
            String mappingEnabled = Strings.toString(XContentFactory.jsonBuilder().startObject()
                .startObject("_all")
                    .field("enabled", true)
                .endObject().endObject()
            );
            MapperParsingException exc = expectThrows(MapperParsingException.class,
                () -> indexService.mapperService().merge("_doc", new CompressedXContent(mappingEnabled), MergeReason.MAPPING_UPDATE));
            assertThat(exc.getMessage(), containsString("unsupported parameters:  [_all"));

            String mappingDisabled = Strings.toString(XContentFactory.jsonBuilder().startObject()
                .startObject("_all")
                    .field("enabled", false)
                .endObject().endObject()
            );
            exc = expectThrows(MapperParsingException.class,
                () -> indexService.mapperService().merge("_doc", new CompressedXContent(mappingDisabled), MergeReason.MAPPING_UPDATE));
            assertThat(exc.getMessage(), containsString("unsupported parameters:  [_all"));

            String mappingAll = Strings.toString(XContentFactory.jsonBuilder().startObject()
                .startObject("_all").endObject().endObject()
            );
            exc = expectThrows(MapperParsingException.class,
                () -> indexService.mapperService().merge("_doc", new CompressedXContent(mappingAll), MergeReason.MAPPING_UPDATE));
            assertThat(exc.getMessage(), containsString("unsupported parameters:  [_all"));

            String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().endObject());
            indexService.mapperService().merge("_doc", new CompressedXContent(mapping), MergeReason.MAPPING_UPDATE);
            assertEquals("{\"_doc\":{}}", indexService.mapperService().documentMapper("_doc").mapping().toString());
        }
    }

    public void testUpdateDefaultSearchAnalyzer() throws Exception {
        IndexService indexService = createIndex("test", Settings.builder()
                .put("index.analysis.analyzer.default_search.type", "custom")
                .put("index.analysis.analyzer.default_search.tokenizer", "standard").build());
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("_doc").endObject().endObject());
        indexService.mapperService().merge("_doc", new CompressedXContent(mapping), MergeReason.MAPPING_UPDATE);
        assertEquals(mapping, indexService.mapperService().documentMapper("_doc").mapping().toString());
    }

}
