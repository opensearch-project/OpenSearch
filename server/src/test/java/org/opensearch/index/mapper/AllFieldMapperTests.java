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

import org.opensearch.common.Strings;
import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.index.IndexService;
import org.opensearch.index.mapper.MapperService.MergeReason;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

public class AllFieldMapperTests extends OpenSearchSingleNodeTestCase {

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    public void testUpdateDefaultSearchAnalyzer() throws Exception {
        IndexService indexService = createIndex(
            "test",
            Settings.builder()
                .put("index.analysis.analyzer.default_search.type", "custom")
                .put("index.analysis.analyzer.default_search.tokenizer", "standard")
                .build()
        );
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("_doc").endObject().endObject());
        indexService.mapperService().merge("_doc", new CompressedXContent(mapping), MergeReason.MAPPING_UPDATE);
        assertEquals(mapping, indexService.mapperService().documentMapper().mapping().toString());
    }

}
