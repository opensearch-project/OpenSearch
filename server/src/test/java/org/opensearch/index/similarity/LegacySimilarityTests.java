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

package org.opensearch.index.similarity;

import org.apache.lucene.search.similarities.BooleanSimilarity;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarity.LegacyBM25Similarity;
import org.opensearch.LegacyESVersion;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;

public class LegacySimilarityTests extends OpenSearchSingleNodeTestCase {

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    public void testResolveDefaultSimilaritiesOn6xIndex() {
        final Settings indexSettings = Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, LegacyESVersion.V_6_3_0) // otherwise classic is forbidden
                .build();
        final SimilarityService similarityService = createIndex("foo", indexSettings).similarityService();
        assertThat(similarityService.getSimilarity("classic").get(), instanceOf(ClassicSimilarity.class));
        assertWarnings("The [classic] similarity is now deprecated in favour of BM25, which is generally "
                + "accepted as a better alternative. Use the [BM25] similarity or build a custom [scripted] similarity "
                + "instead.");
        assertThat(similarityService.getSimilarity("BM25").get(), instanceOf(LegacyBM25Similarity.class));
        assertThat(similarityService.getSimilarity("boolean").get(), instanceOf(BooleanSimilarity.class));
        assertThat(similarityService.getSimilarity("default"), equalTo(null));
    }

    public void testResolveSimilaritiesFromMappingClassic() throws IOException {
        try (XContentBuilder mapping = XContentFactory.jsonBuilder()) {
            mapping.startObject();
            {
                mapping.startObject("type");
                {
                    mapping.startObject("properties");
                    {
                        mapping.startObject("field1");
                        {
                            mapping.field("type", "text");
                            mapping.field("similarity", "my_similarity");
                        }
                        mapping.endObject();
                    }
                    mapping.endObject();
                }
                mapping.endObject();
            }
            mapping.endObject();

            final Settings indexSettings = Settings.builder()
                    .put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), LegacyESVersion.V_6_3_0) // otherwise classic is forbidden
                    .put("index.similarity.my_similarity.type", "classic")
                    .put("index.similarity.my_similarity.discount_overlaps", false)
                    .build();
            final MapperService mapperService = createIndex("foo", indexSettings, "type", mapping).mapperService();
            assertThat(mapperService.fieldType("field1").getTextSearchInfo().getSimilarity().get(),
                instanceOf(ClassicSimilarity.class));

            final ClassicSimilarity similarity
                = (ClassicSimilarity) mapperService.fieldType("field1").getTextSearchInfo().getSimilarity().get();
            assertThat(similarity.getDiscountOverlaps(), equalTo(false));
        }
    }

}
