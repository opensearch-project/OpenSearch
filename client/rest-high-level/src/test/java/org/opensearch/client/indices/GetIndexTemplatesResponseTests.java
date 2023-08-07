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

package org.opensearch.client.indices;

import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.opensearch.index.RandomCreateIndexGenerator.randomIndexSettings;
import static org.opensearch.index.RandomCreateIndexGenerator.randomMappingFields;
import static org.opensearch.test.AbstractXContentTestCase.xContentTester;
import static org.hamcrest.Matchers.equalTo;

public class GetIndexTemplatesResponseTests extends OpenSearchTestCase {

    static final String mappingString = "{\"properties\":{\"f1\": {\"type\":\"text\"},\"f2\": {\"type\":\"keyword\"}}}";

    public void testFromXContent() throws IOException {
        xContentTester(
            this::createParser,
            GetIndexTemplatesResponseTests::createTestInstance,
            GetIndexTemplatesResponseTests::toXContent,
            GetIndexTemplatesResponse::fromXContent
        ).assertEqualsConsumer(GetIndexTemplatesResponseTests::assertEqualInstances)
            .supportsUnknownFields(true)
            .randomFieldsExcludeFilter(randomFieldsExcludeFilter())
            .shuffleFieldsExceptions(new String[] { "aliases", "mappings", "patterns", "settings" })
            .test();
    }

    public void testParsingFromOpenSearchResponse() throws IOException {
        for (int runs = 0; runs < 20; runs++) {
            org.opensearch.action.admin.indices.template.get.GetIndexTemplatesResponse esResponse =
                new org.opensearch.action.admin.indices.template.get.GetIndexTemplatesResponse(new ArrayList<>());

            XContentType xContentType = randomFrom(XContentType.values());
            int numTemplates = randomIntBetween(0, 32);
            for (int i = 0; i < numTemplates; i++) {
                org.opensearch.cluster.metadata.IndexTemplateMetadata.Builder esIMD =
                    new org.opensearch.cluster.metadata.IndexTemplateMetadata.Builder(
                        String.format(Locale.ROOT, "%02d ", i) + randomAlphaOfLength(4)
                    );
                esIMD.patterns(Arrays.asList(generateRandomStringArray(32, 4, false, false)));
                esIMD.settings(randomIndexSettings());
                esIMD.putMapping("_doc", new CompressedXContent(BytesReference.bytes(randomMapping("_doc", xContentType))));
                int numAliases = randomIntBetween(0, 8);
                for (int j = 0; j < numAliases; j++) {
                    esIMD.putAlias(randomAliasMetadata(String.format(Locale.ROOT, "%02d ", j) + randomAlphaOfLength(4)));
                }
                esIMD.order(randomIntBetween(0, Integer.MAX_VALUE));
                esIMD.version(randomIntBetween(0, Integer.MAX_VALUE));
                esResponse.getIndexTemplates().add(esIMD.build());
            }

            XContentBuilder xContentBuilder = XContentBuilder.builder(xContentType.xContent());
            esResponse.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);

            try (
                XContentParser parser = XContentHelper.createParser(
                    NamedXContentRegistry.EMPTY,
                    DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                    BytesReference.bytes(xContentBuilder),
                    xContentType
                )
            ) {
                GetIndexTemplatesResponse response = GetIndexTemplatesResponse.fromXContent(parser);
                assertThat(response.getIndexTemplates().size(), equalTo(numTemplates));

                response.getIndexTemplates().sort(Comparator.comparing(IndexTemplateMetadata::name));
                for (int i = 0; i < numTemplates; i++) {
                    org.opensearch.cluster.metadata.IndexTemplateMetadata esIMD = esResponse.getIndexTemplates().get(i);
                    IndexTemplateMetadata result = response.getIndexTemplates().get(i);

                    assertThat(result.patterns(), equalTo(esIMD.patterns()));
                    assertThat(result.settings(), equalTo(esIMD.settings()));
                    assertThat(result.order(), equalTo(esIMD.order()));
                    assertThat(result.version(), equalTo(esIMD.version()));

                    BytesReference mappingSource = esIMD.mappings().uncompressed();
                    Map<String, Object> expectedMapping = XContentHelper.convertToMap(mappingSource, true, xContentBuilder.contentType())
                        .v2();
                    assertThat(result.mappings().sourceAsMap(), equalTo(expectedMapping.get("_doc")));

                    assertThat(result.aliases().size(), equalTo(esIMD.aliases().size()));
                    List<AliasMetadata> expectedAliases = Arrays.stream(esIMD.aliases().values().toArray(new AliasMetadata[0]))
                        .sorted(Comparator.comparing(AliasMetadata::alias))
                        .collect(Collectors.toList());
                    List<AliasMetadata> actualAliases = Arrays.stream(result.aliases().values().toArray(new AliasMetadata[0]))
                        .sorted(Comparator.comparing(AliasMetadata::alias))
                        .collect(Collectors.toList());
                    for (int j = 0; j < result.aliases().size(); j++) {
                        assertThat(actualAliases.get(j), equalTo(expectedAliases.get(j)));
                    }
                }
            }
        }
    }

    private Predicate<String> randomFieldsExcludeFilter() {
        return (field) -> field.isEmpty()
            || field.endsWith("aliases")
            || field.endsWith("settings")
            || field.endsWith("settings.index")
            || field.endsWith("mappings") // uses parser.map()
            || field.contains("mappings.properties") // cannot have extra properties
        ;
    }

    private static void assertEqualInstances(GetIndexTemplatesResponse expectedInstance, GetIndexTemplatesResponse newInstance) {
        assertEquals(expectedInstance, newInstance);
        // Check there's no doc types at the root of the mapping
        Map<String, Object> expectedMap = XContentHelper.convertToMap(new BytesArray(mappingString), true, XContentType.JSON).v2();
        for (IndexTemplateMetadata template : newInstance.getIndexTemplates()) {
            MappingMetadata mappingMD = template.mappings();
            if (mappingMD != null) {
                Map<String, Object> mappingAsMap = mappingMD.sourceAsMap();
                assertEquals(expectedMap, mappingAsMap);
            }
        }
    }

    static GetIndexTemplatesResponse createTestInstance() {
        List<IndexTemplateMetadata> templates = new ArrayList<>();
        int numTemplates = between(0, 10);
        for (int t = 0; t < numTemplates; t++) {
            IndexTemplateMetadata.Builder templateBuilder = IndexTemplateMetadata.builder("template-" + t);
            templateBuilder.patterns(IntStream.range(0, between(1, 5)).mapToObj(i -> "pattern-" + i).collect(Collectors.toList()));
            int numAlias = between(0, 5);
            for (int i = 0; i < numAlias; i++) {
                templateBuilder.putAlias(AliasMetadata.builder(randomAlphaOfLengthBetween(1, 10)));
            }
            if (randomBoolean()) {
                templateBuilder.settings(Settings.builder().put("index.setting-1", randomLong()));
            }
            if (randomBoolean()) {
                templateBuilder.order(randomInt());
            }
            if (randomBoolean()) {
                templateBuilder.version(between(0, 100));
            }
            if (randomBoolean()) {
                Map<String, Object> map = XContentHelper.convertToMap(new BytesArray(mappingString), true, XContentType.JSON).v2();
                MappingMetadata mapping = new MappingMetadata(MapperService.SINGLE_MAPPING_NAME, map);
                templateBuilder.mapping(mapping);
            }
            templates.add(templateBuilder.build());
        }
        return new GetIndexTemplatesResponse(templates);
    }

    // As the client class GetIndexTemplatesResponse doesn't have toXContent method, adding this method here only for the test
    static void toXContent(GetIndexTemplatesResponse response, XContentBuilder builder) throws IOException {

        // Create a server-side counterpart for the client-side class and call toXContent on it

        List<org.opensearch.cluster.metadata.IndexTemplateMetadata> serverIndexTemplates = new ArrayList<>();
        List<IndexTemplateMetadata> clientIndexTemplates = response.getIndexTemplates();
        for (IndexTemplateMetadata clientITMD : clientIndexTemplates) {
            org.opensearch.cluster.metadata.IndexTemplateMetadata.Builder serverTemplateBuilder =
                org.opensearch.cluster.metadata.IndexTemplateMetadata.builder(clientITMD.name());

            serverTemplateBuilder.patterns(clientITMD.patterns());

            Iterator<AliasMetadata> aliases = clientITMD.aliases().values().iterator();
            aliases.forEachRemaining((a) -> serverTemplateBuilder.putAlias(a));

            serverTemplateBuilder.settings(clientITMD.settings());
            serverTemplateBuilder.order(clientITMD.order());
            serverTemplateBuilder.version(clientITMD.version());
            if (clientITMD.mappings() != null) {
                // The client-side mappings never include a wrapping type, but server-side mappings
                // for index templates still do so we need to wrap things here
                String mappings = "{\"" + MapperService.SINGLE_MAPPING_NAME + "\": " + clientITMD.mappings().source().string() + "}";
                serverTemplateBuilder.putMapping(MapperService.SINGLE_MAPPING_NAME, mappings);
            }
            serverIndexTemplates.add(serverTemplateBuilder.build());

        }
        org.opensearch.action.admin.indices.template.get.GetIndexTemplatesResponse serverResponse =
            new org.opensearch.action.admin.indices.template.get.GetIndexTemplatesResponse(serverIndexTemplates);
        serverResponse.toXContent(builder, ToXContent.EMPTY_PARAMS);
    }

    private static AliasMetadata randomAliasMetadata(String name) {
        AliasMetadata.Builder alias = AliasMetadata.builder(name);
        if (randomBoolean()) {
            if (randomBoolean()) {
                alias.routing(randomAlphaOfLength(5));
            } else {
                if (randomBoolean()) {
                    alias.indexRouting(randomAlphaOfLength(5));
                }
                if (randomBoolean()) {
                    alias.searchRouting(randomAlphaOfLength(5));
                }
            }
        }

        if (randomBoolean()) {
            alias.filter("{\"term\":{\"year\":2016}}");
        }

        if (randomBoolean()) {
            alias.writeIndex(randomBoolean());
        }
        return alias.build();
    }

    static XContentBuilder randomMapping(String type, XContentType xContentType) throws IOException {
        XContentBuilder builder = MediaTypeRegistry.contentBuilder(xContentType);
        builder.startObject().startObject(type);

        randomMappingFields(builder, true);

        builder.endObject().endObject();
        return builder;
    }
}
