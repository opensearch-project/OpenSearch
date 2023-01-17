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

package org.opensearch.action.admin.indices.shrink;

import org.opensearch.action.admin.indices.alias.Alias;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexRequestTests;
import org.opensearch.common.Strings;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.RandomCreateIndexGenerator;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.hamcrest.OpenSearchAssertions;

import java.io.IOException;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasToString;

public class ResizeRequestTests extends OpenSearchTestCase {

    public void testCopySettingsValidation() {
        runTestCopySettingsValidation(false, r -> {
            final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, r::get);
            assertThat(e, hasToString(containsString("[copySettings] can not be explicitly set to [false]")));
        });

        runTestCopySettingsValidation(null, r -> assertNull(r.get().getCopySettings()));
        runTestCopySettingsValidation(true, r -> assertTrue(r.get().getCopySettings()));
    }

    private void runTestCopySettingsValidation(final Boolean copySettings, final Consumer<Supplier<ResizeRequest>> consumer) {
        consumer.accept(() -> {
            final ResizeRequest request = new ResizeRequest();
            request.setCopySettings(copySettings);
            return request;
        });
    }

    public void testToXContent() throws IOException {
        {
            ResizeRequest request = new ResizeRequest("target", "source");
            String actualRequestBody = Strings.toString(XContentType.JSON, request);
            assertEquals("{\"settings\":{},\"aliases\":{}}", actualRequestBody);
        }
        {
            ResizeRequest request = new ResizeRequest();
            CreateIndexRequest target = new CreateIndexRequest("target");
            Alias alias = new Alias("test_alias");
            alias.routing("1");
            alias.filter("{\"term\":{\"year\":2016}}");
            alias.writeIndex(true);
            target.alias(alias);
            Settings.Builder settings = Settings.builder();
            settings.put(SETTING_NUMBER_OF_SHARDS, 10);
            target.settings(settings);
            request.setTargetIndex(target);
            String actualRequestBody = Strings.toString(XContentType.JSON, request);
            String expectedRequestBody = "{\"settings\":{\"index\":{\"number_of_shards\":\"10\"}},"
                + "\"aliases\":{\"test_alias\":{\"filter\":{\"term\":{\"year\":2016}},\"routing\":\"1\",\"is_write_index\":true}}}";
            assertEquals(expectedRequestBody, actualRequestBody);
        }
    }

    public void testToAndFromXContent() throws IOException {
        final ResizeRequest resizeRequest = createTestItem();

        boolean humanReadable = randomBoolean();
        final XContentType xContentType = randomFrom(XContentType.values());
        BytesReference originalBytes = toShuffledXContent(resizeRequest, xContentType, EMPTY_PARAMS, humanReadable);

        ResizeRequest parsedResizeRequest = new ResizeRequest(
            resizeRequest.getTargetIndexRequest().index(),
            resizeRequest.getSourceIndex()
        );
        try (XContentParser xParser = createParser(xContentType.xContent(), originalBytes)) {
            parsedResizeRequest.fromXContent(xParser);
        }

        assertEquals(resizeRequest.getSourceIndex(), parsedResizeRequest.getSourceIndex());
        assertEquals(resizeRequest.getTargetIndexRequest().index(), parsedResizeRequest.getTargetIndexRequest().index());
        CreateIndexRequestTests.assertAliasesEqual(
            resizeRequest.getTargetIndexRequest().aliases(),
            parsedResizeRequest.getTargetIndexRequest().aliases()
        );
        assertEquals(resizeRequest.getTargetIndexRequest().settings(), parsedResizeRequest.getTargetIndexRequest().settings());

        BytesReference finalBytes = toShuffledXContent(parsedResizeRequest, xContentType, EMPTY_PARAMS, humanReadable);
        OpenSearchAssertions.assertToXContentEquivalent(originalBytes, finalBytes, xContentType);
    }

    private static ResizeRequest createTestItem() {
        ResizeRequest resizeRequest = new ResizeRequest(randomAlphaOfLengthBetween(3, 10), randomAlphaOfLengthBetween(3, 10));
        if (randomBoolean()) {
            CreateIndexRequest createIndexRequest = new CreateIndexRequest(randomAlphaOfLengthBetween(3, 10));
            if (randomBoolean()) {
                RandomCreateIndexGenerator.randomAliases(createIndexRequest);
            }
            if (randomBoolean()) {
                createIndexRequest.settings(RandomCreateIndexGenerator.randomIndexSettings());
            }
            resizeRequest.setTargetIndex(createIndexRequest);
        }
        return resizeRequest;
    }
}
