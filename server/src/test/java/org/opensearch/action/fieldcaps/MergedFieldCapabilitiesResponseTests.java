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

package org.opensearch.action.fieldcaps;

import org.opensearch.Version;
import org.opensearch.action.NoShardAvailableActionException;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.AbstractSerializingTestCase;
import org.opensearch.test.VersionUtils;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

public class MergedFieldCapabilitiesResponseTests extends AbstractSerializingTestCase<FieldCapabilitiesResponse> {

    @Override
    protected FieldCapabilitiesResponse doParseInstance(XContentParser parser) throws IOException {
        return FieldCapabilitiesResponse.fromXContent(parser);
    }

    @Override
    protected FieldCapabilitiesResponse createTestInstance() {
        // merged responses
        Map<String, Map<String, FieldCapabilities>> responses = new HashMap<>();

        String[] fields = generateRandomStringArray(5, 10, false, true);
        assertNotNull(fields);

        for (String field : fields) {
            Map<String, FieldCapabilities> typesToCapabilities = new HashMap<>();
            String[] types = generateRandomStringArray(5, 10, false, false);
            assertNotNull(types);

            for (String type : types) {
                typesToCapabilities.put(type, FieldCapabilitiesTests.randomFieldCaps(field));
            }
            responses.put(field, typesToCapabilities);
        }
        int numIndices = randomIntBetween(1, 10);
        String[] indices = new String[numIndices];
        for (int i = 0; i < numIndices; i++) {
            indices[i] = randomAlphaOfLengthBetween(5, 10);
        }
        return new FieldCapabilitiesResponse(indices, responses, randomFailures());
    }

    private static Map<String, Exception> randomFailures() {
        Map<String, Exception> failures = new HashMap<>();
        for (int i = randomIntBetween(0, 3); i > 0; i--) {
            failures.put(randomAlphaOfLengthBetween(5, 10), new NoShardAvailableActionException(null, randomAlphaOfLength(10)));
        }
        return failures;
    }

    @Override
    protected Writeable.Reader<FieldCapabilitiesResponse> instanceReader() {
        return FieldCapabilitiesResponse::new;
    }

    @Override
    protected FieldCapabilitiesResponse mutateInstance(FieldCapabilitiesResponse response) {
        Map<String, Map<String, FieldCapabilities>> mutatedResponses = new HashMap<>(response.get());

        int mutation = response.get().isEmpty() ? randomFrom(0, 3) : randomIntBetween(0, 3);

        switch (mutation) {
            case 0:
                String toAdd = randomAlphaOfLength(10);
                mutatedResponses.put(
                    toAdd,
                    Collections.singletonMap(randomAlphaOfLength(10), FieldCapabilitiesTests.randomFieldCaps(toAdd))
                );
                break;
            case 1:
                String toRemove = randomFrom(mutatedResponses.keySet());
                mutatedResponses.remove(toRemove);
                break;
            case 2:
                String toReplace = randomFrom(mutatedResponses.keySet());
                mutatedResponses.put(
                    toReplace,
                    Collections.singletonMap(randomAlphaOfLength(10), FieldCapabilitiesTests.randomFieldCaps(toReplace))
                );
                break;
            case 3:
                Map<String, Exception> failures = new HashMap<>(response.getFailures());
                failures.put(randomAlphaOfLength(11), new NoShardAvailableActionException(null, "unavailable"));
                return new FieldCapabilitiesResponse(response.getIndices(), response.get(), failures);
        }
        return new FieldCapabilitiesResponse(null, mutatedResponses, response.getFailures());
    }

    @Override
    protected boolean assertToXContentEquivalence() {
        // A parsed failure renders its reason differently; equals() still compares the rest.
        return false;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        // Disallow random fields from being inserted under the 'fields' key, as this
        // map only contains field names, and also under 'fields.FIELD_NAME', as these
        // maps only contain type names.
        return field -> field.matches("fields(\\.\\w+)?") || field.startsWith("failures");
    }

    public void testToXContent() throws IOException {
        FieldCapabilitiesResponse response = createSimpleResponse();

        XContentBuilder builder = MediaTypeRegistry.contentBuilder(MediaTypeRegistry.JSON);
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);

        String generatedResponse = BytesReference.bytes(builder).utf8ToString();
        assertEquals(
            ("{"
                + "    \"indices\": null,"
                + "    \"fields\": {"
                + "        \"rating\": { "
                + "            \"keyword\": {"
                + "                \"type\": \"keyword\","
                + "                \"searchable\": false,"
                + "                \"aggregatable\": true,"
                + "                \"indices\": [\"index3\", \"index4\"],"
                + "                \"non_searchable_indices\": [\"index4\"] "
                + "            },"
                + "            \"long\": {"
                + "                \"type\": \"long\","
                + "                \"searchable\": true,"
                + "                \"aggregatable\": false,"
                + "                \"indices\": [\"index1\", \"index2\"],"
                + "                \"non_aggregatable_indices\": [\"index1\"] "
                + "            }"
                + "        },"
                + "        \"title\": { "
                + "            \"text\": {"
                + "                \"type\": \"text\","
                + "                \"searchable\": true,"
                + "                \"aggregatable\": false"
                + "            }"
                + "        }"
                + "    }"
                + "}").replaceAll("\\s+", ""),
            generatedResponse
        );
    }

    public void testToXContentWithFailures() throws IOException {
        FieldCapabilitiesResponse response = new FieldCapabilitiesResponse(
            new String[] { "index1" },
            Collections.emptyMap(),
            Collections.singletonMap("index2", new NoShardAvailableActionException(null, "unavailable"))
        );

        XContentBuilder builder = MediaTypeRegistry.contentBuilder(MediaTypeRegistry.JSON);
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);

        assertEquals(
            ("{"
                + "    \"indices\": [\"index1\"],"
                + "    \"fields\": {},"
                + "    \"failures\": [{"
                + "        \"index\": \"index2\","
                + "        \"reason\": {"
                + "            \"type\": \"no_shard_available_action_exception\","
                + "            \"reason\": \"unavailable\""
                + "        }"
                + "    }]"
                + "}").replaceAll("\\s+", ""),
            BytesReference.bytes(builder).utf8ToString()
        );
    }

    public void testFailuresSurviveXContentRoundTrip() throws IOException {
        FieldCapabilitiesResponse response = new FieldCapabilitiesResponse(
            new String[] { "index1" },
            Collections.emptyMap(),
            Collections.singletonMap("index2", new NoShardAvailableActionException(null, "no shard available"))
        );

        FieldCapabilitiesResponse parsed = copyViaXContent(response);

        assertEquals(Set.of("index2"), parsed.getFailures().keySet());
        assertTrue(parsed.getFailures().get("index2").getMessage().contains("no shard available"));
    }

    public void testFailuresOnTheWire() throws IOException {
        FieldCapabilitiesResponse response = new FieldCapabilitiesResponse(
            new String[] { "index1" },
            Collections.emptyMap(),
            Collections.singletonMap("index2", new NoShardAvailableActionException(null, "no shard available"))
        );

        FieldCapabilitiesResponse current = copyInstance(response, Version.CURRENT);
        assertEquals(Set.of("index2"), current.getFailures().keySet());
        assertEquals("no shard available", current.getFailures().get("index2").getMessage());

        Version older = VersionUtils.randomVersionBetween(random(), Version.V_3_0_0, VersionUtils.getPreviousVersion(Version.V_3_10_0));
        assertEquals(0, copyInstance(response, older).getFailures().size());
    }

    private FieldCapabilitiesResponse copyViaXContent(FieldCapabilitiesResponse response) throws IOException {
        XContentBuilder builder = MediaTypeRegistry.contentBuilder(MediaTypeRegistry.JSON);
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        try (XContentParser parser = createParser(builder)) {
            return FieldCapabilitiesResponse.fromXContent(parser);
        }
    }

    public void testEmptyResponse() throws IOException {
        FieldCapabilitiesResponse testInstance = new FieldCapabilitiesResponse();
        assertSerialization(testInstance);
    }

    private static FieldCapabilitiesResponse createSimpleResponse() {
        Map<String, FieldCapabilities> titleCapabilities = new HashMap<>();
        titleCapabilities.put("text", new FieldCapabilities("title", "text", true, false, null, null, null, Collections.emptyMap()));

        Map<String, FieldCapabilities> ratingCapabilities = new HashMap<>();
        ratingCapabilities.put(
            "long",
            new FieldCapabilities(
                "rating",
                "long",
                true,
                false,
                new String[] { "index1", "index2" },
                null,
                new String[] { "index1" },
                Collections.emptyMap()
            )
        );
        ratingCapabilities.put(
            "keyword",
            new FieldCapabilities(
                "rating",
                "keyword",
                false,
                true,
                new String[] { "index3", "index4" },
                new String[] { "index4" },
                null,
                Collections.emptyMap()
            )
        );

        Map<String, Map<String, FieldCapabilities>> responses = new HashMap<>();
        responses.put("title", titleCapabilities);
        responses.put("rating", ratingCapabilities);
        return new FieldCapabilitiesResponse(null, responses);
    }
}
