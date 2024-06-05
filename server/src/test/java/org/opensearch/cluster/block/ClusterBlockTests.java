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

package org.opensearch.cluster.block;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.UUIDs;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static java.util.Collections.singletonMap;
import static java.util.EnumSet.copyOf;
import static org.opensearch.cluster.metadata.Metadata.CONTEXT_MODE_GATEWAY;
import static org.opensearch.test.VersionUtils.randomVersion;
import static org.hamcrest.CoreMatchers.endsWith;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.oneOf;

public class ClusterBlockTests extends OpenSearchTestCase {

    public void testSerialization() throws Exception {
        int iterations = randomIntBetween(5, 20);
        for (int i = 0; i < iterations; i++) {
            Version version = randomVersion(random());
            ClusterBlock clusterBlock = randomClusterBlock();

            BytesStreamOutput out = new BytesStreamOutput();
            out.setVersion(version);
            clusterBlock.writeTo(out);

            StreamInput in = out.bytes().streamInput();
            in.setVersion(version);
            ClusterBlock result = new ClusterBlock(in);

            assertClusterBlockEquals(clusterBlock, result);
        }
    }

    public void testToStringDanglingComma() {
        final ClusterBlock clusterBlock = randomClusterBlock();
        assertThat(clusterBlock.toString(), not(endsWith(",")));
    }

    public void testGlobalBlocksCheckedIfNoIndicesSpecified() {
        ClusterBlock globalBlock = randomClusterBlock();
        ClusterBlocks clusterBlocks = new ClusterBlocks(Collections.singleton(globalBlock), Map.of());
        ClusterBlockException exception = clusterBlocks.indicesBlockedException(randomFrom(globalBlock.levels()), new String[0]);
        assertNotNull(exception);
        assertEquals(exception.blocks(), Collections.singleton(globalBlock));
    }

    public void testRemoveIndexBlockWithId() {
        final ClusterBlocks.Builder builder = ClusterBlocks.builder();
        builder.addIndexBlock("index-1", new ClusterBlock(1, "uuid", "", true, true, true, RestStatus.OK, copyOf(ClusterBlockLevel.ALL)));
        builder.addIndexBlock("index-1", new ClusterBlock(2, "uuid", "", true, true, true, RestStatus.OK, copyOf(ClusterBlockLevel.ALL)));
        builder.addIndexBlock("index-1", new ClusterBlock(3, "uuid", "", true, true, true, RestStatus.OK, copyOf(ClusterBlockLevel.ALL)));
        builder.addIndexBlock(
            "index-1",
            new ClusterBlock(3, "other uuid", "", true, true, true, RestStatus.OK, copyOf(ClusterBlockLevel.ALL))
        );

        builder.addIndexBlock("index-2", new ClusterBlock(3, "uuid3", "", true, true, true, RestStatus.OK, copyOf(ClusterBlockLevel.ALL)));

        ClusterBlocks clusterBlocks = builder.build();
        assertThat(clusterBlocks.indices().get("index-1").size(), equalTo(4));
        assertThat(clusterBlocks.indices().get("index-2").size(), equalTo(1));

        builder.removeIndexBlockWithId("index-1", 3);
        clusterBlocks = builder.build();

        assertThat(clusterBlocks.indices().get("index-1").size(), equalTo(2));
        assertThat(clusterBlocks.hasIndexBlockWithId("index-1", 1), is(true));
        assertThat(clusterBlocks.hasIndexBlockWithId("index-1", 2), is(true));
        assertThat(clusterBlocks.indices().get("index-2").size(), equalTo(1));
        assertThat(clusterBlocks.hasIndexBlockWithId("index-2", 3), is(true));

        builder.removeIndexBlockWithId("index-2", 3);
        clusterBlocks = builder.build();

        assertThat(clusterBlocks.indices().get("index-1").size(), equalTo(2));
        assertThat(clusterBlocks.hasIndexBlockWithId("index-1", 1), is(true));
        assertThat(clusterBlocks.hasIndexBlockWithId("index-1", 2), is(true));
        assertThat(clusterBlocks.indices().get("index-2"), nullValue());
        assertThat(clusterBlocks.hasIndexBlockWithId("index-2", 3), is(false));
    }

    public void testGetIndexBlockWithId() {
        final int blockId = randomInt();
        final ClusterBlock[] clusterBlocks = new ClusterBlock[randomIntBetween(1, 5)];

        final ClusterBlocks.Builder builder = ClusterBlocks.builder();
        for (int i = 0; i < clusterBlocks.length; i++) {
            clusterBlocks[i] = new ClusterBlock(blockId, "uuid" + i, "", true, true, true, RestStatus.OK, copyOf(ClusterBlockLevel.ALL));
            builder.addIndexBlock("index", clusterBlocks[i]);
        }

        assertThat(builder.build().indices().get("index").size(), equalTo(clusterBlocks.length));
        assertThat(builder.build().getIndexBlockWithId("index", blockId), is(oneOf(clusterBlocks)));
        assertThat(builder.build().getIndexBlockWithId("index", randomValueOtherThan(blockId, OpenSearchTestCase::randomInt)), nullValue());
    }

    public void testToXContent_APIMode() throws IOException {
        ClusterBlock clusterBlock = randomClusterBlock();
        XContentBuilder builder = JsonXContent.contentBuilder().prettyPrint();
        builder.startObject();
        clusterBlock.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        String expectedString = "{\n" + getExpectedXContentFragment(clusterBlock, "  ", false) + "\n}";

        assertEquals(expectedString, builder.toString());
    }

    public void testToXContent_GatewayMode() throws IOException {
        ClusterBlock clusterBlock = randomClusterBlock();
        XContentBuilder builder = JsonXContent.contentBuilder().prettyPrint();
        builder.startObject();
        clusterBlock.toXContent(builder, new ToXContent.MapParams(singletonMap(Metadata.CONTEXT_MODE_PARAM, CONTEXT_MODE_GATEWAY)));
        builder.endObject();

        String expectedString = "{\n" + getExpectedXContentFragment(clusterBlock, "  ", true) + "\n}";

        assertEquals(expectedString, builder.toString());
    }

    public void testFromXContent() throws IOException {
        ClusterBlock clusterBlock = randomClusterBlock();
        boolean humanReadable = randomBoolean();
        final MediaType mediaType = MediaTypeRegistry.JSON;
        BytesReference originalBytes = toShuffledXContent(clusterBlock, mediaType, ToXContent.EMPTY_PARAMS, humanReadable);
        ClusterBlock parsedBlock = ClusterBlock.fromXContent(createParser(mediaType.xContent(), originalBytes), clusterBlock.id());
        assertEquals(clusterBlock, parsedBlock);
        assertEquals(clusterBlock.description(), parsedBlock.description());
        assertEquals(clusterBlock.retryable(), parsedBlock.retryable());
        assertEquals(clusterBlock.disableStatePersistence(), parsedBlock.disableStatePersistence());
        assertArrayEquals(clusterBlock.levels().toArray(), parsedBlock.levels().toArray());
    }

    static String getExpectedXContentFragment(ClusterBlock clusterBlock, String indent, boolean gatewayMode) {
        return indent
            + "\""
            + clusterBlock.id()
            + "\" : {\n"
            + (clusterBlock.uuid() != null ? indent + "  \"uuid\" : \"" + clusterBlock.uuid() + "\",\n" : "")
            + indent
            + "  \"description\" : \""
            + clusterBlock.description()
            + "\",\n"
            + indent
            + "  \"retryable\" : "
            + clusterBlock.retryable()
            + ",\n"
            + (clusterBlock.disableStatePersistence()
                ? indent + "  \"disable_state_persistence\" : " + clusterBlock.disableStatePersistence() + ",\n"
                : "")
            + String.format(
                Locale.ROOT,
                indent + "  \"levels\" : [%s]",
                clusterBlock.levels().isEmpty()
                    ? " "
                    : "\n"
                        + String.join(
                            ",\n",
                            clusterBlock.levels()
                                .stream()
                                .map(level -> indent + "    \"" + level.name().toLowerCase(Locale.ROOT) + "\"")
                                .toArray(String[]::new)
                        )
                        + "\n  "
                        + indent
            )
            + (gatewayMode
                ? ",\n"
                    + indent
                    + "  \"status\" : \""
                    + clusterBlock.status()
                    + "\",\n"
                    + indent
                    + "  \"allow_release_resources\" : "
                    + clusterBlock.isAllowReleaseResources()
                    + "\n"
                : "\n")
            + indent
            + "}";
    }

    static ClusterBlock randomClusterBlock() {
        return clusterBlockWithId(randomInt());
    }

    static ClusterBlock clusterBlockWithId(int id) {
        final String uuid = randomBoolean() ? UUIDs.randomBase64UUID() : null;
        final List<ClusterBlockLevel> levels = Arrays.asList(ClusterBlockLevel.values());
        return new ClusterBlock(
            id,
            uuid,
            "cluster block #" + randomInt(),
            randomBoolean(),
            randomBoolean(),
            randomBoolean(),
            randomFrom(RestStatus.values()),
            copyOf(randomSubsetOf(randomIntBetween(1, levels.size()), levels))
        );
    }

    private void assertClusterBlockEquals(final ClusterBlock expected, final ClusterBlock actual) {
        assertEquals(expected, actual);
        assertThat(actual.id(), equalTo(expected.id()));
        assertThat(actual.uuid(), equalTo(expected.uuid()));
        assertThat(actual.status(), equalTo(expected.status()));
        assertThat(actual.description(), equalTo(expected.description()));
        assertThat(actual.retryable(), equalTo(expected.retryable()));
        assertThat(actual.disableStatePersistence(), equalTo(expected.disableStatePersistence()));
        assertArrayEquals(actual.levels().toArray(), expected.levels().toArray());
    }
}
