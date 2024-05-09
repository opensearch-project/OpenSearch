/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.block;

import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.XContentTestUtils;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.opensearch.cluster.block.ClusterBlockTests.getExpectedXContentFragment;
import static org.opensearch.cluster.block.ClusterBlockTests.randomClusterBlock;

public class ClusterBlocksTests extends OpenSearchTestCase {
    public void testToXContent() throws IOException {
        ClusterBlocks clusterBlocks = randomClusterBlocks();
        XContentBuilder builder = JsonXContent.contentBuilder().prettyPrint();
        builder.startObject();
        clusterBlocks.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        String expectedXContent = "{\n"
            + "  \"blocks\" : {\n"
            + String.format(
                Locale.ROOT,
                "%s",
                clusterBlocks.global().isEmpty()
                    ? ""
                    : "    \"global\" : {\n"
                        + clusterBlocks.global()
                            .stream()
                            .map(clusterBlock -> getExpectedXContentFragment(clusterBlock, "      ", false))
                            .collect(Collectors.joining(",\n"))
                        + "\n    }"
                        + (!clusterBlocks.indices().isEmpty() ? "," : "")
                        + "\n"
            )
            + String.format(
                Locale.ROOT,
                "%s",
                clusterBlocks.indices().isEmpty()
                    ? ""
                    : "    \"indices\" : {\n"
                        + clusterBlocks.indices()
                            .entrySet()
                            .stream()
                            .map(
                                entry -> "      \""
                                    + entry.getKey()
                                    + "\" : {"
                                    + (entry.getValue().isEmpty()
                                        ? " }"
                                        : "\n"
                                            + entry.getValue()
                                                .stream()
                                                .map(clusterBlock -> getExpectedXContentFragment(clusterBlock, "        ", false))
                                                .collect(Collectors.joining(",\n"))
                                            + "\n      }")
                            )
                            .collect(Collectors.joining(",\n"))
                        + "\n    }\n"
            )
            + "  }\n"
            + "}";

        assertEquals(expectedXContent, builder.toString());
    }

    public void testFromXContent() throws IOException {
        doFromXContentTestWithRandomFields(false);
    }

    public void testFromXContentWithRandomFields() throws IOException {
        doFromXContentTestWithRandomFields(true);
    }

    private void doFromXContentTestWithRandomFields(boolean addRandomFields) throws IOException {
        ClusterBlocks clusterBlocks = randomClusterBlocks();
        boolean humanReadable = randomBoolean();
        final MediaType mediaType = MediaTypeRegistry.JSON;
        BytesReference originalBytes = toShuffledXContent(clusterBlocks, mediaType, ToXContent.EMPTY_PARAMS, humanReadable);

        if (addRandomFields) {
            String unsupportedField = "unsupported_field";
            BytesReference mutated = BytesReference.bytes(
                XContentTestUtils.insertIntoXContent(
                    mediaType.xContent(),
                    originalBytes,
                    Collections.singletonList("blocks"),
                    () -> unsupportedField,
                    () -> randomAlphaOfLengthBetween(3, 10)
                )
            );
            IllegalArgumentException exception = expectThrows(
                IllegalArgumentException.class,
                () -> ClusterBlocks.fromXContent(createParser(mediaType.xContent(), mutated))
            );
            assertEquals("unknown field [" + unsupportedField + "]", exception.getMessage());
        } else {
            try (XContentParser parser = createParser(JsonXContent.jsonXContent, originalBytes)) {
                ClusterBlocks parsedClusterBlocks = ClusterBlocks.fromXContent(parser);
                assertEquals(clusterBlocks.global().size(), parsedClusterBlocks.global().size());
                assertEquals(clusterBlocks.indices().size(), parsedClusterBlocks.indices().size());
                clusterBlocks.global().forEach(clusterBlock -> assertTrue(parsedClusterBlocks.global().contains(clusterBlock)));
                clusterBlocks.indices().forEach((key, value) -> {
                    assertTrue(parsedClusterBlocks.indices().containsKey(key));
                    value.forEach(clusterBlock -> assertTrue(parsedClusterBlocks.indices().get(key).contains(clusterBlock)));
                });
            }
        }
    }

    private ClusterBlocks randomClusterBlocks() {
        int randomGlobalBlocks = randomIntBetween(0, 10);
        Set<ClusterBlock> globalBlocks = new HashSet<>();
        for (int i = 0; i < randomGlobalBlocks; i++) {
            globalBlocks.add(randomClusterBlock());
        }

        int randomIndices = randomIntBetween(0, 10);
        Map<String, Set<ClusterBlock>> indexBlocks = new HashMap<>();
        for (int i = 0; i < randomIndices; i++) {
            int randomIndexBlocks = randomIntBetween(0, 10);
            Set<ClusterBlock> blocks = new HashSet<>();
            for (int j = 0; j < randomIndexBlocks; j++) {
                blocks.add(randomClusterBlock());
            }
            indexBlocks.put("index-" + i, blocks);
        }
        return new ClusterBlocks(globalBlocks, indexBlocks);
    }
}
