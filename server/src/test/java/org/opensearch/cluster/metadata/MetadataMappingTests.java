/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.Version;
import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;

public class MetadataMappingTests extends OpenSearchTestCase {

    private static final String MAPPING_JSON = """
        {
          "properties": {
            "title":   { "type": "keyword" },
            "year":    { "type": "integer" }
          }
        }
        """;

    private static IndexMetadata newIndexWithMapping(String name, String mappingJson) throws IOException {
        return IndexMetadata.builder(name)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT.id)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            )
            .putMapping(new MappingMetadata(new CompressedXContent(mappingJson)))
            .build();
    }

    public void testCanonicalizesIdenticalMappings() throws Exception {
        Metadata metadata = Metadata.builder()
            .put(newIndexWithMapping("index-1", MAPPING_JSON), false)
            .put(newIndexWithMapping("index-2", MAPPING_JSON), false)
            .build();

        assertSame(metadata.index("index-1").mapping(), metadata.index("index-2").mapping());
    }

    public void testDoesNotCanonicalizeDifferentMappings() throws Exception {
        Metadata metadata = Metadata.builder()
            .put(newIndexWithMapping("index-1", MAPPING_JSON), false)
            .put(newIndexWithMapping("index-2", "{\"properties\":{\"score\":{\"type\":\"float\"}}}"), false)
            .build();

        assertNotSame(metadata.index("index-1").mapping(), metadata.index("index-2").mapping());
    }

    public void testDoesNotRebuildCanonicalizedIndices() throws Exception {
        Metadata metadata = Metadata.builder()
            .put(newIndexWithMapping("index-1", MAPPING_JSON), false)
            .put(newIndexWithMapping("index-2", MAPPING_JSON), false)
            .build();

        Metadata rebuiltMetadata = Metadata.builder(metadata).build();

        assertSame(metadata.index("index-1"), rebuiltMetadata.index("index-1"));
        assertSame(metadata.index("index-2"), rebuiltMetadata.index("index-2"));
    }

    public void testDeletingIndexRetainsCanonicalMapping() throws Exception {
        Metadata metadata = Metadata.builder()
            .put(newIndexWithMapping("index-1", MAPPING_JSON), false)
            .put(newIndexWithMapping("index-2", MAPPING_JSON), false)
            .build();

        Metadata afterDeletion = Metadata.builder(metadata).remove("index-1").build();

        assertNull(afterDeletion.index("index-1"));
        assertSame(metadata.index("index-2"), afterDeletion.index("index-2"));
        assertNotNull(afterDeletion.index("index-2").mapping());
    }

    public void testReadCanonicalizesMappings() throws Exception {
        Metadata metadata = Metadata.builder()
            .put(newIndexWithMapping("index-1", MAPPING_JSON), false)
            .put(newIndexWithMapping("index-2", MAPPING_JSON), false)
            .build();

        BytesStreamOutput out = new BytesStreamOutput();
        metadata.writeTo(out);

        NamedWriteableRegistry registry = new NamedWriteableRegistry(
            List.of(new NamedWriteableRegistry.Entry(Metadata.Custom.class, IndexGraveyard.TYPE, IndexGraveyard::new))
        );
        StreamInput in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), registry);
        Metadata deserializedMetadata = Metadata.readFrom(in);

        assertSame(deserializedMetadata.index("index-1").mapping(), deserializedMetadata.index("index-2").mapping());
    }
}
