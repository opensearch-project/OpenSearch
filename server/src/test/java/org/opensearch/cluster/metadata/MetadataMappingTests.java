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

public class MetadataMappingTests extends OpenSearchTestCase {

    private static final String MAPPING_JSON = """
        {
          "properties": {
            "title":   { "type": "keyword" },
            "year":    { "type": "integer" }
          }
        }
        """;

    private static IndexMetadata newIndexWithMapping(String name, String mappingJson) throws Exception {
        MappingMetadata mm = new MappingMetadata(new CompressedXContent(mappingJson));
        return IndexMetadata.builder(name)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT.id) // or your current
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            )
            .putMapping(mm)
            .build();
    }

    private static CompressedXContent cx(String json) {
        try {
            return new CompressedXContent(json);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void testDedupsIdenticalMappingsByIdentity() throws Exception {
        Metadata.Builder mb = Metadata.builder();
        IndexMetadata idx1 = newIndexWithMapping("idx-000001", MAPPING_JSON);
        IndexMetadata idx2 = newIndexWithMapping("idx-000002", MAPPING_JSON + ""); // intentional cloning
        mb.put(idx1, false);
        mb.put(idx2, false);

        Metadata md = mb.build();

        CompressedXContent aAfter = md.index("idx-000001").mapping().source();
        CompressedXContent bAfter = md.index("idx-000002").mapping().source();

        // Identity must be the same now
        assertSame("De-duplicating should make both point to same instance", aAfter, bAfter);
    }

    public void testPartialSharingOnlyDedupsMatchingOnes() throws Exception {
        String different = """
                {"properties":{"title":{"type":"keyword"},"score":{"type":"float"}}}
            """;

        Metadata md = Metadata.builder()
            .put(newIndexWithMapping("idx-A", MAPPING_JSON), false)
            .put(newIndexWithMapping("idx-B", MAPPING_JSON + ""), false) // intentional cloning
            .put(newIndexWithMapping("idx-C", different), false)
            .build();

        var aSrc = md.index("idx-A").mapping().source();
        var bSrc = md.index("idx-B").mapping().source();
        var cSrc = md.index("idx-C").mapping().source();

        assertSame("A and B should share the same canonical instance", aSrc, bSrc);
        assertNotSame("C should not share with A/B", aSrc, cSrc);
    }

    public void testWriteReadRoundTripDedupsMappings() throws Exception {
        // Build once
        var builder = Metadata.builder()
            .put(newIndexWithMapping("i1", MAPPING_JSON), false)
            .put(newIndexWithMapping("i2", MAPPING_JSON + ""), false); // intentional cloning

        Metadata md1 = builder.build();
        var s1 = md1.index("i1").mapping().source();
        var s2 = md1.index("i2").mapping().source();
        assertSame(s1, s2);

        BytesStreamOutput out = new BytesStreamOutput();
        md1.writeTo(out);

        // Minimal registry: only IndexGraveyard (the default Custom in Builder)
        NamedWriteableRegistry registry = new NamedWriteableRegistry(
            java.util.List.of(new NamedWriteableRegistry.Entry(Metadata.Custom.class, IndexGraveyard.TYPE, IndexGraveyard::new))
        );

        StreamInput in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), registry);
        Metadata md2 = Metadata.readFrom(in);

        var t1 = md2.index("i1").mapping().source();
        var t2 = md2.index("i2").mapping().source();
        // After readFrom(), builder.build() was called internally; de-duplicating should apply again.
        assertSame("Round-trip should still end up deduplicated", t1, t2);
    }
}
