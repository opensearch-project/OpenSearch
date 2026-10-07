/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.cluster.ClusterModule;
import org.opensearch.cluster.Diff;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * The Views feature is removed, but a {@code view} custom written by an earlier 3.x node must still deserialize during a
 * rolling upgrade. These tests pin the surviving reader to the persisted wire and XContent formats.
 */
public class ViewMetadataTests extends OpenSearchTestCase {

    private static final NamedWriteableRegistry WRITEABLES = new NamedWriteableRegistry(ClusterModule.getNamedWriteables());
    private static final NamedXContentRegistry XCONTENTS = new NamedXContentRegistry(ClusterModule.getNamedXWriteables());

    static ViewMetadata randomViewMetadata() {
        final View view = new View(
            randomAlphaOfLength(8),
            randomBoolean() ? null : randomAlphaOfLength(20),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            Set.of(new View.Target("logs-*"), new View.Target("metrics-" + randomAlphaOfLength(4)))
        );
        return new ViewMetadata(Map.of(randomAlphaOfLength(8), view));
    }

    public void testWireRoundTrip() throws IOException {
        final Metadata original = Metadata.builder().putCustom(ViewMetadata.TYPE, randomViewMetadata()).build();

        final BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        final Metadata read = Metadata.readFrom(new NamedWriteableAwareStreamInput(out.bytes().streamInput(), WRITEABLES));

        assertThat(read.custom(ViewMetadata.TYPE), equalTo(original.custom(ViewMetadata.TYPE)));
    }

    public void testDiffRoundTrip() throws IOException {
        final Metadata before = Metadata.builder().putCustom(ViewMetadata.TYPE, randomViewMetadata()).build();
        final Metadata after = Metadata.builder(before).putCustom(ViewMetadata.TYPE, randomViewMetadata()).build();

        final BytesStreamOutput out = new BytesStreamOutput();
        after.diff(before).writeTo(out);
        final StreamInput in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), WRITEABLES);
        final Diff<Metadata> diff = Metadata.readDiffFrom(in);

        assertThat(diff.apply(before).custom(ViewMetadata.TYPE), equalTo(after.custom(ViewMetadata.TYPE)));
    }

    public void testXContentRoundTrip() throws IOException {
        final Metadata original = Metadata.builder().putCustom(ViewMetadata.TYPE, randomViewMetadata()).build();

        final XContentBuilder builder = MediaTypeRegistry.JSON.contentBuilder();
        builder.startObject();
        Metadata.Builder.toXContent(
            original,
            builder,
            new ToXContent.MapParams(Map.of(Metadata.CONTEXT_MODE_PARAM, Metadata.CONTEXT_MODE_GATEWAY))
        );
        builder.endObject();

        try (
            XContentParser parser = XContentHelper.createParser(
                XCONTENTS,
                LoggingDeprecationHandler.INSTANCE,
                BytesReference.bytes(builder),
                XContentType.JSON
            )
        ) {
            final Metadata read = Metadata.Builder.fromXContent(parser);
            assertThat(read.custom(ViewMetadata.TYPE), notNullValue());
            assertThat(read.custom(ViewMetadata.TYPE), equalTo(original.custom(ViewMetadata.TYPE)));
        }
    }
}
