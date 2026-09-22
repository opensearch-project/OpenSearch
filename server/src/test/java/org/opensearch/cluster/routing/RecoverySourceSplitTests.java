/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class RecoverySourceSplitTests extends OpenSearchTestCase {

    public void testInPlaceSplitShardRecoverySourceType() {
        RecoverySource.InPlaceSplitShardRecoverySource source = RecoverySource.InPlaceSplitShardRecoverySource.INSTANCE;
        assertEquals(RecoverySource.Type.IN_PLACE_SPLIT_SHARD, source.getType());
    }

    public void testInPlaceSplitShardRecoverySourceIsSingleton() {
        assertSame(RecoverySource.InPlaceSplitShardRecoverySource.INSTANCE, RecoverySource.InPlaceSplitShardRecoverySource.INSTANCE);
    }

    public void testInPlaceSplitShardRecoverySourceDoesNotExpectEmptyRetentionLeases() {
        assertFalse(RecoverySource.InPlaceSplitShardRecoverySource.INSTANCE.expectEmptyRetentionLeases());
    }

    public void testInPlaceSplitShardRecoverySourceToString() {
        assertEquals("in-place shard split", RecoverySource.InPlaceSplitShardRecoverySource.INSTANCE.toString());
    }

    public void testSerializationRoundTrip() throws IOException {
        RecoverySource source = RecoverySource.InPlaceSplitShardRecoverySource.INSTANCE;

        BytesStreamOutput out = new BytesStreamOutput();
        source.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        RecoverySource deserialized = RecoverySource.readFrom(in);

        assertSame(RecoverySource.InPlaceSplitShardRecoverySource.INSTANCE, deserialized);
    }

    public void testTypeEnumOrdinalStability() {
        // IN_PLACE_SPLIT_SHARD must be at the end to preserve ordinals of existing types
        RecoverySource.Type[] types = RecoverySource.Type.values();
        assertEquals(RecoverySource.Type.IN_PLACE_SPLIT_SHARD, types[types.length - 1]);
    }
}
