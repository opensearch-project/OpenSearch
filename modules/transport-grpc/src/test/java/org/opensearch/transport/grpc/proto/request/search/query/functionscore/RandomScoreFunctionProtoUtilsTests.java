/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.query.functionscore;

import org.opensearch.index.query.functionscore.RandomScoreFunctionBuilder;
import org.opensearch.index.query.functionscore.ScoreFunctionBuilder;
import org.opensearch.protobufs.RandomScoreFunction;
import org.opensearch.protobufs.RandomScoreFunctionSeed;
import org.opensearch.test.OpenSearchTestCase;

import static org.hamcrest.Matchers.instanceOf;

public class RandomScoreFunctionProtoUtilsTests extends OpenSearchTestCase {

    public void testFromProtoWithValidRandomScoreFunction() {
        // Create a random score function with int32 seed
        RandomScoreFunctionSeed seed = RandomScoreFunctionSeed.newBuilder().setInt32(12345).build();

        RandomScoreFunction randomScore = RandomScoreFunction.newBuilder().setField("_seq_no").setSeed(seed).build();

        ScoreFunctionBuilder<?> result = RandomScoreFunctionProtoUtils.fromProto(randomScore);

        assertThat(result, instanceOf(RandomScoreFunctionBuilder.class));
        RandomScoreFunctionBuilder randomScoreBuilder = (RandomScoreFunctionBuilder) result;
        assertEquals("_seq_no", randomScoreBuilder.getField());
        assertEquals(Integer.valueOf(12345), randomScoreBuilder.getSeed());
    }

    public void testFromProtoWithInt64Seed() {
        // Create a random score function with int64 seed
        RandomScoreFunctionSeed seed = RandomScoreFunctionSeed.newBuilder().setInt64(9876543210L).build();

        RandomScoreFunction randomScore = RandomScoreFunction.newBuilder().setField("_seq_no").setSeed(seed).build();

        ScoreFunctionBuilder<?> result = RandomScoreFunctionProtoUtils.fromProto(randomScore);

        assertThat(result, instanceOf(RandomScoreFunctionBuilder.class));
        RandomScoreFunctionBuilder randomScoreBuilder = (RandomScoreFunctionBuilder) result;
        assertEquals("_seq_no", randomScoreBuilder.getField());
        // Long seeds are converted to int via Long.hashCode() internally
        assertEquals(Integer.valueOf(Long.hashCode(9876543210L)), randomScoreBuilder.getSeed());
    }

    public void testFromProtoWithStringSeed() {
        // Create a random score function with string seed
        RandomScoreFunctionSeed seed = RandomScoreFunctionSeed.newBuilder().setString("test_seed").build();

        RandomScoreFunction randomScore = RandomScoreFunction.newBuilder().setField("_id").setSeed(seed).build();

        ScoreFunctionBuilder<?> result = RandomScoreFunctionProtoUtils.fromProto(randomScore);

        assertThat(result, instanceOf(RandomScoreFunctionBuilder.class));
        RandomScoreFunctionBuilder randomScoreBuilder = (RandomScoreFunctionBuilder) result;
        assertEquals("_id", randomScoreBuilder.getField());
        assertEquals(Integer.valueOf("test_seed".hashCode()), randomScoreBuilder.getSeed());
    }

    public void testFromProtoWithEmptyField() {
        // Create a random score function with empty field
        RandomScoreFunctionSeed seed = RandomScoreFunctionSeed.newBuilder().setInt32(42).build();

        RandomScoreFunction randomScore = RandomScoreFunction.newBuilder().setField("").setSeed(seed).build();

        ScoreFunctionBuilder<?> result = RandomScoreFunctionProtoUtils.fromProto(randomScore);

        assertThat(result, instanceOf(RandomScoreFunctionBuilder.class));
        RandomScoreFunctionBuilder randomScoreBuilder = (RandomScoreFunctionBuilder) result;
        assertNull(randomScoreBuilder.getField());
        assertEquals(Integer.valueOf(42), randomScoreBuilder.getSeed());
    }

    public void testFromProtoWithNoSeed() {
        // Create a random score function without seed
        RandomScoreFunction randomScore = RandomScoreFunction.newBuilder().setField("_seq_no").build();

        ScoreFunctionBuilder<?> result = RandomScoreFunctionProtoUtils.fromProto(randomScore);

        assertThat(result, instanceOf(RandomScoreFunctionBuilder.class));
        RandomScoreFunctionBuilder randomScoreBuilder = (RandomScoreFunctionBuilder) result;
        assertEquals("_seq_no", randomScoreBuilder.getField());
        // Seed should be null when not set
        assertNull(randomScoreBuilder.getSeed());
    }

    public void testFromProtoWithNullRandomScoreFunction() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> { RandomScoreFunctionProtoUtils.fromProto(null); }
        );
        assertEquals("RandomScoreFunction cannot be null", exception.getMessage());
    }
}
