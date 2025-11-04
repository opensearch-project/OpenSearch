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
import org.opensearch.protobufs.FunctionScoreContainer;
import org.opensearch.protobufs.RandomScoreFunction;
import org.opensearch.protobufs.RandomScoreFunctionSeed;
import org.opensearch.test.OpenSearchTestCase;

import static org.hamcrest.Matchers.instanceOf;

public class RandomScoreFunctionProtoConverterTests extends OpenSearchTestCase {

    private RandomScoreFunctionProtoConverter converter;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        converter = new RandomScoreFunctionProtoConverter();
    }

    public void testGetHandledFunctionCase() {
        assertEquals(FunctionScoreContainer.FunctionScoreContainerCase.RANDOM_SCORE, converter.getHandledFunctionCase());
    }

    public void testFromProtoWithValidRandomScoreFunction() {
        // Create a random score function with int32 seed
        RandomScoreFunctionSeed seed = RandomScoreFunctionSeed.newBuilder().setInt32(12345).build();

        RandomScoreFunction randomScore = RandomScoreFunction.newBuilder().setField("_seq_no").setSeed(seed).build();

        FunctionScoreContainer container = FunctionScoreContainer.newBuilder().setRandomScore(randomScore).setWeight(1.5f).build();

        ScoreFunctionBuilder<?> result = converter.fromProto(container);

        assertThat(result, instanceOf(RandomScoreFunctionBuilder.class));
        RandomScoreFunctionBuilder randomScoreBuilder = (RandomScoreFunctionBuilder) result;
        assertEquals("_seq_no", randomScoreBuilder.getField());
        assertEquals(Integer.valueOf(12345), randomScoreBuilder.getSeed());
    }

    public void testFromProtoWithStringSeed() {
        // Create a random score function with string seed
        RandomScoreFunctionSeed seed = RandomScoreFunctionSeed.newBuilder().setString("test_seed").build();

        RandomScoreFunction randomScore = RandomScoreFunction.newBuilder().setField("_id").setSeed(seed).build();

        FunctionScoreContainer container = FunctionScoreContainer.newBuilder().setRandomScore(randomScore).build();

        ScoreFunctionBuilder<?> result = converter.fromProto(container);

        assertThat(result, instanceOf(RandomScoreFunctionBuilder.class));
        RandomScoreFunctionBuilder randomScoreBuilder = (RandomScoreFunctionBuilder) result;
        assertEquals("_id", randomScoreBuilder.getField());
        assertEquals(Integer.valueOf("test_seed".hashCode()), randomScoreBuilder.getSeed());
    }

    public void testFromProtoWithEmptyField() {
        // Create a random score function with empty field
        RandomScoreFunctionSeed seed = RandomScoreFunctionSeed.newBuilder().setInt32(42).build();

        RandomScoreFunction randomScore = RandomScoreFunction.newBuilder().setField("").setSeed(seed).build();

        FunctionScoreContainer container = FunctionScoreContainer.newBuilder().setRandomScore(randomScore).build();

        ScoreFunctionBuilder<?> result = converter.fromProto(container);

        assertThat(result, instanceOf(RandomScoreFunctionBuilder.class));
        RandomScoreFunctionBuilder randomScoreBuilder = (RandomScoreFunctionBuilder) result;
        assertNull(randomScoreBuilder.getField());
        assertEquals(Integer.valueOf(42), randomScoreBuilder.getSeed());
    }

    public void testFromProtoWithNoSeed() {
        // Create a random score function without seed
        RandomScoreFunction randomScore = RandomScoreFunction.newBuilder().setField("_seq_no").build();

        FunctionScoreContainer container = FunctionScoreContainer.newBuilder().setRandomScore(randomScore).build();

        ScoreFunctionBuilder<?> result = converter.fromProto(container);

        assertThat(result, instanceOf(RandomScoreFunctionBuilder.class));
        RandomScoreFunctionBuilder randomScoreBuilder = (RandomScoreFunctionBuilder) result;
        assertEquals("_seq_no", randomScoreBuilder.getField());
        // Seed should be null when not set
        assertNull(randomScoreBuilder.getSeed());
    }

    public void testFromProtoWithNullContainer() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> { converter.fromProto(null); });
        assertEquals("FunctionScoreContainer must contain a RandomScoreFunction", exception.getMessage());
    }

    public void testFromProtoWithWrongFunctionCase() {
        // Create a container with a different function case
        FunctionScoreContainer container = FunctionScoreContainer.newBuilder().setWeight(1.0f).build();

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> { converter.fromProto(container); });
        assertEquals("FunctionScoreContainer must contain a RandomScoreFunction", exception.getMessage());
    }
}
