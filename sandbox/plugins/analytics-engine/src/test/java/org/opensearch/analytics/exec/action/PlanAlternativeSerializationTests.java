/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.action;

import org.opensearch.analytics.spi.DelegatedExpression;
import org.opensearch.analytics.spi.FilterDelegationInstructionNode;
import org.opensearch.analytics.spi.FilterTreeShape;
import org.opensearch.analytics.spi.FinalAggregateInstructionNode;
import org.opensearch.analytics.spi.InstructionNode;
import org.opensearch.analytics.spi.InstructionType;
import org.opensearch.analytics.spi.PartialAggregateInstructionNode;
import org.opensearch.analytics.spi.ShardScanInstructionNode;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;

/**
 * Wire serialization round-trip tests for {@link FragmentExecutionRequest.PlanAlternative}.
 */
public class PlanAlternativeSerializationTests extends OpenSearchTestCase {

    public void testRoundTripWithShardScanOnly() throws IOException {
        List<InstructionNode> instructions = List.of(new ShardScanInstructionNode());
        FragmentExecutionRequest.PlanAlternative original = new FragmentExecutionRequest.PlanAlternative(
            "datafusion",
            new byte[] { 1, 2, 3 },
            instructions
        );

        FragmentExecutionRequest.PlanAlternative deserialized = roundTrip(original);

        assertEquals("datafusion", deserialized.getBackendId());
        assertArrayEquals(new byte[] { 1, 2, 3 }, deserialized.getFragmentBytes());
        assertEquals(1, deserialized.getInstructions().size());
        assertEquals(InstructionType.SETUP_SHARD_SCAN, deserialized.getInstructions().get(0).type());
    }

    public void testRoundTripWithFilterDelegation() throws IOException {
        List<DelegatedExpression> expressions = List.of(
            new DelegatedExpression(1, "lucene", new byte[] { 10, 20 }),
            new DelegatedExpression(2, "lucene", new byte[] { 30, 40 })
        );
        FilterDelegationInstructionNode filterNode = new FilterDelegationInstructionNode(FilterTreeShape.CONJUNCTIVE, 2, expressions);
        List<InstructionNode> instructions = List.of(new ShardScanInstructionNode(), filterNode);
        FragmentExecutionRequest.PlanAlternative original = new FragmentExecutionRequest.PlanAlternative(
            "datafusion",
            new byte[] { 5, 6 },
            instructions
        );

        FragmentExecutionRequest.PlanAlternative deserialized = roundTrip(original);

        assertEquals(2, deserialized.getInstructions().size());
        assertEquals(InstructionType.SETUP_SHARD_SCAN, deserialized.getInstructions().get(0).type());
        assertEquals(InstructionType.SETUP_FILTER_DELEGATION_FOR_INDEX, deserialized.getInstructions().get(1).type());

        FilterDelegationInstructionNode deserializedFilter = (FilterDelegationInstructionNode) deserialized.getInstructions().get(1);
        assertEquals(FilterTreeShape.CONJUNCTIVE, deserializedFilter.getTreeShape());
        assertEquals(2, deserializedFilter.getDelegatedPredicateCount());
        assertEquals(2, deserializedFilter.getDelegatedQueries().size());
        assertEquals(1, deserializedFilter.getDelegatedQueries().get(0).getAnnotationId());
        assertEquals("lucene", deserializedFilter.getDelegatedQueries().get(0).getAcceptingBackendId());
        assertArrayEquals(new byte[] { 10, 20 }, deserializedFilter.getDelegatedQueries().get(0).getExpressionBytes());
    }

    public void testRoundTripWithAllTypes() throws IOException {
        List<InstructionNode> instructions = List.of(
            new ShardScanInstructionNode(),
            new FilterDelegationInstructionNode(
                FilterTreeShape.INTERLEAVED_BOOLEAN_EXPRESSION,
                1,
                List.of(new DelegatedExpression(3, "lucene", new byte[] { 99 }))
            ),
            new PartialAggregateInstructionNode(),
            new FinalAggregateInstructionNode()
        );
        FragmentExecutionRequest.PlanAlternative original = new FragmentExecutionRequest.PlanAlternative(
            "datafusion",
            new byte[] { 7 },
            instructions
        );

        FragmentExecutionRequest.PlanAlternative deserialized = roundTrip(original);

        assertEquals(4, deserialized.getInstructions().size());
        assertEquals(InstructionType.SETUP_SHARD_SCAN, deserialized.getInstructions().get(0).type());
        assertEquals(InstructionType.SETUP_FILTER_DELEGATION_FOR_INDEX, deserialized.getInstructions().get(1).type());
        assertEquals(InstructionType.SETUP_PARTIAL_AGGREGATE, deserialized.getInstructions().get(2).type());
        assertEquals(InstructionType.SETUP_FINAL_AGGREGATE, deserialized.getInstructions().get(3).type());
    }

    private FragmentExecutionRequest.PlanAlternative roundTrip(FragmentExecutionRequest.PlanAlternative original) throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        return new FragmentExecutionRequest.PlanAlternative(in);
    }
}
