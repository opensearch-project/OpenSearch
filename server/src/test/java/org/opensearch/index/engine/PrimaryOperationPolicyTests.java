/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.lucene.index.Term;
import org.opensearch.index.VersionType;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

/**
 * Tests {@link DefaultPrimaryOperationPolicy}, the only provider core owns, and the defaults the
 * {@link PrimaryOperationPolicy} interface gives plugin implementations. Planning is verified
 * against a recording planner so the tests assert which planning path was taken and with which
 * operation, without needing a live engine.
 */
public class PrimaryOperationPolicyTests extends OpenSearchTestCase {

    /** Captures which planning path was invoked and with what operation. */
    private static class RecordingPlanner<T extends Engine.Operation, V extends OperationStrategy>
        implements
            OperationStrategyPlanner<T, V> {
        T asPrimary;
        T asNonPrimary;

        @Override
        public V planOperationAsPrimary(T operation) throws IOException {
            asPrimary = operation;
            return null;
        }

        @Override
        public V planOperationAsNonPrimary(T operation) throws IOException {
            asNonPrimary = operation;
            return null;
        }
    }

    private static Engine.Index primaryIndex(long seqNo) {
        return new Engine.Index(
            new Term(IdFieldMapper.NAME, "1"),
            null,
            seqNo,
            2L,
            5L,
            VersionType.INTERNAL,
            Engine.Operation.Origin.PRIMARY,
            System.nanoTime(),
            -1L,
            false,
            7L,
            3L
        );
    }

    private static Engine.Delete primaryDelete(long seqNo) {
        return new Engine.Delete(
            "1",
            new Term(IdFieldMapper.NAME, "1"),
            seqNo,
            2L,
            5L,
            VersionType.INTERNAL,
            Engine.Operation.Origin.PRIMARY,
            System.nanoTime(),
            7L,
            3L,
            "route-1"
        );
    }

    public void testDefaultPolicyCapabilities() {
        PrimaryOperationPolicy provider = DefaultPrimaryOperationPolicy.INSTANCE;
        assertFalse(provider.acceptsPreAssignedSeqNos());
    }

    public void testDefaultPolicyPlansAsPrimaryWithoutCopying() throws IOException {
        RecordingPlanner<Engine.Index, IndexingStrategy> indexPlanner = new RecordingPlanner<>();
        Engine.Index index = primaryIndex(SequenceNumbers.UNASSIGNED_SEQ_NO);
        DefaultPrimaryOperationPolicy.INSTANCE.planIndex(indexPlanner, index);
        assertSame("default provider must pass the operation through untouched", index, indexPlanner.asPrimary);
        assertNull(indexPlanner.asNonPrimary);

        RecordingPlanner<Engine.Delete, DeletionStrategy> deletePlanner = new RecordingPlanner<>();
        Engine.Delete delete = primaryDelete(SequenceNumbers.UNASSIGNED_SEQ_NO);
        DefaultPrimaryOperationPolicy.INSTANCE.planDelete(deletePlanner, delete);
        assertSame(delete, deletePlanner.asPrimary);
        assertNull(deletePlanner.asNonPrimary);
    }
}
