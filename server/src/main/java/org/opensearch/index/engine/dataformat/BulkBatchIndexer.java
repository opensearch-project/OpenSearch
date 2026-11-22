/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.action.bulk.BulkShardRequest;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.mapper.MapperService;

/**
 * An {@link IndexingExecutionEngine} that can parse the documents of a whole shard bulk ahead of the
 * item loop, in one pass, into its own columnar form.
 * <p>
 * Called once per shard bulk on the primary, before any item executes. Returning {@code null} means the
 * bulk runs entirely on the per-document path; the engine decides from the index (today only an index
 * with {@code index.append_only.enabled}), the mapping (flat scalar leaves only) and the items. A plan
 * never changes what an item answers: seq_no, version, primary term, translog and the item response are
 * produced by the unchanged item loop, and any document the batch did not accept is parsed by the
 * mapper as today, so its error text is the mapper's.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface BulkBatchIndexer {

    /**
     * Parses the batch-eligible {@code index}/{@code create} items of {@code request}.
     *
     * @param request       the shard bulk about to execute on the primary
     * @param indexMetadata the shard's current index metadata
     * @param mapperService the shard's mapper service; the plan pins its current document mapper
     * @return the plan, or {@code null} to run the bulk per document. Must not throw for a bad document:
     *         such documents are left out of the plan.
     */
    @Nullable
    BulkBatchPlan planBulkBatch(BulkShardRequest request, IndexMetadata indexMetadata, MapperService mapperService);
}
