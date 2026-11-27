/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.action.DocWriteRequest;
import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.lease.Releasable;
import org.opensearch.index.mapper.ParsedDocument;

/**
 * The documents of one shard bulk that a {@link BulkBatchIndexer} parsed ahead of the item loop.
 * <p>
 * The item loop of the primary is unchanged: for every item it asks {@link #parsedDocument} and, when a
 * document comes back, indexes it instead of parsing the item's source. An item the plan does not hold
 * (a non-index operation, a document the batch parser did not take, any item once {@link #isCurrent}
 * turns false) is executed exactly as without a plan. The plan is closed once per shard bulk, after the
 * last item completed; a document obtained from it must not be used after that.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface BulkBatchPlan extends Releasable {

    /**
     * Returns the pre-parsed document for {@code item}, or {@code null} when the item is not part of the
     * batch and must take the per-document path. Each call builds a fresh document; the result carries the
     * item's id, routing and source and a {@link DocumentInput} that writes the item's row.
     */
    @Nullable
    ParsedDocument parsedDocument(DocWriteRequest<?> item);

    /**
     * Whether the mapping the plan was built against is still the shard's current mapping. The item loop
     * checks this before using a document, so a mapping update that lands mid-bulk sends the remaining
     * items down the per-document path and no batched document is ever written under a mapping it was not
     * parsed against.
     */
    boolean isCurrent();

    /**
     * Delivers the parsed values of {@code item}'s row to {@code target} through
     * {@link DocumentInput#addField}, with the Java value the mapper would have passed for each field.
     * A format that composes several formats uses this to feed the secondary formats from the batch;
     * the primary format writes its own columns directly. Fields no format other than the batch's own
     * format has a capability for may be skipped.
     */
    void replay(DocWriteRequest<?> item, DocumentInput<?> target);

    /** Number of items of the shard bulk this plan holds a document for. */
    int batchedItems();
}
