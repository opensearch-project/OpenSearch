/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.lucene.index;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.lease.Releasable;

/**
 * An opened {@link BatchPlan}. While it is open, the provider serves the per-document reads of the batch's documents
 * made on the opening thread (doc values of its per-document view, and stored fields) from the batch; every other
 * read goes the ordinary way. Closing it, on the opening thread, frees everything the batch holds.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface BatchedDocumentValues extends Releasable {}
