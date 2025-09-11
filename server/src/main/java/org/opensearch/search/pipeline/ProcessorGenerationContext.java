/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline;

import org.opensearch.action.search.SearchRequest;

/**
 * A DTO to hold the context used for a system generated factory to evaluate should we generate the processor
 */
public record ProcessorGenerationContext(SearchRequest searchRequest) {
}
