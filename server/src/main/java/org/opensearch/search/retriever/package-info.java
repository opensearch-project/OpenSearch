/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Internal support classes for the retriever framework that replay a coordinator-computed ranking
 * onto the final search, including the query and sort used to carry and apply that ranking on each shard.
 */
package org.opensearch.search.retriever;
