/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Index-aware search request processors: an extension point that lets a plugin change a search request on
 * the coordinating node, before the coordinator query rewrite, knowing the concrete indices the request
 * targets and their mappings.
 */
package org.opensearch.search.indexaware;
