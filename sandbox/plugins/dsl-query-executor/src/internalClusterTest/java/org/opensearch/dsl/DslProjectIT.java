/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl;

import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.fetch.subphase.FetchSourceContext;

/**
 * Integration tests for DSL _source filtering (projection) conversion.
 * Uses matchAllQuery; focus is on _source includes/excludes behavior.
 */
public class DslProjectIT extends DslIntegTestBase {

    public void testNoSourceFiltering() {
        createTestIndex();
        assertOk(search(new SearchSourceBuilder()));
    }

    public void testIncludeSpecificFields() {
        createTestIndex();
        assertOk(search(new SearchSourceBuilder().fetchSource(new String[]{"name", "price"}, null)));
    }

    public void testExcludeFields() {
        createTestIndex();
        assertOk(search(new SearchSourceBuilder().fetchSource(new String[]{}, new String[]{"rating"})));
    }

    public void testSourceDisabled() {
        createTestIndex();
        assertOk(search(new SearchSourceBuilder().fetchSource(false)));
    }

    public void testWildcardIncludes() {
        createTestIndex();
        assertOk(search(new SearchSourceBuilder().fetchSource(new String[]{"na*"}, null)));
    }

    public void testWildcardExcludes() {
        createTestIndex();
        assertOk(search(new SearchSourceBuilder().fetchSource(new String[]{}, new String[]{"ra*"})));
    }
}
