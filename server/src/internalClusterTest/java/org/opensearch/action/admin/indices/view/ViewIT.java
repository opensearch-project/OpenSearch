/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.view;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.test.BackgroundIndexer;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;
import org.opensearch.test.OpenSearchIntegTestCase.Scope;

import java.util.List;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.is;

@ClusterScope(scope = Scope.TEST, numDataNodes = 2)
public class ViewIT extends OpenSearchIntegTestCase {

    private int createIndexWithDocs(final String indexName) throws Exception {
        createIndex(indexName);
        ensureGreen(indexName);

        final int numOfDocs = scaledRandomIntBetween(0, 200);
        try (final BackgroundIndexer indexer = new BackgroundIndexer(indexName, "_doc", client(), numOfDocs)) {
            waitForDocs(numOfDocs, indexer);
        }

        refresh(indexName);
        assertHitCount(client().prepareSearch(indexName).setSize(0).get(), numOfDocs);
        return numOfDocs;
    }

    private CreateViewAction.Response createView(final String name, final String indexPattern) throws Exception {
        final CreateViewAction.Request request = new CreateViewAction.Request(
            name,
            null,
            List.of(new CreateViewAction.Request.Target(indexPattern))
        );
        final CreateViewAction.Response response = client().admin().indices().createView(request).actionGet();
        performRemoteStoreTestAction();
        return response;
    }

    private SearchResponse searchView(final String viewName) throws Exception {
        final SearchViewAction.Request request = SearchViewAction.createRequestWith(viewName, new SearchRequest());
        final SearchResponse response = client().admin().indices().searchView(request).actionGet();
        return response;
    }

    public void testBasicOperations() throws Exception {
        final String indexInView1 = "index-1";
        final String indexInView2 = "index-2";
        final String indexNotInView = "another-index-1";

        final int indexInView1DocCount = createIndexWithDocs(indexInView1);
        final int indexInView2DocCount = createIndexWithDocs(indexInView2);
        createIndexWithDocs(indexNotInView);

        logger.info("Testing view with no matches");
        createView("no-matches", "this-pattern-will-match-nothing");
        final IndexNotFoundException ex = assertThrows(IndexNotFoundException.class, () -> searchView("no-matches"));
        assertThat(ex.getMessage(), is("no such index [this-pattern-will-match-nothing]"));

        logger.info("Testing view with exact index match");
        createView("only-index-1", "index-1");
        assertHitCount(searchView("only-index-1"), indexInView1DocCount);

        logger.info("Testing view with wildcard matches");
        createView("both-indices", "index-*");
        assertHitCount(searchView("both-indices"), indexInView1DocCount + indexInView2DocCount);
    }
}
