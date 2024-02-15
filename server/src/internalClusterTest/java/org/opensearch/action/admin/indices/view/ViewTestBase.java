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
import org.opensearch.test.BackgroundIndexer;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.List;
import java.util.stream.Collectors;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;

public abstract class ViewTestBase extends OpenSearchIntegTestCase {

    protected int createIndexWithDocs(final String indexName) throws Exception {
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

    protected GetViewAction.Response createView(final String name, final String indexPattern) throws Exception {
        return createView(name, List.of(indexPattern));
    }

    protected GetViewAction.Response createView(final String name, final List<String> targets) throws Exception {
        final CreateViewAction.Request request = new CreateViewAction.Request(
            name,
            null,
            targets.stream().map(CreateViewAction.Request.Target::new).collect(Collectors.toList())
        );
        return client().admin().indices().createView(request).actionGet();
    }

    protected GetViewAction.Response getView(final String name) {
        return client().admin().indices().getView(new GetViewAction.Request(name)).actionGet();

    }

    protected void deleteView(final String name) {
        client().admin().indices().deleteView(new DeleteViewAction.Request(name)).actionGet();
        performRemoteStoreTestAction();
    }

    protected List<String> listViewNames() {
        return client().listViewNames(new ListViewNamesAction.Request()).actionGet().getViewNames();
    }

    protected SearchResponse searchView(final String viewName) throws Exception {
        final SearchViewAction.Request request = new SearchViewAction.Request(viewName, new SearchRequest());
        final SearchResponse response = client().searchView(request).actionGet();
        return response;
    }

    protected GetViewAction.Response updateView(final String name, final String description, final String indexPattern) {
        final CreateViewAction.Request request = new CreateViewAction.Request(
            name,
            description,
            List.of(new CreateViewAction.Request.Target(indexPattern))
        );
        final GetViewAction.Response response = client().admin().indices().updateView(request).actionGet();
        return response;
    }
}
