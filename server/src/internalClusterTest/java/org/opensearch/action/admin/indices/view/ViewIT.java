/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.view;

import org.opensearch.ResourceNotFoundException;
import org.opensearch.cluster.metadata.View;
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;
import org.opensearch.test.OpenSearchIntegTestCase.Scope;
import org.hamcrest.MatcherAssert;

import java.util.List;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

@ClusterScope(scope = Scope.TEST, numDataNodes = 2)
public class ViewIT extends ViewTestBase {

    public void testCreateView() throws Exception {
        final String viewName = "test-view";
        final String indexPattern = "test-index-*";

        logger.info("Testing createView with valid parameters");
        final View view = createView(viewName, indexPattern).getView();
        MatcherAssert.assertThat(view.getName(), is(viewName));
        MatcherAssert.assertThat(view.getTargets().size(), is(1));
        MatcherAssert.assertThat(view.getTargets().get(0).getIndexPattern(), is(indexPattern));

        logger.info("Testing createView with existing view name");
        final Exception ex = assertThrows(ResourceNotFoundException.class, () -> createView(viewName, "new-pattern"));
        MatcherAssert.assertThat(ex.getMessage(), is("View [test-view] already exists"));
    }

    public void testGetView() throws Exception {
        final String viewName = "existing-view";

        logger.info("Testing getView with existing view");
        createView(viewName, "index-*");
        final View view = getView(viewName).getView();
        MatcherAssert.assertThat(view.getName(), is(viewName));

        logger.info("Testing getView with non-existent view");
        final Exception whenNeverExistedEx = assertThrows(ResourceNotFoundException.class, () -> getView("non-existent"));
        MatcherAssert.assertThat(whenNeverExistedEx.getMessage(), is("View [non-existent] does not exist"));
    }

    public void testDeleteView() throws Exception {
        final String viewName = "deleted-view";
        createView(viewName, "index-*");

        logger.info("Testing deleteView with existing view");
        deleteView(viewName);
        final Exception whenDeletedEx = assertThrows(ResourceNotFoundException.class, () -> getView(viewName));
        MatcherAssert.assertThat(whenDeletedEx.getMessage(), is("View [deleted-view] does not exist"));

        logger.info("Testing deleteView with non-existent view");
        final Exception whenNeverExistedEx = assertThrows(ResourceNotFoundException.class, () -> deleteView("non-existent"));
        MatcherAssert.assertThat(whenNeverExistedEx.getMessage(), is("View [non-existent] does not exist"));
    }

    public void testUpdateView() throws Exception {
        final String viewName = "updatable-view";
        final View originalView = createView(viewName, "index-old-*").getView();

        logger.info("Testing updateView with existing view");
        final View updatedView = updateView(viewName, "new description", "index-new-*").getView();

        MatcherAssert.assertThat(updatedView, not(is(originalView)));
        MatcherAssert.assertThat(updatedView.getDescription(), is("new description"));
        MatcherAssert.assertThat(updatedView.getTargets(), hasSize(1));
        MatcherAssert.assertThat(updatedView.getTargets().get(0).getIndexPattern(), is("index-new-*"));

        logger.info("Testing updateView with non-existent view");
        final Exception whenNeverExistedEx = assertThrows(
            ResourceNotFoundException.class,
            () -> updateView("non-existent", null, "index-*")
        );
        MatcherAssert.assertThat(whenNeverExistedEx.getMessage(), is("View [non-existent] does not exist"));
    }

    public void testListViewNames() throws Exception {
        final String view1 = "view1";
        final String view2 = "view2";
        createView(view1, "index-1-*");
        createView(view2, "index-2-*");

        logger.info("Testing listViewNames");
        final List<String> views = listViewNames();
        MatcherAssert.assertThat(views, containsInAnyOrder(view1, view2));

        logger.info("Testing listViewNames after deleting a view");
        deleteView(view1);
        final List<String> viewsAfterDeletion = listViewNames();
        MatcherAssert.assertThat(viewsAfterDeletion, not(contains(view1)));
        MatcherAssert.assertThat(viewsAfterDeletion, contains(view2));
    }

    public void testSearchOperations() throws Exception {
        final String indexInView1 = "index-1";
        final String indexInView2 = "index-2";
        final String indexNotInView = "another-index-1";

        final int indexInView1DocCount = createIndexWithDocs(indexInView1);
        final int indexInView2DocCount = createIndexWithDocs(indexInView2);
        createIndexWithDocs(indexNotInView);

        logger.info("Testing view with no matches");
        createView("no-matches", "this-pattern-will-match-nothing");
        final Exception ex = assertThrows(ResourceNotFoundException.class, () -> searchView("no-matches"));
        MatcherAssert.assertThat(ex.getMessage(), is("no such index [this-pattern-will-match-nothing]"));

        logger.info("Testing view with exact index match");
        createView("only-index-1", "index-1");
        assertHitCount(searchView("only-index-1"), indexInView1DocCount);

        logger.info("Testing view with wildcard matches");
        createView("both-indices", "index-*");
        assertHitCount(searchView("both-indices"), indexInView1DocCount + indexInView2DocCount);

        logger.info("Testing searchView with non-existent view");
        final Exception whenNeverExistedEx = assertThrows(ResourceNotFoundException.class, () -> searchView("non-existent"));
        MatcherAssert.assertThat(whenNeverExistedEx.getMessage(), is("View [non-existent] does not exist"));

    }
}
