/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.view;

import org.opensearch.cluster.metadata.View;
import org.opensearch.index.IndexNotFoundException;
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
        final String viewName = randomAlphaOfLength(8);
        final String indexPattern = randomAlphaOfLength(8);

        logger.info("Testing createView with valid parameters");
        final View view = createView(viewName, indexPattern).getView();
        MatcherAssert.assertThat(view.getName(), is(viewName));
        MatcherAssert.assertThat(view.getTargets().size(), is(1));
        MatcherAssert.assertThat(view.getTargets().first().getIndexPattern(), is(indexPattern));

        logger.info("Testing createView with existing view name");
        final Exception ex = assertThrows(ViewAlreadyExistsException.class, () -> createView(viewName, randomAlphaOfLength(8)));
        MatcherAssert.assertThat(ex.getMessage(), is("View [" + viewName + "] already exists"));
    }

    public void testCreateViewTargetsSet() throws Exception {
        final String viewName = randomAlphaOfLength(8);
        final String indexPattern = "a" + randomAlphaOfLength(8);
        final String indexPattern2 = "b" + randomAlphaOfLength(8);
        final List<String> targetPatterns = List.of(indexPattern2, indexPattern, indexPattern);

        logger.info("Testing createView with targets that will be reordered and deduplicated");
        final View view = createView(viewName, targetPatterns).getView();
        MatcherAssert.assertThat(view.getName(), is(viewName));
        MatcherAssert.assertThat(view.getTargets().size(), is(2));
        MatcherAssert.assertThat(view.getTargets().first().getIndexPattern(), is(indexPattern));
        MatcherAssert.assertThat(view.getTargets().last().getIndexPattern(), is(indexPattern2));
    }

    public void testGetView() throws Exception {
        final String viewName = randomAlphaOfLength(8);
        createView(viewName, randomAlphaOfLength(8));

        final View view = getView(viewName).getView();
        MatcherAssert.assertThat(view.getName(), is(viewName));

        logger.info("Testing getView with non-existent view");
        final String nonExistentView = "non-existent-" + randomAlphaOfLength(8);
        final Exception whenNeverExistedEx = assertThrows(ViewNotFoundException.class, () -> getView(nonExistentView));
        MatcherAssert.assertThat(whenNeverExistedEx.getMessage(), is("View [" + nonExistentView + "] does not exist"));
    }

    public void testDeleteView() throws Exception {
        final String viewName = randomAlphaOfLength(8);
        createView(viewName, randomAlphaOfLength(8));

        logger.info("Testing deleteView with existing view");
        deleteView(viewName);
        final Exception whenDeletedEx = assertThrows(ViewNotFoundException.class, () -> getView(viewName));
        MatcherAssert.assertThat(whenDeletedEx.getMessage(), is("View [" + viewName + "] does not exist"));

        logger.info("Testing deleteView with non-existent view");
        final String nonExistentView = "non-existent-" + randomAlphaOfLength(8);
        final Exception whenNeverExistedEx = assertThrows(ViewNotFoundException.class, () -> deleteView(nonExistentView));
        MatcherAssert.assertThat(whenNeverExistedEx.getMessage(), is("View [" + nonExistentView + "] does not exist"));
    }

    public void testUpdateView() throws Exception {
        final String viewName = randomAlphaOfLength(8);
        final String originalIndexPattern = randomAlphaOfLength(8);
        final View originalView = createView(viewName, originalIndexPattern).getView();

        logger.info("Testing updateView with existing view");
        final String newDescription = randomAlphaOfLength(20);
        final String newIndexPattern = "newPattern-" + originalIndexPattern;
        final View updatedView = updateView(viewName, newDescription, newIndexPattern).getView();

        MatcherAssert.assertThat(updatedView, not(is(originalView)));
        MatcherAssert.assertThat(updatedView.getDescription(), is(newDescription));
        MatcherAssert.assertThat(updatedView.getTargets(), hasSize(1));
        MatcherAssert.assertThat(updatedView.getTargets().first().getIndexPattern(), is(newIndexPattern));

        logger.info("Testing updateView with non-existent view");
        final String nonExistentView = "non-existent-" + randomAlphaOfLength(8);
        final Exception whenNeverExistedEx = assertThrows(ViewNotFoundException.class, () -> updateView(nonExistentView, null, "index-*"));
        MatcherAssert.assertThat(whenNeverExistedEx.getMessage(), is("View [" + nonExistentView + "] does not exist"));
    }

    public void testListViewNames() throws Exception {
        logger.info("Testing listViewNames when no views have been created");
        MatcherAssert.assertThat(listViewNames(), is(List.of()));

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
        final Exception ex = assertThrows(IndexNotFoundException.class, () -> searchView("no-matches"));
        MatcherAssert.assertThat(ex.getMessage(), is("no such index [this-pattern-will-match-nothing]"));

        logger.info("Testing view with exact index match");
        createView("only-index-1", "index-1");
        assertHitCount(searchView("only-index-1"), indexInView1DocCount);

        logger.info("Testing view with wildcard matches");
        createView("both-indices", "index-*");
        assertHitCount(searchView("both-indices"), indexInView1DocCount + indexInView2DocCount);

        logger.info("Testing searchView with non-existent view");
        final String nonExistentView = "non-existent-" + randomAlphaOfLength(8);
        final Exception whenNeverExistedEx = assertThrows(ViewNotFoundException.class, () -> searchView(nonExistentView));
        MatcherAssert.assertThat(whenNeverExistedEx.getMessage(), is("View [" + nonExistentView + "] does not exist"));
    }
}
