/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.querygroup;

import org.opensearch.action.search.SearchAction;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.tasks.Task;

import java.util.Collections;

import static org.opensearch.test.OpenSearchTestCase.randomLong;

public class QueryGroupTestHelpers {

    public static Task getRandomTask(long id) {
        return new Task(
            id,
            "transport",
            SearchAction.NAME,
            "test description",
            new TaskId(randomLong() + ":" + randomLong()),
            Collections.emptyMap()
        );
    }
}
