/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.view;

import org.opensearch.common.annotation.ExperimentalApi;

/** Action to list a view names */
@ExperimentalApi
public class ListViewNamesAction {

    public static final ListViewNamesAction INSTANCE = new ListViewNamesAction();
    public static final String NAME = "views:data/read/list";

}
