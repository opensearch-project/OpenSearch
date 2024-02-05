/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.view;

import org.opensearch.common.annotation.ExperimentalApi;

/** Action to delete a view */
@ExperimentalApi
public class DeleteViewAction {
    
    public static final DeleteViewAction INSTANCE = new DeleteViewAction();
    public static final String NAME = "cluster:admin/views/delete";

}
