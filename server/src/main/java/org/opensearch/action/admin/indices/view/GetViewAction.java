/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.view;

import org.opensearch.common.annotation.ExperimentalApi;

/** Action to get a view */
@ExperimentalApi
public class GetViewAction {

    public static final GetViewAction INSTANCE = new GetViewAction();
    public static final String NAME = "views:data/read/get";

}
