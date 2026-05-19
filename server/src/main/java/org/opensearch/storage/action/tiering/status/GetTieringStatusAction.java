/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.action.tiering.status;

import org.opensearch.action.ActionType;
import org.opensearch.storage.action.tiering.status.model.GetTieringStatusResponse;

/** Action type for getting tiering status of a single index. */
public class GetTieringStatusAction extends ActionType<GetTieringStatusResponse> {

    /** Singleton instance. */
    public static final GetTieringStatusAction INSTANCE = new GetTieringStatusAction();
    /** Action name. */
    public static final String NAME = "indices:admin/_tier/get";

    /** Constructs a new GetTieringStatusAction. */
    public GetTieringStatusAction() {
        super(NAME, GetTieringStatusResponse::new);
    }
}
