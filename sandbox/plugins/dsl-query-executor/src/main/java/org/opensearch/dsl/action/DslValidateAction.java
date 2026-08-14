/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.action;

import org.opensearch.action.ActionType;
import org.opensearch.action.admin.indices.validate.query.ValidateQueryResponse;

/**
 * Action type for validating a query against the DSL-to-Calcite converter.
 * Dispatched by {@link SearchActionFilter} when it intercepts a {@code _validate/query} request.
 */
public class DslValidateAction extends ActionType<ValidateQueryResponse> {

    /** Singleton instance. */
    public static final DslValidateAction INSTANCE = new DslValidateAction();

    /** Action name. */
    public static final String NAME = "indices:data/read/dsl_validate";

    private DslValidateAction() {
        super(NAME, ValidateQueryResponse::new);
    }
}
