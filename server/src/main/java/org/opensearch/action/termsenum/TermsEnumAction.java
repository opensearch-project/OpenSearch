/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.termsenum;

import org.opensearch.action.ActionType;
import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Lists the distinct terms of one field that start with a prefix, across the target indices.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class TermsEnumAction extends ActionType<TermsEnumResponse> {

    /** The action name. */
    public static final String NAME = "indices:data/read/terms_enum";

    /** The single instance of this action. */
    public static final TermsEnumAction INSTANCE = new TermsEnumAction();

    private TermsEnumAction() {
        super(NAME, TermsEnumResponse::new);
    }
}
