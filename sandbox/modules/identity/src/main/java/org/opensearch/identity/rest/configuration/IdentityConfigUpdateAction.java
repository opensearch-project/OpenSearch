/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.rest.configuration;

import org.opensearch.action.ActionType;

/**
 * Propagates any changes in `identity` index
 */
public class IdentityConfigUpdateAction extends ActionType<IdentityConfigUpdateResponse> {

    public static final IdentityConfigUpdateAction INSTANCE = new IdentityConfigUpdateAction();
    public static final String NAME = "cluster:admin/identity/config/update";

    protected IdentityConfigUpdateAction() {
        super(NAME, IdentityConfigUpdateResponse::new);
    }
}
