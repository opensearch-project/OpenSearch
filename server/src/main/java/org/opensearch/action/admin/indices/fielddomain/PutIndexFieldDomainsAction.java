/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.fielddomain;

import org.opensearch.action.ActionType;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Action type for publishing index-level field-domain metadata.
 *
 * This action is intended for trusted metadata producers. The cluster-manager node validates and merges the supplied
 * field-domain metadata into the target concrete index metadata.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class PutIndexFieldDomainsAction extends ActionType<AcknowledgedResponse> {
    /**
     * Singleton action instance.
     */
    public static final PutIndexFieldDomainsAction INSTANCE = new PutIndexFieldDomainsAction();

    /**
     * Transport action name.
     */
    public static final String NAME = "indices:admin/field_domains/put";

    private PutIndexFieldDomainsAction() {
        super(NAME, AcknowledgedResponse::new);
    }
}
