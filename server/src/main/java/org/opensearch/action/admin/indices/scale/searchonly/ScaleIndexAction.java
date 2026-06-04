/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.scale.searchonly;

import org.opensearch.action.ActionType;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;

/**
 * Action type for search-only scale operations on indices.
 * <p>
 * This action type represents the administrative operation to transition indices
 * between normal operation and search-only mode. It provides the action name constant
 * used for transport actions and request routing. The action produces an
 * {@link AcknowledgedResponse} to indicate success or failure.
 * <p>
 * The action is registered with the name "indices:admin/scale/search_only" in the
 * transport action registry.
 */
public class ScaleIndexAction extends ActionType<AcknowledgedResponse> {
    /**
     * Singleton instance of the SearchOnlyScaleAction.
     */
    public static final ScaleIndexAction INSTANCE = new ScaleIndexAction();

    /**
     * The name of this action, used for transport action registration and routing.
     * <p>
     * This action name follows the OpenSearch convention of prefixing administrative
     * index actions with "indices:admin/".
     */
    public static final String NAME = "indices:admin/scale/search_only";

    /**
     * Private constructor to enforce singleton pattern.
     * <p>
     * Initializes the action with the appropriate name and response reader.
     */
    private ScaleIndexAction() {
        super(NAME, AcknowledgedResponse::new);
    }
}
