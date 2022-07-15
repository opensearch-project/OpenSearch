/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.cat;

/**
 * _cat API action to list cluster_manager information
 *
 * @opensearch.api
 * @deprecated As of 2.2, because supporting inclusive language, replaced by {@link RestClusterManagerAction}
 */
@Deprecated
public class RestMasterAction extends RestClusterManagerAction {

}
