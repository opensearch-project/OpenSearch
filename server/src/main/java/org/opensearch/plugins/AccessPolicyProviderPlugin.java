/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.action.support.ReadAccessPolicyProvider;
import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Plugin extension point for supplying effective document-level read policies. This is usually only implemented by the
 * security plugin. There can be max ONE plugin in a node implementing this interface.
 */
@ExperimentalApi
public interface AccessPolicyProviderPlugin {

    /** Returns this plugin's read-access policy provider. */
    ReadAccessPolicyProvider getReadAccessPolicyProvider();
}
