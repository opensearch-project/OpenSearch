/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity;

import org.opensearch.Application;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginInfo;

/**
 * This interface defines the expected methods of a service account manager
 */
public interface ServiceAccountManager {

    /**
     * Get service account
     */
    public ServiceAccount getServiceAccount(Application app);

    public void registerServiceAccount(PluginInfo info, Plugin plugin);
}
