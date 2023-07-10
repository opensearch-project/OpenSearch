/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.noop;

import org.opensearch.Application;
import org.opensearch.identity.ServiceAccount;
import org.opensearch.identity.ServiceAccountManager;

/**
 * This class is a NoopServiceAccountManager used by a Noop version of the Identity Plugin.
 */
public class NoopServiceAccountManager extends ServiceAccountManager {

    @Override
    public ServiceAccount getServiceAccount(Application app) {
        return null;
    }
}
