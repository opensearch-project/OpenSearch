/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.shiro;

import java.util.HashMap;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.Application;
import org.opensearch.identity.IdentityService;
import org.opensearch.identity.ServiceAccount;
import org.opensearch.identity.ServiceAccountManager;

/**
 * Oversees the assignment of ServiceAccounts when using the ShiroIdentityPlugin
 *
 * @opensearch.experimental
 */
class ShiroServiceAccountManager implements ServiceAccountManager {

    private static final Logger log = LogManager.getLogger(IdentityService.class);

    private static Map<ShiroSubject, ServiceAccount> subjectServiceAccountMap = new HashMap<>();

    @Override
    public ServiceAccount getServiceAccount(Application app) {
        return null;
    }
}
