/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity;

import java.util.HashMap;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.Application;

/**
 * This interface defines the expected methods of a service account manager
 */
public class ServiceAccountManager {

    private static final Logger log = LogManager.getLogger(IdentityService.class);

    private static Map<Application, ServiceAccount> applicationServiceAccountMap = new HashMap<>();

    public ServiceAccount getServiceAccount(Application app) {
        if (applicationServiceAccountMap.get(app) == null) {
            applicationServiceAccountMap.put(app, new ServiceAccount(app));
        }
        return applicationServiceAccountMap.get(app);
    }

    public Map<Application, ServiceAccount> getApplicationServiceAccountMap() {
        return applicationServiceAccountMap;
    }
}
