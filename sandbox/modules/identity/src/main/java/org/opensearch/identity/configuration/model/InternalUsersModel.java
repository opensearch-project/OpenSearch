/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.configuration.model;

import org.opensearch.identity.User;
import org.opensearch.identity.configuration.SecurityDynamicConfiguration;

import java.util.Map;

public class InternalUsersModel {

    private final SecurityDynamicConfiguration<User> internalUserSecurityDC;

    public InternalUsersModel(SecurityDynamicConfiguration<User> internalUserSecurityDC) {
        super();
        this.internalUserSecurityDC = internalUserSecurityDC;
    }

    public User getUser(String username) {
        return internalUserSecurityDC.getCEntry(username);
    }

    public boolean exists(String username) {
        return internalUserSecurityDC.exists(username);
    }

    public Map<String, String> getAttributes(String username) {
        User tmp = internalUserSecurityDC.getCEntry(username);
        return tmp == null ? null : tmp.getAttributes();
    }

    public String getHash(String username) {
        User tmp = internalUserSecurityDC.getCEntry(username);
        return tmp == null ? null : tmp.getHash();
    }
}
