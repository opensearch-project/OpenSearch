/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.shiro;

import org.apache.shiro.subject.Subject;
import org.apache.shiro.mgt.DefaultSecurityManager;
import org.apache.shiro.mgt.SubjectDAO;
import org.opensearch.identity.shiro.realm.InternalRealm;

/**
 * OpenSearch specific security manager implementation
 *
 * @opensearch.experimental
 */
public class ShiroSecurityManager extends DefaultSecurityManager {

    /**
     * Creates the security manager using a default realm and no session storage
     */
    public ShiroSecurityManager() {
        super(InternalRealm.INSTANCE);

        // By default shiro stores session information into a cache, there were performance
        // issues with this sessions cache and so are defaulting to a stateless configuration
        this.subjectDAO = new StatelessDAO();
    }

    /**
     *
     * @opensearch.experimental
     */
    private static class StatelessDAO implements SubjectDAO {
        public Subject save(final Subject s) {
            return s;
        }

        public void delete(final Subject s) {}
    }
}
