/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.shiro;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.List;
import org.opensearch.identity.User;
import org.opensearch.identity.UserProvider;

public class ShiroUserProvider implements UserProvider {
    @Override
    public User getUser(String username) {
        return null;
    }

    @Override
    public void removeUser(String username) {

    }

    @Override
    public void putUser(ObjectNode userContent) {

    }

    @Override
    public List<User> getUsers() {
        return null;
    }
}
