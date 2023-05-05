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

/**
 * A shiro implementation of the UserProvider interface
 */
public class ShiroUserProvider implements UserProvider {

    /**
     * Returns a new User object wih the associated username
     * @param username The username of the User
     * @return A user to be returned
     */
    @Override
    public User getUser(String username) {
        return null;
    }

    /**
     * Removes a user object wih the associated username
     * @param username The username of the User
     */
    @Override
    public void removeUser(String username) {

    }

    /**
     * Adds a new User object wih the associated userContent to the User store
     * @param userContent The information for the User to be added
     */
    @Override
    public void putUser(ObjectNode userContent) {

    }

    /**
     * Returns a list of all User objects in the User store
     * @return A list of Users in the  User store
     */
    @Override
    public List<User> getUsers() {
        return null;
    }
}
