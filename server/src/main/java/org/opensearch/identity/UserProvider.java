/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.List;

public interface UserProvider {

    public User getUser(String username);

    public void removeUser(String username);

    public void putUser(ObjectNode userContent);

    public List<User> getUsers();
}
