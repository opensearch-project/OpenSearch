/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.authn.realm;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.opensearch.authn.DefaultObjectMapper;
import org.opensearch.authn.User;

import java.io.IOException;
import java.net.URL;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @opensearch.experimental
 */
public class InternalUsersStore {

    public static ConcurrentMap<String, User> readInternalSubjectsAsMap(String pathToInternalUsersYaml) {
        ConcurrentMap<String, User> internalUsersMap = new ConcurrentHashMap<>();
        URL resourceUrl = InternalUsersStore.class.getClassLoader().getResource(pathToInternalUsersYaml);
        if (resourceUrl == null) {
            throw new RuntimeException(pathToInternalUsersYaml + " not found");
        }

        try {
            JsonNode yamlAsNode = DefaultObjectMapper.YAML_MAPPER.readTree(resourceUrl);
            Iterator<String> subjectIterator = yamlAsNode.fieldNames();
            while (subjectIterator.hasNext()) {
                String primaryPrincipal = subjectIterator.next();
                JsonNode subjectNode = yamlAsNode.get(primaryPrincipal);
                ObjectNode o = (ObjectNode) subjectNode;
                o.put("primary_principal", primaryPrincipal);
                String subjectNodeString = DefaultObjectMapper.writeValueAsString((JsonNode) o, false);

                /**
                 * Reflects access permissions to prevent jackson databind from throwing InvalidDefinitionException
                 * Counter-part is added in security.policy to grant jackson-databind ReflectPermission
                 *
                 * {@code
                 * com.fasterxml.jackson.databind.exc.InvalidDefinitionException: Cannot access public org.opensearch.authn.User()
                 * (from class org.opensearch.authn.User; failed to set access: access denied
                 * ("java.lang.reflect.ReflectPermission" "suppressAccessChecks")
                 * }
                 *
                 * TODO: Check if there is a better way around this
                 */
                User user = AccessController.doPrivileged((PrivilegedAction<User>) () -> {
                    try {
                        return DefaultObjectMapper.readValue(subjectNodeString, User.class);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });

                internalUsersMap.put(primaryPrincipal, user);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return internalUsersMap;
    }
}
