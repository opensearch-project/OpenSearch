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
                User user = DefaultObjectMapper.readValue(subjectNodeString, User.class);
                internalUsersMap.put(primaryPrincipal, user);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return internalUsersMap;
    }
}
