/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.authn.realm;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.opensearch.authn.DefaultObjectMapper;
import org.opensearch.authn.InternalSubject;

import java.io.IOException;
import java.net.URL;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @opensearch.experimental
 */
public class InternalSubjectsStore {

    public static ConcurrentMap<String, InternalSubject> readInternalSubjectsAsMap(String pathToInternalUsersYaml) {
        ConcurrentMap<String, InternalSubject> internalSubjectsMap = new ConcurrentHashMap<>();
        URL resourceUrl = InternalSubjectsStore.class.getClassLoader().getResource(pathToInternalUsersYaml);
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
                InternalSubject theSubject = DefaultObjectMapper.readValue(subjectNodeString, InternalSubject.class);
                internalSubjectsMap.put(primaryPrincipal, theSubject);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return internalSubjectsMap;
    }
}
