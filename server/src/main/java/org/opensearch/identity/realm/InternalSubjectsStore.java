/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.realm;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.opensearch.identity.InternalSubject;
import org.opensearch.identity.config.DefaultObjectMapper;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class InternalSubjectsStore {

    public static Map<String, InternalSubject> readInternalSubjectsAsMap(String pathToInternalUsersYaml) throws FileNotFoundException {
        Map<String, InternalSubject> internalSubjectsMap = new HashMap<>();
        URL resourceUrl = InternalSubjectsStore.class.getClassLoader().getResource(pathToInternalUsersYaml);
        if (resourceUrl == null) {
            throw new FileNotFoundException(pathToInternalUsersYaml + " not found");
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
