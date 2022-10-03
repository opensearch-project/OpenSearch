/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.authn.identity;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.Map;

public class InternalSubject {

    @JsonProperty(value = "primary_principal")
    private String primaryPrincipal;

    private String hash;
    private Map<String, String> attributes = Collections.emptyMap();

    @JsonProperty(value = "primary_principal")
    public String getPrimaryPrincipal() {
        return primaryPrincipal;
    }

    @JsonProperty(value = "primary_principal")
    public void setPrimaryPrincipal(String primaryPrincipal) {
        this.primaryPrincipal = primaryPrincipal;
    }

    public String getHash() {
        return hash;
    }

    public void setHash(String hash) {
        this.hash = hash;
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, String> attributes) {
        this.attributes = attributes;
    }

    @Override
    public String toString() {
        return "InternalSubject [primaryPrincipal=" + primaryPrincipal + ", hash=" + hash + ", attributes=" + attributes + "]";
    }
}
