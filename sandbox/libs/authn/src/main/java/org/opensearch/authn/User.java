/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.authn;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.Map;

public class User implements Subject {

    @JsonProperty(value = "primary_principal")
    private StringPrincipal primaryPrincipal;

    @JsonProperty(value = "hash")
    private String bcryptHash;

    @JsonProperty(value = "attributes")
    private Map<String, String> attributes = Collections.emptyMap();

    @Override
    @JsonProperty(value = "primary_principal")
    public StringPrincipal getPrincipal() {
        return primaryPrincipal;
    }

    @JsonProperty(value = "primary_principal")
    public void setPrimaryPrincipal(StringPrincipal primaryPrincipal) {
        this.primaryPrincipal = primaryPrincipal;
    }

    @JsonProperty(value = "hash")
    public String getBcryptHash() {
        return bcryptHash;
    }

    @JsonProperty(value = "hash")
    public void setBcryptHash(String bcryptHash) {
        this.bcryptHash = bcryptHash;
    }

    @JsonProperty(value = "attributes")
    public Map<String, String> getAttributes() {
        return attributes;
    }

    @JsonProperty(value = "attributes")
    public void setAttributes(Map<String, String> attributes) {
        this.attributes = attributes;
    }

    @Override
    public String toString() {
        return "User [primaryPrincipal=" + primaryPrincipal + ", bcryptHash=" + bcryptHash + ", attributes=" + attributes + "]";
    }
}
