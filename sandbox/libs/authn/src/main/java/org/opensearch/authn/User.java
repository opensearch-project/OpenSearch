/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.authn;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.security.Principal;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class User extends Subject {

    @JsonProperty(value = "primary_principal")
    private StringPrincipal primaryPrincipal;

    @JsonProperty(value = "hash")
    private String bcryptHash;
    private Map<String, String> attributes = Collections.emptyMap();

    @JsonProperty(value = "primary_principal")
    public StringPrincipal getPrimaryPrincipal() {
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

    public Map<String, String> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, String> attributes) {
        this.attributes = attributes;
    }

    @Override
    public String toString() {
        return "InternalSubject [primaryPrincipal=" + primaryPrincipal + ", bcryptHash=" + bcryptHash + ", attributes=" + attributes + "]";
    }

    @Override
    public Principal getPrincipal() {
        return primaryPrincipal;
    }

    @Override
    public void login(AuthenticationToken token) {
        // TODO implement this
    }

    @Override
    public void updateSubjectAttributes(Map<String, String> attributes) {
       this.attributes.putAll(attributes);
    }

    @Override
    public void removeSubjectAttributes(List<String> attributesToBeRemoved) {
        for (String attribute : attributesToBeRemoved) {
            this.attributes.remove(attribute);
        }
    }
}
