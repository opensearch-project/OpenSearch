/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.extensions;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Holder for extension security configuration.
 *
 * @opensearch.experimental
 */

public class ExtensionSecurityConfig {

    public ExtensionSecurityConfig() {}

    // Base64 Encoded HS512 signingKey
    @JsonProperty(value = "signingKey")
    private String signingKey;

    @JsonProperty(value = "signingKey")
    public String getSigningKey() {
        return signingKey;
    }

    @JsonProperty(value = "signingKey")
    public void setSigningKey(String signingKey) {
        this.signingKey = signingKey;
    }

    @Override
    public String toString() {
        return "ExtensionSecurity [signingKey=" + signingKey + "]";
    }
}
