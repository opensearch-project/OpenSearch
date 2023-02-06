/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.extensions;

import org.apache.commons.lang.RandomStringUtils;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

public class ExtensionsSecretsGenerator {
    public static String generateSigningKey() {
        final String signingKey = RandomStringUtils.randomAlphanumeric(256);
        String signingKeyB64 = Base64.getEncoder().encodeToString(signingKey.getBytes(StandardCharsets.UTF_8));
        return signingKeyB64;
    }
}
