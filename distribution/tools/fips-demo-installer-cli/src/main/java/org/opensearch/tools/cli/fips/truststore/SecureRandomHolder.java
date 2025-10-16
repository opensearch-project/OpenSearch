/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tools.cli.fips.truststore;

import org.bouncycastle.crypto.fips.FipsDRBG;
import org.bouncycastle.crypto.util.BasicEntropySourceProvider;

import java.security.GeneralSecurityException;
import java.security.SecureRandom;

public class SecureRandomHolder {

    static final SecureRandom RANDOM = getSecureRandom();

    static SecureRandom getSecureRandom() {
        SecureRandom secureRandom;
        boolean isPredictionResistant = true;
        try {
            var entropySource = SecureRandom.getInstance("DEFAULT", "BCFIPS");
            var entropyProvider = new BasicEntropySourceProvider(entropySource, isPredictionResistant);
            var builder = FipsDRBG.SHA512_HMAC.fromEntropySource(entropyProvider);
            secureRandom = builder.build(null, isPredictionResistant);
        } catch (GeneralSecurityException e) {
            throw new RuntimeException(e);
        }
        return secureRandom;
    }

}
