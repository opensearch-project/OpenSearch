/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.bouncycastle.jce.provider;

import java.security.Provider;

/**
 * Mock provider for Netty's PKI testing facility, should never be used
 */
public final class BouncyCastleProvider extends Provider {
    private static final long serialVersionUID = 1L;

    public BouncyCastleProvider() {
        super("N/A", "N/A", "N/A");
    }
}
