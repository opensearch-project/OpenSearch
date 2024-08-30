/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.client;

import org.apache.hc.core5.concurrent.DefaultThreadFactory;
import org.bouncycastle.crypto.CryptoServicesRegistrar;

import java.util.concurrent.ThreadFactory;

public class FipsEnabledThreadFactory implements ThreadFactory {

    private final ThreadFactory defaultFactory;
    private final boolean isFipsEnabled;

    public FipsEnabledThreadFactory(String namePrefix, boolean isFipsEnabled) {
        this.defaultFactory = new DefaultThreadFactory(namePrefix);
        this.isFipsEnabled = isFipsEnabled;
    }

    @Override
    public Thread newThread(final Runnable target) {
        return defaultFactory.newThread(() -> {
            if (isFipsEnabled) {
                CryptoServicesRegistrar.setApprovedOnlyMode(true);
            }
            target.run();
        });
    }

}
