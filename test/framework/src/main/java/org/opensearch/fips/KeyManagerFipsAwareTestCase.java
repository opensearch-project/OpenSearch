/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.fips;

import javax.net.ssl.KeyManagerFactory;

public interface KeyManagerFipsAwareTestCase {

    default KeyManagerFactory createKeyManagerFactory() {
        var cfg = FipsConfig.detect();
        return createKeyManagerFactory(cfg.keyStoreType(), cfg.fileExtension(), cfg.jcaProvider(), cfg.jsseProvider());
    }

    KeyManagerFactory createKeyManagerFactory(String keyStoreType, String fileExtension, String jcaProvider, String jsseProvider);

}
