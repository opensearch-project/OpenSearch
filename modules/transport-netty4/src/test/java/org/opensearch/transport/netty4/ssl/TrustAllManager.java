/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.netty4.ssl;

import javax.net.ssl.X509TrustManager;

import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

public class TrustAllManager implements X509TrustManager {
    public static final X509TrustManager INSTANCE = new TrustAllManager();

    private TrustAllManager() {}

    public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {}

    public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {}

    public X509Certificate[] getAcceptedIssuers() {
        return new X509Certificate[0];
    }
}
