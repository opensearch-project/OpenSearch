/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tools.cli.fips.truststore;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Locale;

/**
 * Encapsulates properties related to a TrustStore configuration. Is primarily used to manage and
 * log details of TrustStore configurations within the system.
 */
public record ConfigurationProperties(String trustStorePath, String trustStoreType, String trustStorePassword, String trustStoreProvider) {

    public static final String JAVAX_NET_SSL_TRUST_STORE = "javax.net.ssl.trustStore";
    public static final String JAVAX_NET_SSL_TRUST_STORE_TYPE = "javax.net.ssl.trustStoreType";
    public static final String JAVAX_NET_SSL_TRUST_STORE_PROVIDER = "javax.net.ssl.trustStoreProvider";
    public static final String JAVAX_NET_SSL_TRUST_STORE_PASSWORD = "javax.net.ssl.trustStorePassword";

    StringWriter logout() {
        var detailLog = new StringWriter();
        var writer = new PrintWriter(detailLog);
        var passwordSetStatus = trustStorePassword.isEmpty() ? "[NOT SET]" : "[SET]";

        writer.printf(Locale.ROOT, "\n" + JAVAX_NET_SSL_TRUST_STORE + ": " + trustStorePath);
        writer.printf(Locale.ROOT, "\n" + JAVAX_NET_SSL_TRUST_STORE_TYPE + ": " + trustStoreType);
        writer.printf(Locale.ROOT, "\n" + JAVAX_NET_SSL_TRUST_STORE_PROVIDER + ": " + trustStoreProvider);
        writer.printf(Locale.ROOT, "\n" + JAVAX_NET_SSL_TRUST_STORE_PASSWORD + ": " + passwordSetStatus);
        writer.flush();

        return detailLog;
    }
}
