/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.common.ssl;

import org.bouncycastle.crypto.CryptoServicesRegistrar;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Assert;

import javax.net.ssl.X509ExtendedTrustManager;

import java.security.Provider;
import java.security.Security;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.not;

public class DefaultJdkTrustConfigTests extends OpenSearchTestCase {

    private static final BiFunction<String, String, String> EMPTY_SYSTEM_PROPERTIES = (key, defaultValue) -> defaultValue;
    private static final BiFunction<String, String, String> PKCS11_SYSTEM_PROPERTIES = (key, defaultValue) -> {
        if ("javax.net.ssl.trustStoreType".equals(key)) {
            return "PKCS11";
        }
        return defaultValue;
    };
    private static final BiFunction<String, String, String> BCFKS_SYSTEM_PROPERTIES = (key, defaultValue) -> {
        if ("javax.net.ssl.trustStoreType".equals(key)) {
            return "BCFKS";
        }
        return defaultValue;
    };
    private static final BiFunction<String, String, String> FIPS_AWARE_SYSTEM_PROPERTIES = CryptoServicesRegistrar.isInApprovedOnlyMode()
        ? BCFKS_SYSTEM_PROPERTIES
        : EMPTY_SYSTEM_PROPERTIES;

    public void testGetSystemPKCS11TrustStoreWithSystemProperties() throws Exception {
        assumeFalse("Should only run when PKCS11 provider is installed.", findPKCS11ProviderService().isEmpty());
        final DefaultJdkTrustConfig trustConfig = new DefaultJdkTrustConfig(PKCS11_SYSTEM_PROPERTIES);
        assertThat(trustConfig.getDependentFiles(), emptyIterable());
        final X509ExtendedTrustManager trustManager = trustConfig.createTrustManager();
        assertStandardIssuers(trustManager);
    }

    public void testGetSystemTrustStoreWithNoSystemProperties() throws Exception {
        final DefaultJdkTrustConfig trustConfig = new DefaultJdkTrustConfig(FIPS_AWARE_SYSTEM_PROPERTIES);
        assertThat(trustConfig.getDependentFiles(), emptyIterable());
        final X509ExtendedTrustManager trustManager = trustConfig.createTrustManager();
        assertStandardIssuers(trustManager);
    }

    public void testGetNonPKCS11TrustStoreWithPasswordSet() throws Exception {
        final DefaultJdkTrustConfig trustConfig = new DefaultJdkTrustConfig(FIPS_AWARE_SYSTEM_PROPERTIES, "fakepassword".toCharArray());
        assertThat(trustConfig.getDependentFiles(), emptyIterable());
        final X509ExtendedTrustManager trustManager = trustConfig.createTrustManager();
        assertStandardIssuers(trustManager);
    }

    private void assertStandardIssuers(X509ExtendedTrustManager trustManager) {
        assertThat(trustManager.getAcceptedIssuers(), not(emptyArray()));
        // This is a sample of the CAs that we expect on every JRE.
        // We can safely change this list if the JRE's issuer list changes, but we want to assert something useful.
        // - https://bugs.openjdk.java.net/browse/JDK-8215012: VeriSign, GeoTrust" and "thawte" are gone
        assertHasTrustedIssuer(trustManager, "DigiCert");
        assertHasTrustedIssuer(trustManager, "COMODO");
    }

    private void assertHasTrustedIssuer(X509ExtendedTrustManager trustManager, String name) {
        final String lowerName = name.toLowerCase(Locale.ROOT);
        final Optional<X509Certificate> ca = Stream.of(trustManager.getAcceptedIssuers())
            .filter(cert -> cert.getSubjectX500Principal().getName().toLowerCase(Locale.ROOT).contains(lowerName))
            .findAny();
        if (ca.isPresent() == false) {
            logger.info("Failed to find issuer [{}] in trust manager, but did find ...", lowerName);
            for (X509Certificate cert : trustManager.getAcceptedIssuers()) {
                logger.info(" - {}", cert.getSubjectX500Principal().getName().replaceFirst("^\\w+=([^,]+),.*", "$1"));
            }
            Assert.fail("Cannot find trusted issuer with name [" + name + "].");
        }
    }

    private List<Provider.Service> findPKCS11ProviderService() {
        return Arrays.stream(Security.getProviders())
            .filter(it -> it.getName().toUpperCase(Locale.ROOT).contains("PKCS11"))
            .map(it -> it.getService("KeyStore", "PKCS11"))
            .filter(Objects::nonNull)
            .toList();
    }

}
