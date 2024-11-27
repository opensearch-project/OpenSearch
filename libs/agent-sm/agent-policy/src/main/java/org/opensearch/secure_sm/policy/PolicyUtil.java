/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.secure_sm.policy;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.util.Arrays;

public class PolicyUtil {

    // standard PKCS11 KeyStore type
    private static final String P11KEYSTORE = "PKCS11";

    // reserved word
    private static final String NONE = "NONE";

    /*
     * Fast path reading from file urls in order to avoid calling
     * FileURLConnection.connect() which can be quite slow the first time
     * it is called. We really should clean up FileURLConnection so that
     * this is not a problem but in the meantime this fix helps reduce
     * start up time noticeably for the new launcher. -- DAC
     */
    public static InputStream getInputStream(URL url) throws IOException {
        if ("file".equals(url.getProtocol())) {
            String path = url.getFile().replace('/', File.separatorChar);
            path = ParseUtil.decode(path);
            return new FileInputStream(path);
        } else {
            return url.openStream();
        }
    }

    /**
     * this is intended for use by the policy parser to
     * instantiate a KeyStore from the information in the GUI/policy file
     */
    public static KeyStore getKeyStore(
        URL policyUrl,                 // URL of policy file
        String keyStoreName,            // input: keyStore URL
        String keyStoreType,            // input: keyStore type
        String keyStoreProvider,        // input: keyStore provider
        String storePassURL            // input: keyStore password
    ) throws KeyStoreException, IOException, NoSuchProviderException, NoSuchAlgorithmException, java.security.cert.CertificateException {

        if (keyStoreName == null) {
            throw new IllegalArgumentException("null KeyStore name");
        }

        char[] keyStorePassword = null;
        try {
            KeyStore ks;
            if (keyStoreType == null) {
                keyStoreType = KeyStore.getDefaultType();
            }

            if (P11KEYSTORE.equalsIgnoreCase(keyStoreType) && !NONE.equals(keyStoreName)) {
                throw new IllegalArgumentException(
                    "Invalid value ("
                        + keyStoreName
                        + ") for keystore URL.  If the keystore type is \""
                        + P11KEYSTORE
                        + "\", the keystore url must be \""
                        + NONE
                        + "\""
                );
            }

            if (keyStoreProvider != null) {
                ks = KeyStore.getInstance(keyStoreType, keyStoreProvider);
            } else {
                ks = KeyStore.getInstance(keyStoreType);
            }

            if (storePassURL != null) {
                URL passURL;
                try {
                    @SuppressWarnings("deprecation")
                    var _unused = passURL = new URL(storePassURL);
                    // absolute URL
                } catch (MalformedURLException e) {
                    // relative URL
                    if (policyUrl == null) {
                        throw e;
                    }
                    @SuppressWarnings("deprecation")
                    var _unused = passURL = new URL(policyUrl, storePassURL);
                }

                try (InputStream in = passURL.openStream()) {
                    keyStorePassword = Password.readPassword(in);
                }
            }

            if (NONE.equals(keyStoreName)) {
                ks.load(null, keyStorePassword);
            } else {
                /*
                 * location of keystore is specified as absolute URL in policy
                 * file, or is relative to URL of policy file
                 */
                URL keyStoreUrl;
                try {
                    @SuppressWarnings("deprecation")
                    var _unused = keyStoreUrl = new URL(keyStoreName);
                    // absolute URL
                } catch (MalformedURLException e) {
                    // relative URL
                    if (policyUrl == null) {
                        throw e;
                    }
                    @SuppressWarnings("deprecation")
                    var _unused = keyStoreUrl = new URL(policyUrl, keyStoreName);
                }

                try (InputStream inStream = new BufferedInputStream(getInputStream(keyStoreUrl))) {
                    ks.load(inStream, keyStorePassword);
                }
            }
            return ks;
        } finally {
            if (keyStorePassword != null) {
                Arrays.fill(keyStorePassword, ' ');
            }
        }
    }
}
