/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.auth;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.bouncycastle.crypto.generators.OpenBSDBCrypt;
import org.greenrobot.eventbus.Subscribe;

import org.opensearch.OpenSearchSecurityException;
import org.opensearch.authn.StringPrincipal;
import org.opensearch.authn.User;
import org.opensearch.identity.configuration.model.InternalUsersModel;

public class InternalAuthenticationBackend implements AuthenticationBackend, AuthorizationBackend {

    private InternalUsersModel internalUsersModel;

    @Override
    public boolean exists(User user) {

        if (user == null || internalUsersModel == null) {
            return false;
        }

        final boolean exists = internalUsersModel.exists(user.getPrimaryPrincipal().getName());

        if (exists) {
            // FIX https://github.com/opendistro-for-elasticsearch/security/pull/23
            // Credits to @turettn
            final Map<String, String> customAttributes = internalUsersModel.getAttributes(user.getPrimaryPrincipal().getName());
            Map<String, String> attributeMap = new HashMap<>();

            if (customAttributes != null) {
                for (Entry<String, String> attributeEntry : customAttributes.entrySet()) {
                    attributeMap.put("attr.internal." + attributeEntry.getKey(), attributeEntry.getValue());
                }
            }

            Map<String, String> existingAttributes = user.getAttributes();
            if (existingAttributes == null) {
                existingAttributes = new HashMap<>();
            }

            existingAttributes.putAll(attributeMap);

            user.setAttributes(existingAttributes);
            return true;
        }

        return false;
    }

    @Override
    public User authenticate(final AuthCredentials credentials) {

        if (internalUsersModel == null) {
            throw new OpenSearchSecurityException("Internal authentication backend not configured. May be OpenSearch is not initialized.");
        }

        if (!internalUsersModel.exists(credentials.getUsername())) {
            throw new OpenSearchSecurityException(credentials.getUsername() + " not found");
        }

        final byte[] password = credentials.getPassword();

        if (password == null || password.length == 0) {
            throw new OpenSearchSecurityException("empty passwords not supported");
        }

        ByteBuffer wrap = ByteBuffer.wrap(password);
        CharBuffer buf = StandardCharsets.UTF_8.decode(wrap);
        char[] array = new char[buf.limit()];
        buf.get(array);

        Arrays.fill(password, (byte) 0);

        try {
            if (OpenBSDBCrypt.checkPassword(internalUsersModel.getHash(credentials.getUsername()), array)) {
                final Map<String, String> customAttributes = internalUsersModel.getAttributes(credentials.getUsername());
                if (customAttributes != null) {
                    for (Entry<String, String> attributeName : customAttributes.entrySet()) {
                        credentials.addAttribute("attr.internal." + attributeName.getKey(), attributeName.getValue());
                    }
                }

                final User user = new User();
                user.setPrimaryPrincipal(new StringPrincipal(credentials.getUsername()));

                return user;
            } else {
                throw new OpenSearchSecurityException("password does not match");
            }
        } finally {
            Arrays.fill(wrap.array(), (byte) 0);
            Arrays.fill(buf.array(), '\0');
            Arrays.fill(array, '\0');
        }
    }

    @Override
    public String getType() {
        return "internal";
    }

    @Override
    public void fillRoles(User user, AuthCredentials credentials) throws OpenSearchSecurityException {

        if (internalUsersModel == null) {
            throw new OpenSearchSecurityException(
                "Internal authentication backend not configured. May be OpenSearch Security is not initialized."
            );

        }

    }

    @Subscribe
    public void onInternalUsersModelChanged(InternalUsersModel ium) {
        this.internalUsersModel = ium;
    }

}
