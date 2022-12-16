/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.auth;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.opensearch.OpenSearchSecurityException;

/**
 * AuthCredentials are an abstraction to encapsulate credentials like passwords or generic
 * native credentials like GSS tokens.
 *
 */
public final class AuthCredentials {

    private static final String DIGEST_ALGORITHM = "SHA-256";
    private final String username;
    private byte[] password;
    private Object nativeCredentials;
    private boolean complete;
    private final byte[] internalPasswordHash;
    private final Map<String, String> attributes = new HashMap<>();

    /**
     * Create new credentials with a username and native credentials
     *
     * @param username The username, must not be null or empty
     * @param nativeCredentials Arbitrary credentials (like GSS tokens), must not be null
     * @throws IllegalArgumentException if username or nativeCredentials are null or empty
     */
    public AuthCredentials(final String username, final Object nativeCredentials) {
        this(username, null, nativeCredentials);

        if (nativeCredentials == null) {
            throw new IllegalArgumentException("nativeCredentials must not be null or empty");
        }
    }

    /**
     * Create new credentials with a username and password
     *
     * @param username The username, must not be null or empty
     * @param password The password, must not be null or empty
     * @throws IllegalArgumentException if username or password is null or empty
     */
    public AuthCredentials(final String username, final byte[] password) {
        this(username, password, null);

        if (password == null || password.length == 0) {
            throw new IllegalArgumentException("password must not be null or empty");
        }
    }

    /**
     * Create new credentials with a username, a initial optional set of roles and empty password/native credentials

     * @param username The username, must not be null or empty
     * @param backendRoles set of roles this user is a member of
     * @throws IllegalArgumentException if username is null or empty
     */
    public AuthCredentials(final String username, String... backendRoles) {
        this(username, null, null, backendRoles);
    }

    private AuthCredentials(final String username, byte[] password, Object nativeCredentials, String... backendRoles) {
        super();

        if (username == null || username.isEmpty()) {
            throw new IllegalArgumentException("username must not be null or empty");
        }

        this.username = username;
        // make defensive copy
        this.password = password == null ? null : Arrays.copyOf(password, password.length);

        if(this.password != null) {
            try {
                MessageDigest digester = MessageDigest.getInstance(DIGEST_ALGORITHM);
                internalPasswordHash = digester.digest(this.password);
            } catch (NoSuchAlgorithmException e) {
                throw new OpenSearchSecurityException("Unable to digest password", e);
            }
        } else {
            internalPasswordHash = null;
        }

        if(password != null) {
            Arrays.fill(password, (byte) '\0');
            password = null;
        }

        this.nativeCredentials = nativeCredentials;
        nativeCredentials = null;
    }

    /**
     * Wipe password and native credentials
     */
    public void clearSecrets() {
        if (password != null) {
            Arrays.fill(password, (byte) '\0');
            password = null;
        }

        nativeCredentials = null;
    }

    public String getUsername() {
        return username;
    }

    /**
     *
     * @return Defensive copy of the password
     */
    public byte[] getPassword() {
        // make defensive copy
        return password == null ? null : Arrays.copyOf(password, password.length);
    }

    public Object getNativeCredentials() {
        return nativeCredentials;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(internalPasswordHash);
        result = prime * result + ((username == null) ? 0 : username.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        AuthCredentials other = (AuthCredentials) obj;
        if (internalPasswordHash == null || other.internalPasswordHash == null || !MessageDigest.isEqual(internalPasswordHash, other.internalPasswordHash))
            return false;
        if (username == null) {
            if (other.username != null)
                return false;
        } else if (!username.equals(other.username))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "AuthCredentials [username=" + username + ", password empty=" + (password == null) + ", nativeCredentials empty="
            + (nativeCredentials == null) +"]";
    }

    public boolean isComplete() {
        return complete;
    }

    /**
     * If the credentials are complete and no further roundtrips with the originator are due
     * then this method <b>must</b> be called so that the authentication flow can proceed.
     * 
     * If this credentials are already marked a complete then a call to this method does nothing.
     *
     * @return this
     */
    public AuthCredentials markComplete() {
        this.complete = true;
        return this;
    }

    public void addAttribute(String name, String value) {
        if(name != null && !name.isEmpty()) {
            this.attributes.put(name, value);
        }
    }

    public Map<String, String> getAttributes() {
        return Collections.unmodifiableMap(this.attributes);
    }
}

