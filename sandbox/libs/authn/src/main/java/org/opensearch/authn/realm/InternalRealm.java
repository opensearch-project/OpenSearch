/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.authn.realm;

import org.apache.cxf.rs.security.jose.jwt.JwtToken;
import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.authc.AuthenticationInfo;
import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.authc.BearerToken;
import org.apache.shiro.authc.CredentialsException;
import org.apache.shiro.authc.IncorrectCredentialsException;
import org.apache.shiro.authc.SimpleAuthenticationInfo;
import org.apache.shiro.authc.UnknownAccountException;
import org.apache.shiro.authc.UsernamePasswordToken;
import org.apache.shiro.realm.AuthenticatingRealm;

import org.opensearch.authn.StringPrincipal;
import org.opensearch.authn.User;
import org.opensearch.authn.jwt.BadCredentialsException;
import org.opensearch.authn.jwt.JwtVerifier;

import java.util.Objects;
import java.util.concurrent.ConcurrentMap;
import java.util.List;
import java.util.Map;

/**
 * Internal Realm is a custom realm using the internal OpenSearch IdP
 *
 * @opensearch.experimental
 */
public class InternalRealm extends AuthenticatingRealm {
    public static final String INVALID_SUBJECT_MESSAGE = "Subject can't be null";

    public static final String INVALID_ARGUMENTS_MESSAGE = "primaryPrincipal or hash can't be null or empty";

    public static final String INCORRECT_CREDENTIALS_MESSAGE = "Incorrect credentials";

    private static final String DEFAULT_REALM_NAME = "internal";

    private static final String DEFAULT_INTERNAL_USERS_FILE = "example/example_internal_users.yml";

    public static final InternalRealm INSTANCE = new InternalRealm.Builder(DEFAULT_REALM_NAME, DEFAULT_INTERNAL_USERS_FILE).build();

    private String realmName;

    private ConcurrentMap<String, User> internalUsers;

    private InternalRealm(String realmName, ConcurrentMap<String, User> internalUsers) {
        super(new BCryptPasswordMatcher());
        this.realmName = realmName;
        this.internalUsers = internalUsers;
    }

    public InternalRealm() {
        super(new BCryptPasswordMatcher());
    }

    public static final class Builder {
        private final String name;

        private final String pathToInternalUsersYaml;

        public Builder(String name, String pathToInternalUsersYaml) {
            this.name = Objects.requireNonNull(name);
            this.pathToInternalUsersYaml = pathToInternalUsersYaml;
        }

        public InternalRealm build() {
            ConcurrentMap<String, User> internalUsers = InternalUsersStore.readUsersAsMap(pathToInternalUsersYaml);
            return new InternalRealm(name, internalUsers);
        }
    }

    private void initializeUsersStore(String pathToInternalUsersYaml) {
        // TODO load this at cluster start
        internalUsers = InternalUsersStore.readUsersAsMap(pathToInternalUsersYaml);
    }

    public User getInternalUser(String principalIdentifier) throws UnknownAccountException {
        User userRecord = internalUsers.get(principalIdentifier);
        // UserRecord userRecord = lookupUserRecord(username);
        // No record found - don't know who this is
        if (userRecord == null) {
            throw new UnknownAccountException(principalIdentifier + " does not exist in " + realmName + " realm.");
        }
        return userRecord;
    }

    // TODO: Revisit this
    // This was overridden to support all kinds of AuthTokens
    @Override
    public boolean supports(AuthenticationToken token) {
        return true;
    }

    @Override
    protected void assertCredentialsMatch(AuthenticationToken token, AuthenticationInfo info) throws AuthenticationException {
        if (token instanceof BearerToken) {
            // TODO: Check if this is correct
            // Token has previously been verified at doGetAuthenticationInfo
            // no auth required as bearer token is assumed to have correct credentials
        } else if (token instanceof UsernamePasswordToken) {
            super.assertCredentialsMatch(token, info); // continue as normal for basic-auth token
        }
    }

    @Override
    protected AuthenticationInfo doGetAuthenticationInfo(AuthenticationToken token) throws AuthenticationException {
        if (token instanceof UsernamePasswordToken) {
            String username = ((UsernamePasswordToken) token).getUsername();
            // Look up the user by the provide username
            User userRecord = getInternalUser(username);
            // Check for other things, like a locked account, expired password, etc.

            // Verify the user
            SimpleAuthenticationInfo sai = new SimpleAuthenticationInfo(
                userRecord.getPrimaryPrincipal(),
                userRecord.getBcryptHash(),
                realmName
            );
            boolean successfulAuthentication = getCredentialsMatcher().doCredentialsMatch(token, sai);

            if (successfulAuthentication) {
                // Check for anything else that might prevent login (expired password, locked account, etc.)
                // if (other problems) {
                // throw new CredentialsException(); // Or something more specific
                // }
                // Success!
                return sai;
            } else {
                // Bad password
                throw new IncorrectCredentialsException(INCORRECT_CREDENTIALS_MESSAGE);
            }
        } else if (token instanceof BearerToken) {
            JwtToken jwtToken;

            // Verify the validity of JWT token
            try {
                jwtToken = JwtVerifier.getVerifiedJwtToken(((BearerToken) token).getToken());
            } catch (BadCredentialsException e) {
                throw new IncorrectCredentialsException(e.getMessage()); // Invalid Token
            }

            String subject = jwtToken.getClaims().getSubject();

            // We need to extract the subject here to create an identity subject that can be utilized across the realm
            return new SimpleAuthenticationInfo(subject, null, realmName);
        }
        // Don't know what to do with this token
        throw new CredentialsException();
    }

    // TODO: Expose all the operations below as a rest API

    /**
     * Creates a user in an in-memory data store
     * @param user to be created. It should be passed in {@link User}
     */
    public void createUser(User user) {
        if (user == null) {
            throw new IllegalArgumentException(INVALID_SUBJECT_MESSAGE);
        }
        String primaryPrincipal = user.getPrimaryPrincipal().getName();

        // TODO: should we update if an object already exists with same principal.
        // If so, it should be handled in updateSubject
        if (this.internalUsers.containsKey(primaryPrincipal)) {
            throw new RuntimeException("User with principal= " + primaryPrincipal + " already exists in realm= " + realmName);
        }

        // TODO: add checks to restrict the users that are allowed to create
        this.internalUsers.put(primaryPrincipal, user);
    }

    /**
     * Creates a user in in-memory data-store when relevant details are passed
     * @param primaryPrincipal the primary identifier of this user (must be unique)
     * @param hash the password passed as hash
     * @param attributes passed in key-value format
     * @throws IllegalArgumentException if primaryPrincipal or hash is null or empty
     */
    public void createUser(String primaryPrincipal, String hash, Map<String, String> attributes) {
        // We don't create a user if primaryPrincipal and/or hash is empty or null
        if (primaryPrincipal == null || hash == null || primaryPrincipal == "" || hash == "") {
            throw new IllegalArgumentException(INVALID_ARGUMENTS_MESSAGE);
        }

        User user = new User();
        user.setPrimaryPrincipal(new StringPrincipal(primaryPrincipal));
        user.setBcryptHash(hash);
        user.setAttributes(attributes);

        createUser(user);
    }

    public void setRealmName(String realmName) {
        this.realmName = realmName;
    }

    public void setInternalUsersYaml(String internalUsersYaml) {
        initializeUsersStore(internalUsersYaml);
    }

    /**
     * Updates the user's password
     * @param primaryPrincipal the principal whose password is to be updated
     * @param hash The new password
     * @return true if password update was successful, false otherwise
     *
     * TODO: Add restrictions around who can do this
     */
    public boolean updateUserPassword(String primaryPrincipal, String hash) {
        if (!this.internalUsers.containsKey(primaryPrincipal)) {
            throw new RuntimeException(userDoesNotExistMessage(primaryPrincipal));
        }
        User userToBeUpdated = this.internalUsers.get(primaryPrincipal);
        userToBeUpdated.setBcryptHash(hash);

        this.internalUsers.put(primaryPrincipal, userToBeUpdated);
        return true;
    }

    /**
     * Adds new attributes to the user's current list (stored as map) AND
     * updates the existing attributes if there is a match
     * @param primaryPrincipal the principal whose attributes are to be updated
     * @param attributesToBeAdded new attributes to be added
     * @return true if the addition was successful, false otherwise
     *
     * TODO: Add restrictions around who can do this
     */
    public boolean updateUserAttributes(String primaryPrincipal, Map<String, String> attributesToBeAdded) {
        if (!this.internalUsers.containsKey(primaryPrincipal)) {
            throw new RuntimeException(userDoesNotExistMessage(primaryPrincipal));
        }

        User userToBeUpdated = this.internalUsers.get(primaryPrincipal);
        userToBeUpdated.getAttributes().putAll(attributesToBeAdded);

        this.internalUsers.put(primaryPrincipal, userToBeUpdated);
        return true;
    }

    /**
     * Removes the list of attributes for a given user
     * @param primaryPrincipal the principal whose attributes are to be deleted
     * @param attributesToBeRemoved the list of attributes to be deleted (list of keys in the attribute map)
     * @return true is successful, false otherwise
     *
     * TODO: 1. Are we supporting this. 2. If so add restrictions around who can do this
     */
    public boolean removeAttributesFromUser(String primaryPrincipal, List<String> attributesToBeRemoved) {
        if (!this.internalUsers.containsKey(primaryPrincipal)) {
            throw new RuntimeException(userDoesNotExistMessage(primaryPrincipal));
        }

        User userToBeUpdated = this.internalUsers.get(primaryPrincipal);
        Map<String, String> currentAttributes = userToBeUpdated.getAttributes();
        for (String attribute : attributesToBeRemoved) {
            currentAttributes.remove(attribute);
        }
        userToBeUpdated.setAttributes(currentAttributes);

        this.internalUsers.put(primaryPrincipal, userToBeUpdated);
        return true;
    }

    /**
     * Removes a user given its primaryPrincipal from the in-memory store
     * @param primaryPrincipal the primaryPrincipal of the user to be deleted
     * @return {@linkplain User} the deleted user
     *
     * TODO: Add restrictions around who can do this
     */
    public User removeUser(String primaryPrincipal) {

        User removedUser = this.internalUsers.remove(primaryPrincipal);
        if (removedUser == null) {
            throw new RuntimeException(userDoesNotExistMessage(primaryPrincipal));
        }
        return removedUser;
    }

    /**
     * Generates an Exception message
     * @param primaryPrincipal to be added to this message
     * @return the exception message string
     */
    public String userDoesNotExistMessage(String primaryPrincipal) {
        return "Subject with primaryPrincipal=" + primaryPrincipal + " doesn't exist";
    }
}
