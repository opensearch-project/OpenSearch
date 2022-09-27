package org.opensearch.identity;

/**
 * Vends out access tokens 
 * 
 * @opensearch.experimental
 */
public interface AccessTokenManager {
    /**
     * Forces expiration on all tokens
     */
    public void expireAllTokens();

    /**
     * Generates a new access token from the current subject
     */
    public AccessToken generate();

    /**
     * Creates a new access token from a previous access token
     */
    public AccessToken refresh(final AccessToken token);
}
