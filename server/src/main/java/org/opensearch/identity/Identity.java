package org.opensearch.identity;

/**
 * Application wide access for identity systems
 */
public final class Identity {
    private static AuthenticationManager AUTH_MANAGER = null;
    
    /** Do not allow instances of this class to be created */
    private Identity() {}

    /** Gets the Authentication Manager for this application */
    public static AuthenticationManager getAuthenticationManager() {
        return AUTH_MANAGER;
    }

    /** Gets the Authentication Manager for this application */
    public static void setAuthenticationManager(final AuthenticationManager authenticationManager) {
        AUTH_MANAGER = authenticationManager;
    }
}
