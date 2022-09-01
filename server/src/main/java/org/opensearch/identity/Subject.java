package org.opensearch.identity;

import java.security.Principal;

/**
 * An individual, process, or device that causes information to flow among objects or change to the system state.
 * 
 * Used to authorize actions inside of the OpenSearch ecosystem.
 */
public interface Subject {
    /**
     * Get the application-wide uniquely identifying principal
     * */
    public Principal getPrincipal();

    /**
     * Check if the permission is allows for this subject
     *
     * @param permission The type of these expression of permissions is TDB
     * */
    public PermissionResult isPermitted(final String permission);
}
