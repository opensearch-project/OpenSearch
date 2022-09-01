package org.opensearch.identity;

/**
 * Results of permissions checks
 * */
public interface PermissionResult {

    /**
     * True if the action is allowed, false if it should be rejected.
     * */
    public boolean isAllowed();

    /**
     * Gets the permission error message.
     *
     * Detailed messages contents are governed by access to permissions 'opensearch:security/permission-result/details', disabled by default
     * */
    public String getErrorMessage();
}
