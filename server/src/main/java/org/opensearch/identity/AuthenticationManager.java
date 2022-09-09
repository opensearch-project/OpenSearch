package org.opensearch.identity;

import java.util.concurrent.Callable;

/**
 * Authentication management for OpenSearch.
 *
 * Retrieve the current subject, switch to a subject, and persist subject identity through request lifetime
 * */
public interface AuthenticationManager {

    /**
     * Get the current subject
     * */
    public Subject getCurrentSubject();

    // Update the current subject

    /**
     * Authenticate a Subject via an http header
     *
     * Note: define exceptional states
     * */
    public void authenticateWithHeader(final String authorizationHeader);

    /**
     * authenticateAs a Subject by an identifier
     *
     * Why dangerous?  authenticateAs(...) allows OpenSearch to run as Subject without
     * any other checks or safeguards.  If downstream code is not safeguarded it could run
     * as permissions that are not indented, resulting in security issues.
     *
     * External data should never be directly populate subject
     *
     * Note: define exceptional states
     *
     * */
    public AuthenticationSession dangerousAuthenticateAs(final String subject);

    // Persists subject lifetime through thread boundaries

    /**
     * Ensures the execution of this runnable is done with the current subject
     * */
    public Runnable associateWith(Runnable r);

    /**
     * Ensures the execution of this callable is done with the current subject
     * */
    public <V> Callable<V> associateWith(Callable<V> c);

    /**
     * Executes this runnable is done with the current subject
     * */
    public void executeWith(Runnable r);

    /**
     * Executes this callable is done with the current subject
     * */
    public <V> V executeWith(Callable<V> c);
}
