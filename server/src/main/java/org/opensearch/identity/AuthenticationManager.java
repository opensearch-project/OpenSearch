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
    public Subject getSubject();

    // Update the current subject

    /**
     * Authenticate a Subject via a supported token
     *
     * Note: define exceptional states
     * Exceptions:
     *  Token not supported
     * */
    public void login(final AuthenticationToken token);

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
