package org.opensearch.identity;

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

    /**
     * Authenticates the runnable as an OpenSearch system response.
     * 
     * TODO: systemResource is coming through as `opensearch[runTask-0][management]` when
     * run through the OpenSearchThreadFactory, ideally this should be an enum looking somewhat
     * like ThreadPool.Names that can having a mapping for permissions associated with it.
     */
    public Runnable systemLogin(final Runnable runnable, final String systemResource);
}
