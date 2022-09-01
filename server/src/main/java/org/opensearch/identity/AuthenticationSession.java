package org.opensearch.identity;

/**
 * Support for try-with-resources for Authentication sessions that need to be manually exited
 * */
public interface AuthenticationSession extends AutoCloseable {
    @Override
    void close();
}