package org.opensearch.identity.noop;

import org.opensearch.identity.AuthenticationManager;
import org.opensearch.identity.AuthenticationToken;
import org.opensearch.identity.Subject;

/** No Operation implementation of an Authentication Manager */
public class NoopAuthenticationManger implements AuthenticationManager {

    public Subject getSubject() {
        return null;
    }

    public void login(AuthenticationToken token) {
    }

    public Runnable systemLogin(final Runnable runnable, final String systemResource) {
        return runnable;
    }
}
