package org.opensearch.identity.noop;

import java.util.concurrent.Callable;

import org.opensearch.identity.AuthenticationManager;
import org.opensearch.identity.PermissionResult;
import org.opensearch.identity.AuthenticationToken;
import org.opensearch.identity.Subject;

public class NoopAuthenticationManger implements AuthenticationManager {

    public Subject getSubject() {
        return null;
    }

    public void login(AuthenticationToken token) {
    }
}
