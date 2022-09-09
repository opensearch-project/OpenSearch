package org.opensearch.identity.noop;

import java.security.Principal;

import org.opensearch.identity.AuthenticationManager;
import org.opensearch.identity.PermissionResult;
import org.opensearch.identity.AuthenticationSession;
import org.opensearch.identity.Subject;

public class NoopSubject implements Subject {

    @Override
    public Principal getPrincipal() {
        return null;
    }

    @Override
    public NoopPermissionResult isPermitted(final String permission) {
        return new NoopPermissionResult();
    }
}
