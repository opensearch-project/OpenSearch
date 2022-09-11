package org.opensearch.identity.noop;

import java.security.Principal;

import org.opensearch.identity.Subject;

/** No Operation implementation of an Subject, always appears signed in */
public class NoopSubject implements Subject {

    @Override
    public Principal getPrincipal() {
        return new Principal() {
            @Override
            public String getName() {
                return "NoOpPrincipal";
            }
        };
    }

    @Override
    public NoopPermissionResult isPermitted(final String permission) {
        return new NoopPermissionResult();
    }
}
