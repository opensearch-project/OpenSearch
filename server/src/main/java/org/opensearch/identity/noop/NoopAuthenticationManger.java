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

    public Runnable associateWith(Runnable r) {
        return r;
    }

    public <V> Callable<V> associateWith(Callable<V> c) {
        return c;
    }

    public void executeWith(Runnable r) {
        r.run();
    }

    public <V> V executeWith(Callable<V> c) {
        try {
            return c.call();
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }
}
