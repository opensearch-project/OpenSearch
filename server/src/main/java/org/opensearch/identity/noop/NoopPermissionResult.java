package org.opensearch.identity.noop;

import org.opensearch.identity.AuthenticationManager;
import org.opensearch.identity.PermissionResult;
import org.opensearch.identity.AuthenticationSession;
import org.opensearch.identity.Subject;

public class NoopPermissionResult implements PermissionResult {

    @Override
    public boolean isAllowed() {
        return true;
    }

    @Override
    public String getErrorMessage() {
        return "No-op permissions results are always allowed";
    }
}
