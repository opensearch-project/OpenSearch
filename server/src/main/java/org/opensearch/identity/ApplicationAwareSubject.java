/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.identity;

import java.security.Principal;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.Supplier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.opensearch.identity.scopes.ApplicationScope;
import org.opensearch.identity.scopes.ApplicationScope;
import org.opensearch.identity.scopes.Scope;
import org.opensearch.identity.tokens.AuthToken;

/**
 * This class defines an ApplicationAwareSubject.
 *
 * An ApplicatioAwarenSubject is a type of Subject which wraps another basic Subject object.
 * With an ApplicationAwareSubject, we represent a system that interacts with the Identity access control system.
 *
 * @opensearch.experimental
 */
@SuppressWarnings("overrides")
public class ApplicationAwareSubject implements Subject {

    private final AtomicReference<Application> assumedApplication = new AtomicReference<>();
    private final Supplier<Subject> wrapped;
    private final ApplicationManager applicationManager;

    /**
     * We wrap a basic Subject object to create an ApplicationAwareSubject -- this should come from the IdentityService
     * @param wrapped The Subject to be wrapped
     */
    public ApplicationAwareSubject(final Supplier<Subject> wrapped, final ApplicationManager applicationManager) {
        this.wrapped = wrapped;
        this.applicationManager = applicationManager;
    }

    /**
     * Checks scopes of an application subject and determine if it is allowed to perform an operation based on the given scopes
     * @param scopes The scopes to check against the subject
     * @return true if allowed, false if none of the scopes are allowed.
     */
    public boolean isAllowed(final List<Scope> scopes) {
        final boolean isSubjectApplicationAllowed = isAllowed(getApplication(), scopes);

        final Optional<Principal> assumedApplicationPrincipal = Optional.ofNullable(assumedApplication.get())
            .map(Application::getPrincipal);
        final boolean isAssumedApplicationAllowed = isAllowed(assumedApplicationPrincipal, scopes);

        return isSubjectApplicationAllowed && isAssumedApplicationAllowed;
    }

    private boolean isAllowed(final Optional<Principal> application, final List<Scope> scopes) {
        if (application.isEmpty()) {
            // If there is no application, actions are allowed by default
            return true;
        }

        final Optional<Set<Scope>> scopesOfApplication = applicationManager.getScopes(application.get());
        if (scopesOfApplication.isEmpty()) {
            // If no matching application was found, actions are denied by default
            return false;
        }

        final boolean isApplicationSuperUser = scopesOfApplication.get().contains(ApplicationScope.SUPER_USER_ACCESS);
        if (isApplicationSuperUser) {
            return true;
        }

        // Retain only the elements present in the list
        final Set<Scope> scopesCopy = new HashSet<>(scopesOfApplication.get());
        scopesCopy.retainAll(scopes);

        final boolean hasMatchingScopes = !scopesCopy.isEmpty();
        return hasMatchingScopes;
    }

    public synchronized <T> T runAs(final Application application, final Callable<T> callable) {
        if (!assumedApplication.compareAndSet(null, application)) {
            throw new AssumeIdentityException("Subject is already running as another application, " + assumedApplication.get() + ", tried to become " + application);
        }
        try {
            return callable.call();
        } catch (final Exception e) {
            throw new RuntimeException(e);
        } finally {
            assumedApplication.set(null);
        }
    }

    public Principal getPrincipal() {
        return wrapped.get().getPrincipal();
    }

    public void authenticate(final AuthToken token) {
        wrapped.get().authenticate(token);
    }

    public Optional<Principal> getApplication() {
        return wrapped.get().getApplication();
    }
}
