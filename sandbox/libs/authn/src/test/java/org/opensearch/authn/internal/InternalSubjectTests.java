/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.authn.internal;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import org.apache.shiro.SecurityUtils;
import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.subject.Subject;
import org.junit.After;
import org.junit.Before;
import org.opensearch.authn.AuthenticationManager;
import org.opensearch.authn.tokens.AuthenticationToken;
import org.opensearch.authn.tokens.BasicAuthToken;
import org.opensearch.identity.Identity;
import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class InternalSubjectTests extends OpenSearchTestCase {
    private static final AuthenticationToken AUTH_TOKEN = new BasicAuthToken("Basic YWRtaW46YWRtaW4=");

    @Before
    public void initRealm() {
        final AuthenticationManager authManager = new InternalAuthenticationManager();
        Identity.setAuthManager(authManager);
    }

    @After
    public void logoutSubject() {
        Subject subject = SecurityUtils.getSubject();
        if (subject == null) return;
        if (subject.getSession() != null) subject.getSession().stop();
        SecurityUtils.getSubject().logout();
    }

    public void testGetPrincipal() {
        InternalSubject internalSubject = new InternalSubject(SecurityUtils.getSubject());
        assertNull(internalSubject.getPrincipal());

        internalSubject.login(AUTH_TOKEN);

        assertNotNull(internalSubject.getPrincipal());
    }

    public void testLoginBehaviors() {
        Subject subject = mock(Subject.class);
        InternalSubject internalSubject = new InternalSubject(subject);

        // 1. Verify that login() is called on first attempt

        internalSubject.login(AUTH_TOKEN);
        verify(subject, times(1)).login(any());

        // 2. Verify that call to login() is skipped on 2nd attempt for same user

        when(subject.isAuthenticated()).thenReturn(true);
        when(subject.getPrincipal()).thenReturn("admin");

        internalSubject.login(AUTH_TOKEN);
        verify(subject, times(1)).login(any());

        // 3. Verify that login() is called again for a different user

        when(subject.isAuthenticated()).thenReturn(true);
        when(subject.getPrincipal()).thenReturn("marvin");

        // even though we are using same token, the mock stub returns different principal and hence login is called again
        internalSubject.login(AUTH_TOKEN);
        verify(subject, times(2)).login(any());
    }

    public void testSuccessfulLoginAndLogOut() {
        InternalSubject internalSubject = new InternalSubject(SecurityUtils.getSubject());

        internalSubject.login(AUTH_TOKEN);
        assertTrue(internalSubject.isAuthenticated());

        internalSubject.logout();
        assertFalse(internalSubject.isAuthenticated());

        // subsequent logout calls for same user does not fail
        internalSubject.logout();
    }

    public void testLoginFailure() {
        InternalSubject internalSubject = new InternalSubject(SecurityUtils.getSubject());

        AuthenticationToken authToken = new BasicAuthToken("Basic bWFydmluOmdhbGF4eQ==");

        assertThrows(AuthenticationException.class, () -> internalSubject.login(authToken));
    }

    public void testIsAuthenticated() {
        InternalSubject internalSubject = new InternalSubject(SecurityUtils.getSubject());

        assertFalse(internalSubject.isAuthenticated());
        internalSubject.login(AUTH_TOKEN);
        assertTrue(internalSubject.isAuthenticated());
    }
}
