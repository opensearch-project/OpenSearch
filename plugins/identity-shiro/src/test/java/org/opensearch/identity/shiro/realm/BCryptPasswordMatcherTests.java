/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.shiro.realm;

import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.hamcrest.Matchers.equalTo;

import org.apache.shiro.authc.UsernamePasswordToken;
import org.apache.shiro.authc.AuthenticationInfo;

public class BCryptPasswordMatcherTests extends OpenSearchTestCase {

    public void testCredentialMatch() {
        final UsernamePasswordToken token = mock(UsernamePasswordToken.class);
        when(token.getPassword()).thenReturn("admin".toCharArray());
        final AuthenticationInfo info = mock(AuthenticationInfo.class);
        when(info.getCredentials()).thenReturn("$2a$12$VcCDgh2NDk07JGN0rjGbM.Ad41qVR/YFJcgHp0UGns5JDymv..TOG");

        final BCryptPasswordMatcher matcher = new BCryptPasswordMatcher();
        final boolean result = matcher.doCredentialsMatch(token, info);

        assertThat(result, equalTo(true));
    }

    public void testCredentialDoNotMatch() {
        final UsernamePasswordToken token = mock(UsernamePasswordToken.class);
        when(token.getPassword()).thenReturn("HashedPassword".toCharArray());
        final AuthenticationInfo info = mock(AuthenticationInfo.class);
        when(info.getCredentials()).thenReturn("$2a$12$VcCDgh2NDk07JGN0rQGbM.Ad41qVR/YFJcgHp0UGns5JDymv..TOG");

        final BCryptPasswordMatcher matcher = new BCryptPasswordMatcher();
        final boolean result = matcher.doCredentialsMatch(token, info);

        assertThat(result, equalTo(false));
    }
}
