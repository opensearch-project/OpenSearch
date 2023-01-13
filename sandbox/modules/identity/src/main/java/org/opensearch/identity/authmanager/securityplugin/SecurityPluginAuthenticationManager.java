/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.authmanager.securityplugin;

import org.opensearch.authn.AccessTokenManager;
import org.opensearch.authn.AuthenticationManager;
import org.opensearch.authn.Subject;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.threadpool.ThreadPool;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class SecurityPluginAuthenticationManager implements AuthenticationManager {
    private final ThreadPool threadPool;

    /**
     * Security manager is loaded with default user set,
     * and this instantiation uses the default security manager
     */
    public SecurityPluginAuthenticationManager(ThreadPool threadPool) {
        this.threadPool = threadPool;
    }

    @Override
    public Subject getSubject() {
        final Object userObj = getThreadContext().getTransient(SecurityPluginConstants.OPENDISTRO_SECURITY_USER);
        Class<?> clazz = userObj.getClass();
        try {
            Method method = clazz.getMethod("getName");
            String fieldValue = (String) method.invoke(userObj);
            SecurityPluginSubject sub = new SecurityPluginSubject(fieldValue);
            return sub;
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        } catch (InvocationTargetException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public AccessTokenManager getAccessTokenManager() {
        return null;
    }

    private ThreadContext getThreadContext() {
        return threadPool.getThreadContext();
    }
}
