/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.identity;

import java.security.Principal;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchException;
import org.opensearch.common.settings.Settings;
import org.opensearch.identity.noop.NoopIdentityPlugin;
import org.opensearch.identity.noop.NoopSubject;
import org.opensearch.identity.tokens.TokenManager;
import org.opensearch.plugins.IdentityPlugin;
import org.opensearch.common.util.concurrent.ThreadContext;
/**
 * Identity and access control for OpenSearch
 *
 * @opensearch.experimental
 * */
public class IdentityService {
    private static final Logger log = LogManager.getLogger(IdentityService.class);

    private final Settings settings;
    private final IdentityPlugin identityPlugin;
    private final ApplicationManager applicationManager;
    private final AtomicReference<ThreadContext> threadContext = new AtomicReference<>();

    public IdentityService(
        final Settings settings,
        final List<IdentityPlugin> identityPlugins,
        final ApplicationManager applicationManager
    ) {
        this.settings = settings;
        this.applicationManager = applicationManager;

        if (identityPlugins.size() == 0) {
            log.debug("Identity plugins size is 0");
            identityPlugin = new NoopIdentityPlugin();
        } else if (identityPlugins.size() == 1) {
            log.debug("Identity plugins size is 1");
            identityPlugin = identityPlugins.get(0);
        } else {
            throw new OpenSearchException(
                "Multiple identity plugins are not supported, found: "
                    + identityPlugins.stream().map(Object::getClass).map(Class::getName).collect(Collectors.joining(","))
            );
        }
    }

    public void associateThreadContext(final ThreadContext threadContext) {
        if (!this.threadContext.compareAndSet(null, threadContext)) {
            throw new OpenSearchException("Thread context was already associated to identity service");
        }
    }

    /**
     * Gets the current Subject
     */
    public ApplicationAwareSubject getSubject() {
        return getSubjectFromContext().orElseGet(this::createSubjectAndPutInContext);
    }

    /**
     * Gets the token manager
     */
    public TokenManager getTokenManager() {
        return identityPlugin.getTokenManager();
    }

    private static final String SUBJECT_CONTEXT_KEY = "application_aware_subject";
    private Optional<ApplicationAwareSubject> getSubjectFromContext() {
        return Optional.ofNullable(threadContext.get())
            .map(context -> context.getPersistent(SUBJECT_CONTEXT_KEY))
            .map(sub -> {
                if (sub instanceof ApplicationAwareSubject) {
                    return (ApplicationAwareSubject)sub;
                }
                return null;
            });
    }

    private ApplicationAwareSubject createSubjectAndPutInContext() {
        final ApplicationAwareSubject newSubject = new ApplicationAwareSubject(identityPlugin::getSubject, applicationManager);
        Optional.ofNullable(threadContext.get())
            .ifPresent(context -> context.putPersistent(SUBJECT_CONTEXT_KEY, newSubject));
        return newSubject;
    }
}
