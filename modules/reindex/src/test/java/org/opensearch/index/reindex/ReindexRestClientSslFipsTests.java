/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.reindex;

import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.env.TestEnvironment;
import org.opensearch.watcher.ResourceWatcherService;
import org.hamcrest.Matchers;

import java.io.IOException;

import static org.mockito.Mockito.mock;

public class ReindexRestClientSslFipsTests extends ReindexRestClientSslTests {

    public void testClientWithVerificationDisabled() throws IOException {
        final Settings settings = Settings.builder()
            .put("path.home", createTempDir())
            .put("reindex.ssl.verification_mode", "NONE")
            .put("reindex.ssl.supported_protocols", "TLSv1.2")
            .build();
        final Environment environment = TestEnvironment.newEnvironment(settings);
        var exception = expectThrows(
            IllegalStateException.class,
            () -> new ReindexSslConfig(settings, environment, mock(ResourceWatcherService.class))
        );
        assertThat(exception.getMessage(), Matchers.containsString("The use of TrustEverythingConfig is not permitted in FIPS mode"));
    }

}
