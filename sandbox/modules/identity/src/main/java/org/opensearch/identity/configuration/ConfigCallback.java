/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.configuration;

import org.opensearch.action.get.MultiGetResponse.Failure;

public interface ConfigCallback {

    void success(SecurityDynamicConfiguration<?> dConf);

    void noData(String id);

    void singleFailure(Failure failure);

    void failure(Throwable t);

}
