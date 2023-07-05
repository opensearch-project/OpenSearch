/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.core.common.io.stream;

import java.io.InputStream;

/**
 * Foundation class for reading core types off the transport stream
 *
 * todo: refactor {@code StreamInput} primitive readers to this class
 *
 * @opensearch.internal
 */
public abstract class BaseStreamInput extends InputStream {}
