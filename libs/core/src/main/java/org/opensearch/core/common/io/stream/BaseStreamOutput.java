/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.core.common.io.stream;

import java.io.OutputStream;

/**
 * Foundation class for writing core types over the transport stream
 *
 * todo: refactor {@code StreamOutput} primitive writers to this class
 *
 * @opensearch.internal
 */
public abstract class BaseStreamOutput extends OutputStream {}
