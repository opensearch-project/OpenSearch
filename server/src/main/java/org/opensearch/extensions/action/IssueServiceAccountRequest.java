/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions.action;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.transport.TransportRequest;

import java.io.IOException;
import java.util.Objects;

/**
 * This class represents a Transport Request for issuing a service account to an extension.
 */
public class IssueServiceAccountRequest extends TransportRequest {

	private final String serviceAccountToken;

	/**
	 * This takes a string representing a service account token
	 * @param serviceAccountToken The string making up the service account token
	 */
	public IssueServiceAccountRequest(String serviceAccountToken) {
		this.serviceAccountToken = serviceAccountToken;
	}

	/**
	 * This takes a stream representing a service account token
	 * @param in The stream passing the token
	 */
	public IssueServiceAccountRequest(StreamInput in) throws IOException {
		super(in);
		this.serviceAccountToken = in.readString();
	}

	@Override
	public void writeTo(StreamOutput out) throws IOException {
		super.writeTo(out);
		out.writeString(serviceAccountToken);
	}

	public String getServiceAccountToken() {
		return this.serviceAccountToken;
	}

	@Override
	public String toString() {
		return "IssueServiceAccountRequest {" + "serviceAccountToken=" + serviceAccountToken + "}";
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		IssueServiceAccountRequest that = (IssueServiceAccountRequest) o;
		return Objects.equals(serviceAccountToken, that.serviceAccountToken);
	}

	@Override
	public int hashCode() {
		return Objects.hash(serviceAccountToken);
	}
}
