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
import org.opensearch.core.transport.TransportResponse;

import java.io.IOException;
import java.util.Objects;

/**
 * This class represents a Transport Request for issuing a service account to an extension.
 */
public class IssueServiceAccountResponse extends TransportResponse {

	private String name;
	private String serviceAccountToken;

	/**
	 * This takes in a name for the extension and the service account token string
	 * @param name The name of the extension
	 * @param serviceAccountToken A string encapsulating the service account token
	 */
	public IssueServiceAccountResponse(String name, String serviceAccountToken) {
		this.name = name;
		this.serviceAccountToken = serviceAccountToken;
	}

	/**
	 * This takes in a stream containing for the extension and the service account token
	 * @param in the stream containing the extension name and the service account token
	 */
	public IssueServiceAccountResponse(StreamInput in) throws IOException {
		this.name = in.readString();
		this.serviceAccountToken = in.readString();
	}

	@Override
	public void writeTo(StreamOutput out) throws IOException {
		out.writeString(name);
		out.writeString(serviceAccountToken);
	}

	/**
	 * @return the node that is currently leading, according to the responding node.
	 */

	public String getName() {
		return this.name;
	}

	public String getServiceAccountString() {
		return this.serviceAccountToken;
	}

	@Override
	public String toString() {
		return "IssueServiceAccountResponse{" + "name = " + name + " , " + "received service account = " + serviceAccountToken + "}";
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		IssueServiceAccountResponse that = (IssueServiceAccountResponse) o;
		return Objects.equals(name, that.name) && Objects.equals(serviceAccountToken, that.serviceAccountToken);
	}

	@Override
	public int hashCode() {
		return Objects.hash(name, serviceAccountToken);
	}

}
