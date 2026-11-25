/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Pull-based ingestion source that reads objects at rest in object storage (S3, GCS, Azure Blob) directly
 * through OpenSearch's {@link org.opensearch.common.blobstore.BlobContainer} abstraction. The source splits the
 * work deterministically across shards from a frozen manifest, frames records into byte arrays, derives a
 * positional and injection-safe {@code _id}, and emits them into the existing pull-based ingestion pipeline.
 */
package org.opensearch.plugin.ingestion.objectstore;
