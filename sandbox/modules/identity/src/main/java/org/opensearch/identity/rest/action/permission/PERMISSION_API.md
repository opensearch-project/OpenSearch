/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

openapi: 3.0.0
info:
  version: 1.0.0
  title: OpenSearch Identity Permission API
  license:
    name: Apache 2.0
paths:
  /permissions:
    get:
      summary: List all permissions associated with a principal
      operationId: checkPermissions
      tags:
        - permissions
      parameters:
        - name: principal name
          in: query
          description: the principal name to get the permissions for
          required: true
          schema:
            type: string
            format: utf-8
            $ref: 'OpenSearch/sandbox/libs/authn/src/main/Principals'
      responses:
        "200":
          description: A list of permission strings
          content:
            application/json:
              schema:
                $ref: '#/../../../../authz/OpenSearchPermissions'
        default:
          description: illegal argument exception
    post:
      summary: Add a permission
      operationId: Add a permission
      tags:
        - permissions
      parameters:
          - name: principal name
            in: query
            description: the principal name to grant a permission to
            required: true
            schema:
              type: string
              format: utf-8
              $ref: 'OpenSearch/sandbox/libs/authn/src/main/Principals'
          - name: permission string
            in: query
            description: the permission string for the permission being granted
            required: true
            schema:
              type: string
              format: utf-8
              $ref: '#/../../../../authz/OpenSearchPermissions'
      responses:
        "200":
          description: Success
          application/json:
            schema:
              type: string
              format: utf-8
              $ref: '#/add/AddPermissionResponseInfo'
        default:
          description: unexpected error
          schema:
            $ref: '#/add/AddPermissionResponseInfo'
    delete:
      summary: Delete a permission
      operationId: Delete a permission
      tags:
        - permissions
      parameters:
        - name: principal name
          in: query
          description: the principal name to delete a permission from
          required: true
          schema:
            type: string
            format: utf-8
            $ref: 'OpenSearch/sandbox/libs/authn/src/main/Principals'
        - name: permission string
          in: query
          description: the permission string for the permission being deleted
          required: true
          schema:
            type: string
            format: utf-8
            $ref: '#/../../../../authz/OpenSearchPermissions'
      responses:
        "200":
          description: Success
          application/json:
            schema:
              type: string
              format: utf-8
              $ref: '#/add/DeletePermissionResponseInfo'
        default:
          description: unexpected error
          schema:
            $ref: '#/add/DeletePermissionResponseInfo'
