# Views

Views define how searches are performed against indices on a cluster, uniform data access that is configured separately from the queries.


## Design

### View data

Views create a mapping to the resources that hold information to be searched over in a consistent manner.  This abstraction allows for indirection with the backing indices, so they might be changed without callers being impacted.  This can also be used to simplify the security model - searches over views do not require permissions to the backing indices only permissions to the view itself.

```mermaid
classDiagram
    class View {
        +String name
        +String description
        +long createdAt
        +long modifiedAt
        +List<Target> targets
        +toXContent(XContentBuilder, Params) XContentBuilder
        +writeTo(StreamOutput) void
    }
    class Target {
        +String indexPattern
        +toXContent(XContentBuilder, Params) XContentBuilder
        +writeTo(StreamOutput) void
    }
    class StreamOutput
    class XContentBuilder

    View  -- Target : contains
    View -- StreamOutput : writes to
    View -- XContentBuilder : outputs to
    Target -- StreamOutput : writes to
    Target -- XContentBuilder : outputs to
```

### View persistence

Views are long lived objects in OpenSearch, all operations on them should be fully committed before responding to the caller.  Views are intentionally created for user scenarios following a similar creation cadence to indices.

Committed implies that the updates are synchronized across all nodes in a cluster.  The Cluster Metadata Store is already available and allows for acknowledging that changes have been applied to all nodes.  While this data could be stored in a new purpose built index, index data replication has delays and ensuring synchronization is non-trivial to implement as is seen in the Security plugins [1].

- [1] https://github.com/opensearch-project/security/issues/3275

```mermaid
sequenceDiagram
    participant Client
    participant HTTP_Request as ActionHandler
    participant Cluster_Metadata as Cluster Metadata Store
    participant Data_Store as Indices

    Client->>HTTP_Request: View List/Get/Update/Create/Delete<BR>/views or /views/{view_id}
    HTTP_Request->>Cluster_Metadata: Query Views
    alt Update/Create/Delete
        Cluster_Metadata->>Cluster_Metadata: Refresh Cluster
    end
    Cluster_Metadata-->>HTTP_Request: Return
    HTTP_Request-->>Client: Return

    Client->>HTTP_Request: Search View<br>/views/{view_id}/search
    HTTP_Request->>Cluster_Metadata: Query Views
    Cluster_Metadata-->>HTTP_Request: Return
    HTTP_Request->>HTTP_Request: Rewrite Search Request
    HTTP_Request->>HTTP_Request: Validate Search Request
    HTTP_Request->>Data_Store: Search indices
    Data_Store-->>HTTP_Request: Return
    HTTP_Request-->>Client: Return
```

### Resource Request

In order to permissions views OpenSearch needs a way to consistently refer to them, this is a generic problem and views will be a first use case.  Resource requests require a map of types to identifiers for the request, multiple resources could be part of a single request, but only one of each type.

Considering the request to search a view, `POST /view/{view_id}/_search`, the path parameter 'view_id' is the type and the value from the request would be the identifier.  

```java
public interface ResourceRequest {
   /** Returns the resource types and ids associated with this request */
    Map<String, String> getResourceTypeAndIds();

    /** Validates the resource type and id pairs are in an allowed format */
    public static ActionRequestValidationException validResourceIds(
        final ResourceRequest resourceRequest,
        final ActionRequestValidationException validationException
    ) {;}
}
```

### Resource Permission Grants
With requests include resource type and identifiers the security plugin will need to allow for grants to these new types.  Modify the security role to include this information so it can be checked and then the request can be permitted.

```yaml
all_access:
  reserved: true
  hidden: false
  static: true
  description: "Allow full access to all indices and all cluster APIs"
  cluster_permissions:
    - "*"
  index_permissions:
    - index_patterns:
        - "*"
      allowed_actions:
        - "*"
  tenant_permissions:
    - tenant_patterns:
        - "*"
      allowed_actions:
        - "kibana_all_write"
  resource_permissions:
    - resource_type: "view"
      resource_ids: ["songs", "albums"]
```

## Frequently Asked Questions

### How do views work with fine grain access control of index data?
*To be determined...*

### What happens with existing DLS and FLS rules and searches on views? 
*To be determined...*

### Additional Question(s)
*To be determined...*

## Appendix

### Local Testing

```
curl localhost:9200/abc/_doc \
 -XPOST \
 --header "Content-Type: application/json" \
 --data '{"foo":"bar"}' \

curl localhost:9200/views \
 -XPOST \
 --header "Content-Type: application/json" \
 --data '{"name":"hi", "createdAt": -1, "modifiedAt": -1, "targets":[]}' \
 -v

curl localhost:9200/views \
 -XPOST \
 --header "Content-Type: application/json" \
 --data '{"name":"hi", "createdAt": -1, "modifiedAt": -1, "targets":[{"indexPattern":"abc"}]}' \
 -v


curl localhost:9200/views/hi/_search
```

### v0 View Data Model

```
VIEW MODEL
{
   name: STRING,        // [Optional] Friendly name resolves to ID
   id: STRING,          // Non-mutatable identifier
   description: STRING, // [Optional] Description of the view
   created: DATE,       // Creation time of the view
   modified: DATE       // Last modified time of the view
   query: QUERY,        // enforced query
   filter: QUERY,       // P2 enforced query after transformations
   targets: [
      {
         indexPattern: STRING, // No wildcard/aliases!
                               // P2 Allow wildcard/aliases query parameter
         query: QUERY,         // enforced query specific for this target
         filter: QUERY,        // P2 enforced query specific after transformations
         documentTransformer: SCRIPT // P2 Convert the results in some way
      }
   ],
   documentTransformer: SCRIPT  // P2 Convert the results in some way
}
```

#### View Operations

| Method | Path |
| - | - |
| POST   | /views |
| GET    | /views/{view_id} |
| PUT    | /views/{view_id} | 
| PATCH  | /views/{view_id} | 
| DELETE | /views/{view_id} |

#### Enumerate Views

| Method | Path |
| - | - |
| GET | /views |

#### Perform a Search on a view
| Method | Path |
| - | - |
| GET  | /views/{view_id}/_search |
| POST | /views/{view_id}/_search |

#### Search Views // P2?
| Method | Path |
| - | - |
| GET  | /views/_search |
| POST | /views/_search |

#### Mapping // P2?  Need to understand the utility / impact of not having this
| Method | Path |
| - | - |
| GET   | /views/{view_id}/_mappings |
| PUT   | /views/{view_id}/_mappings |
| PATCH | /views/{view_id}/_mappings |


*Results do not include any fields '_', how to protect leaking data?*

#### Response on Create/Enumerate/Search

views: [
   {
      name: STRING,
      id: STRING,
      description: STRING,
      created: DATE,
      modified: DATE
   }
]