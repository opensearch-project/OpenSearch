# Permissions Model - Native AuthZ

Addresses the following [Identity Roadmap](../../../../../../../../IDENTITY_ROADMAP.md) Authorization items:

> - [Complexity 3] Design the permissions model and any potential changes. Notably existing permissions based on action names will be on deprecation path
> - [Complexity 8] For the new permissions model come up with structured naming convention building blocks, such as {SourceName}{ResourceName}{ActionVerb} -> Ad.Detector.Create.  Additionally come up with a 'blessed' list of action verbs to prevent http methods from being used as action names.

### Permissions Model

In order to align with the Identity-focused changes being made to OpenSearch, the way that Permissions are handled requires modification.
Prior to the Identity changes, Permissions were conceptualized as floodgates which controlled access to a given action that an OpenSearch cluster could perform.
The pre-Identity permissions model made common use of a self-describing nomenclature which saw many permissions correlate to REST API operations.
Despite this correlation, many REST API operations make use of numerous permissions at a time making the control of API access insufficient.

With the transition to an Identity-based OpenSearch, it is productive to re-conceptualize Permissions as tokens which grant subjects increased privileges to certain data and operations.
Keeping with the general practice of granting "least privileges" Permissions can be renovated to associate with a given subject Identity instead of being based on a cluster operation.
Instead of Permissions primarily serving to restrict the certain operations on a cluster, the new Permission model seeks to grant individual Subject the privileges to execute operations.

To further clarify this change consider the difference between the two lenses of the scenario outlined on the [OpenSearch Permission Documentation](https://opensearch.org/docs/latest/security-plugin/access-control/permissions/):

Imagine that a subject `User_1` seeks to perform a `_bulk` request using the following:

```
POST _bulk
{ "delete": { "_index": "test-index", "_id": "tt2229499" } }
{ "index": { "_index": "test-index", "_id": "tt1979320" } }
{ "title": "Rush", "year": 2013 }
{ "create": { "_index": "test-index", "_id": "tt1392214" } }
{ "title": "Prisoners", "year": 2013 }
{ "update": { "_index": "test-index", "_id": "tt0816711" } }
{ "doc" : { "title": "World War Z" } }
```

Under the previous Permissions model, `User_1` needed to have four separate permissions for `test_index` which all required verification each time the operation would be performed.

Under the new model of Permissions, it is possible to simply check the Identity of `User_1` for the required permissions and then after authorizing the request once, the operation can be assumed authorized until the authorization token expires. This Permission model is similar to the
authentication model introduced with the Identity changes and makes use of a Permission model that maintains a least privileges model by assuming all Subjects are permission-less upon initial authentication.
After a Subject is authenticated, Permission tokens are only added to their Identity upon the Subjects first attempt to operate using them. Returning to the example, when `User_1` logs into OpenSearch, they are assumed to have no permissions.
Then, when they attempt to perform the `_bulk` request, the appropriate Permissions checks are executed for each of the operations constituting their request. As each operation is approved, a decaying token is added to their Identity which signifies
that for the life of the token, any further attempts to perform the same type of operation are automatically authorized. After a decay period, the token is reset and the subject will once again require authorization in order to perform the requests.
Tokens are also reset everytime the authentication token of the Subject is reset.


### Permission Nomenclature

With the restructuring of the Permissions Model, a simplification of the Permissions Nomenclature is in order. Instead of the previous naming convention which
emphasized the different operations which required permissions at the `cluster` and `index` level, the new naming convention takes an Identity-focused structure.

For any given Permission, the terminology of the specific operation can be broken into a constituent pattern `<Subject>.<Scope>.<Operation>.<Token_Hash>`. For instance,
returning to the above `_bulk` request example, the Permission associated with `User_1` deleting data from `test-index` would be: `User_1.test-index.delete.<Token_Hash>`.
The Token Hash is an identifier associated with the current authorization token which allows the Permissions authorization methods to quickly check the appropriate token field of the Subject.
This new nomenclature pattern has the benefit of being easy to understand in logs and translating across both cluster-scale and index-scale permissions.
