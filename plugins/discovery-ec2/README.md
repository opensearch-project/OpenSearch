# discovery-ec2

The discovery-ec2 plugin allows OpenSearch to find the master-eligible nodes in a cluster running on AWS EC2 by querying the AWS Instance Metadata Service ([IMDS](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-instance-metadata.html)) API for the addresses of the EC2 instances running these nodes.

This is typically configured as follows in `opensearch.yml`.

```yml
cluster.initial_cluster_manager_nodes: ["seed"]
discovery.seed_providers: ec2
discovery.ec2.tag.role: my-cluster-manager
```

The implementation in the discovery-ec2 plugin queries for instances of a given type (e.g. private or public IP), with a tag, or running in a specific availability zone.

## Testing

### Unit and Integration Tests

```
./gradlew :plugins:discovery-ec2:check
```

### Manual Testing

The following instructions manually exercise the plugin.

Setup a 5-node (Ubuntu) cluster on EC2, with 3 of them tagged with `role: my-cluster-manager`, and a custom TCP rule to expose ports 9200 to 9300 to allow TCP traffic. Default EC2 configuration only allows SSH TCP traffic and hence only exposes port 22.

Set the following properties.

- `network.host`: Set this to the IP of the eth0 network adapter, can be fetched by `ipconfig` or by running `hostname -I` on Linux hosts.
- `discovery.seed_hosts`: List of all nodes in the cluster.
- `cluster.initial_cluster_manager_nodes`: List of all initial cluster manager nodes.
- `discovery.seed_providers`: Set this to `ec2` for EC2 IMDS based discovery.
- `discovery.ec2.tag.role: my-cluster-manager`: Filter out only those nodes with value `my-cluster-manager` for the `role` tag.
- `discovery.ec2.region`: Optionally set to your region, e.g. `us-east-1`, by default the plugin uses the region of the current EC2 instance.

While sending the request to IMDS, specified discovery settings for finding master-eligible nodes are being set. You will see the following in the logs.

```
[2023-05-31T15:22:39,274][DEBUG][o.o.d.e.AwsEc2SeedHostsProvider] [ip-172-31-73-184] using host_type [private_ip], tags [{role=[my-cluster-manager]}], groups [[]] with any_group [true], availability_zones [[]]
```

The nodes getting added as eligible masters are ones with the role tag set to `my-cluster-manager`.

```
[2023-05-31T15:23:03,676][TRACE][o.o.d.e.AwsEc2SeedHostsProvider] [ip-172-31-73-184] finding seed nodes...
[2023-05-31T15:23:03,677][TRACE][o.o.d.e.AwsEc2SeedHostsProvider] [ip-172-31-73-184] adding i-01fa1736e8566c693, address 172.31.73.184, transport_address 172.31.73.184:9300
[2023-05-31T15:23:03,677][TRACE][o.o.d.e.AwsEc2SeedHostsProvider] [ip-172-31-73-184] adding i-03d70a4521045cc3b, address 172.31.74.169, transport_address 172.31.74.169:9300
[2023-05-31T15:23:03,677][TRACE][o.o.d.e.AwsEc2SeedHostsProvider] [ip-172-31-73-184] adding i-0c6ffdd10ebd3c2f1, address 172.31.74.156, transport_address 172.31.74.156:9300
```

## Troubleshooting

### Trace Level Logs

Enable `TRACE`-level logging in `opensearch.yml` to see more output.

```yml
logger.org.opensearch.discovery.ec2: trace
```

### Sample IMDS Query

You may need to query IMDS without running OpenSearch. Use [this sample](https://github.com/dblock/aws-imds-sample) or copy the following code that makes a query to IMDS that looks for `running`, `pending` or `stopped` instances.

```java
DefaultCredentialsProvider credentialsProviderChain = DefaultCredentialsProvider.create();

RetryPolicy retryPolicy = RetryPolicy.builder()
  .numRetries(10)
  .build();

Ec2Client client = Ec2Client.builder()
  .httpClientBuilder(ApacheHttpClient.builder())
  .credentialsProvider(credentialsProviderChain)
  .build();

DescribeInstancesRequest describeInstancesRequest = DescribeInstancesRequest.builder()
  .filters(
    Filter.builder()
      .name("instance-state-name")
      .values("running", "pending", "stopped")
      .build()
  ).build();

DescribeInstancesResponse describeInstancesResponse = client.describeInstances(describeInstancesRequest);
for (final Reservation reservation : describeInstancesResponse.reservations()) {
  System.out.println(reservation.reservationId());
  for (final Instance instance : reservation.instances()) {
    System.out.println("\t" + instance.publicDnsName());
  }
}
```
