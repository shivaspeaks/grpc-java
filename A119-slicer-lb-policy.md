Title: Live Content

Description: Fetched live

Source: https://raw.githubusercontent.com/easwars/proposal/slicer/A119-slicer-lb-policy.md

---

A119: Slicer LB Policy
----
* Author: easwars
* Approver: markdroth
* Implemented in: TBD
* Last updated: 2026-07-27
* Discussion at: TDB

## Abstract

Add support for a sharding load balancing policy, that communicates with an
external sharding service to receive resource assignments. This policy should be
supported in both xDS and non-xDS based deployments.

## Background

An auto-sharding service enables client-side load balancing through the
following process:

* Dividing the keyspace into distinct, non-overlapping ranges (or slices)
* Assigning specific resources to these key ranges
* Adjusting these mappings in real-time to account for resource availability and
  fluctuating load
* Gathering load metrics for keys within an application-defined keyspace

Within this framework, application-defined keys generally consist of arbitrary
byte sequences, such as:

* Individual User IDs or Project IDs
* Tenant identifiers for multi-tenant architectures
* Identifiers created via hashing

The targets for traffic distribution, or resources, frequently include:

* Application servers
* Kubernetes pods within a cluster

Implementing a load balancing policy in gRPC that uses an auto-sharding service
has applications in various scenarios, such as:

* Enhancing request affinity in stateful environments
* Improving isolation and system resilience for multi-tenant services
* Providing the scalability required for rapid growth in AI-driven applications

### Related Proposals

* [A42: xDS Ring Hash LB Policy][A42]
* [A52: gRPC xDS Custom Load Balancer Configuration][A52]
* [A62: Pick First][A62]
* [A74: xDS Config Tears][A74]
* [A75: xDS Aggregate Cluster Behavior Fixes][A75]
* [A78: gRPC OTel Metrics for WRR, Pick First, and XdsClient][A78]
* [A81: xDS Authority Rewriting][A81]
* [A102: xDS GrpcService Support][A102]
* [A121: RPC Delay Observability][A121]
* [OSS DynamicSharding gRPC Protocol Spec](TBD)

## Proposal

Add the `slicer_experimental` LB policy in gRPC that contains the following
functionality:

* Utilizing the OSS DynamicSharding gRPC protocol for communicating with a
  sharding service and processing assignments from that service.
  * These assignments will partition an application-defined keyspace into
    distinct, non-overlapping key-ranges or slices, each associated with a set
    of server endpoints.
* Mapping client application requests to a specific key within the
  application-defined keyspace.
* Identifying the matching key-range and choosing a server endpoint assigned to
  it.
* Providing a fallback mechanism to route client traffic when assignments from
  the sharding service are unusable.

Crucially, the LB policy receives its configuration and endpoint data from the
Name Resolver and not from the sharding service.

### LB Policy Architecture

![LB Policy Architecture](A119_graphics/slicer_lb_policy_architecture.png)

The LB policy receives the following information from the Name Resolver apart
from its configuration:

* A set of endpoints where each endpoint may include an optional hostname
  attribute. If this attribute is missing, the first address associated with the
  endpoint shall serve as the hostname. The endpoint hostname attribute
  described in [gRFC A81][A81] will be used here.
* A "Channel Factory" that returns a fully functional gRPC Channel to the
  sharding service, given an opaque string specified in the configuration

The endpoints are stored in a map where the key is the hostname of the endpoint,
and the value is the state associated with the endpoint. This state includes a
`pick_first` child policy LB policy that is created lazily, and the most recent
connectivity state and picker returned by that policy. We'll call this map the
`EndpointMap` going forward. This could look something like this:

```python
class EndpointState:
  child_lb: Balancer           # Child balancer managing the endpoint
  state:    ConnectivityState  # Most recent connectivity state of the endpoint
  picker:   Picker             # Most recent picker returned by the child balancer

class EndpointMap:
  m: dict[str, EndpointState]  # Map from endpoint hostname to endpoiont state
```

The LB policy must use the injected "Channel Factory" to create a gRPC channel
to the sharding service, and must create a `Shard` stream on it. The sharding
service will send assignments on this stream. These will be stored internally in
a data structure named `LogicalAssignment`, and will contain key-ranges and
their associated endpoint names. This could look something like this:

```python
class Slice:
  start_key:  bytes      # Inclusive
  end_key:    bytes      # Exclusive, None for sentinel
  endpoints:  list[int]  # Indices into LogicalAssignment.endpoint_names

class LogicalAssignment:
  slices:         list[Slice]  # List of non-overlapping key-range slice assignments
  endpoint_names: list[str]    # Complete list of endpoint names in the assignment
  generation:     int          # Generation number of the assignment
```

`EndpointMap` and `LogicalAssignment` are combined into a data structure name
`SliceMap`, which is optimized for lookups. Given a key, it returns a matching
key-range. The `SliceMap` must be immutable, allowing the Picker to access it
without any explicit synchronization with the LB policy.

In Go, the `SliceMap` could look like this:

```python
class AssignedEndpoints:
  all_endpoints_in_slice: list[int] # Indices into SliceMap.all_endpoints
  in_fallback:            bool      # True if no valid endpoints or all in TRANSIENT_FAILURE

class SliceEntry:
  start_key:      bytes             # Inclusive start key
  endpoints_pool: AssignedEndpoints # Assigned endpoints for this slice

class SliceMap:
  slices:        list[SliceEntry]    # Sorted by start_key
  all_endpoints: list[EndpointState] # State of all endpoints across the assignment
  fallback_pool: AssignedEndpoints   # Includes all endpoints provided by resolver
  generation:    int                 # Snapshot generation number
```

Because assignments are pre-validated to have no gaps and cover the full key
range, and since `SliceMap.slices` is sorted by `start_Key`, the implementation
of `SliceMap.lookup` boils down to a binary search to find the smallest index
`i` where `SliceMap.slices[i].start_Key > key`. Once we have `i`, index `i - 1`
is what we are actually looking for. Here is a psuedo-code for it:

```python
def lookup(self, key: bytes) -> SliceEntry | None:
  # Handle the fallback-at-startup case where the policy has no assignments.
  if not self.slices:
    return None

  # Binary search for key, comparing against slice_entry.start_key.
  # Returns (idx, found):
  # - found = True  if slices[idx].start_key == key
  # - found = False if key is not an exact start_key match;
  #           idx is the insertion index (first slice where start_key > key).
  idx, found = binary_search(self.slices, key, key_func=lambda se: se.start_key)

  # Exact match on start_key ([start_key, next_start_key)).
  if found:
    return self.slices[idx]

  if idx == 0:
    return None

  # Key falls inside the range [slices[idx - 1].start_key, slices[idx].start_key).
  return self.slices[idx - 1]
```

#### Building the SliceMap

The `SliceMap` is generated from the `EndpointMap` and `LogicalAssignment` when
either of them change. Here is the pseudo-code for the logic to build the
`SliceMap`:

```python
def new_slice_map(endpoint_map: EndpointMap, assignment: LogicalAssignment | None) -> SliceMap:
  slice_map = SliceMap()

  # No logical assignment received yet. Fallback at startup.
  # Populate all_endpoints and fallback_pool directly from all resolved endpoints.
  if assignment is None:
    for state in endpoint_map.m.values():
      slice_map.all_endpoints.append(state)

    # all_indices will be [0, 1, ..., N-1] where N is the number of endpoints
    # returned by the Name Resolver.
    all_indices = list(range(len(slice_map.all_endpoints)))
    slice_map.fallback_pool = create_pool_for_indices(all_indices, slice_map.all_endpoints)
    return slice_map

  slice_map.generation = assignment.generation

  # Match endpoints in assignment with resolved endpoints in endpoint_map.
  # Slots 0..N-1 align 1:1 with assignment.endpoint_names so slice indices
  # remain valid.
  num_assigned = len(assignment.endpoint_names)
  valid_indices = [False] * num_assigned # A bit-map of size N
  fallback_indices = []

  slice_map.all_endpoints = [None] * num_assigned
  seen_in_assignment = set(assignment.endpoint_names)

  for i, name in enumerate(assignment.endpoint_names):
      if name in endpoint_map.m:
          slice_map.all_endpoints[i] = endpoint_map.m[name]
          valid_indices[i] = True
          fallback_indices.append(i)

  # Append resolver endpoints NOT mentioned in assignment to all_endpoints,
  # ensuring fallback_pool includes 100% of endpoints provided by the Name
  # Resolver.
  for name, state in endpoint_map.m.items():
      if name not in seen_in_assignment:
          fallback_indices.append(len(slice_map.all_endpoints))
          slice_map.all_endpoints.append(state)

  # Precompute fallback pool across all resolved endpoints
  slice_map.fallback_pool = create_pool_for_indices(fallback_indices, slice_map.all_endpoints)

  # Build slice entries (assumed pre-sorted by start_key in the assignment)
  for slice_data in assignment.slices:
      # Keep only endpoint indices that were found in endpoint_map
      valid_slice_endpoints = [idx for idx in slice_data.endpoints if valid_indices[idx]]

      slice_map.slices.append(SliceEntry(
          start_key      = slice_data.start_key,
          endpoints_pool = create_pool_for_indices(valid_slice_endpoints, slice_map.all_endpoints)
      ))

  return slice_map


def create_pool_for_indices(indices: list[int], all_endpoints: list[EndpointState]) -> AssignedEndpoints:
    # A pool is in fallback if it contains zero valid endpoints or if all
    # assigned endpoints are in TRANSIENT_FAILURE.
    if not indices:
        return AssignedEndpoints(all_endpoints_in_slice=[], in_fallback=True)

    all_tf = all(all_endpoints[i].state == ConnectivityState.TRANSIENT_FAILURE for i in indices)

    return AssignedEndpoints(
        all_endpoints_in_slice = indices,
        in_fallback            = all_tf
    )
```

### Fallback Mechanism

The LB policy must support a fallback mechanism that utilizes all endpoints
provided by the Name Resolver. There are two types of fallback:

* Per-slice fallback:
  * This happens when the LB policy contains valid endpoints and assignments,
    but all endpoints in the matching `SliceEntry` for an RPC are in
    `TRANSIENT_FAILURE`.
* Fallback at startup (see [section](#fallback-at-startup) for more details):
  * This happens when the following conditions are met:
    * No valid assignments have been received from the sharding service, and,
    * Initial assignment timer has expired

Key considerations here:

* The LB policy must employ the fallback mechanism only when enabled in the LB
  policy configuration.
* The LB policy must consider all available endpoints during fallback and must
  not employ any sort of subsetting.
* The LB policy must continue using previously received good assignments from
  the sharding service, if it subsequently receives a bad one or if the
  connection to the sharding service fails.

#### Fallback at Startup

Whenever the LB policy creates a new gRPC Channel to the sharding service, it
must start a timer for the duration specified by the
`initial_assignment_timeout` field in the LB policy configuration. There are two
possible scenarios here:

* If the policy contains valid assignments from the previous gRPC Channel, it
  must continue using them until it receives one from the new gRPC Channel or
  the timer expires. While the timer is pending and the LB policy is using the
  existing assignment, it must continue to process endpoint updates from the
  Name Resolver and state updates from the child LB policies as normal.
* If the policy does not contain valid assignments, it must queue RPCs until it
  receives one from the new gRPC Channel or the timer expires.

When the policy receives a valid assignment from the sharding server or the
timer expires, it must build a `SliceMap` and update the parent gRPC Channel
with a new `Picker`, which then retries any queued RPCs:

* If a valid assignment was received from the sharding service, the new `Picker`
  will this assignment for the retried RPCs.
* If the timer expired:
  * If fallback is enabled: RPCs are routed at random to all endpoints provided
    by the Name Resolver.
  * If fallback is disabled: RPCs fail until a valid assignment is received.

While RPCs are queued waiting for one of the above events to happen, the
`Picker` must set `delay_type` to "slicer_assignment_pending". See [WIP gRFC
A121][A121].

### Supported modes of operation

The LB policy must support two primary modes of operation:

* An LB policy that performs both locality and endpoint picking:
  * In xDS use-cases, such an LB policy receives endpoints across all
    localities and shards requests accordingly. This is similar to how the
    `ring_hash` LB policy, specified in [gRFC A42][A42], works.
  * In non-xDS use-cases, such an LB policy will be configured as the top-level
    LB policy, sharding requests across a flat list of endpoints provided by the
    Name Resolver.
* An LB policy that only performs endpoint picking:
  * In xDS use-cases, such an LB policy will be configured under a policy like
    `weighted_target_experimental` that handles locality picking, while each
    `slicer_experimental` child policy instance only handles endpoint picking
    within its specific locality.

The LB policy must maintain consistent behavior across both modes and must not
require explicit knowledge of its operational context. Notably, we do not
support using this LB policy solely for locality picking with delegation to a
separate endpoint-picking policy. This

