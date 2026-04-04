"""
Docker dispatch example using the atomic Python API.

Demonstrates PyDockerStub: partition data is JSON-encoded by the Python client
and sent to a Docker container via the 4-byte framing protocol. The container
sums its partition and returns a JSON-encoded result.

Prerequisites:
  # Build the JSON task Docker image:
  docker build -t docker-json-task:latest \
    -f examples/docker_json_task/Dockerfile .

  # Build the atomic Python wheel:
  cd crates/atomic-python && maturin develop --release && cd ../..

Run:
  python examples/python/docker_sum_job.py
"""

import struct
import atomic

ctx = atomic.Context(default_parallelism=3)

# Numbers 1..12 split across 3 partitions: [1-4], [5-8], [9-12]
data = ctx.parallelize(list(range(1, 13)), num_partitions=3)

# DockerStub resolves the image name and entrypoint from the manifest.
stub = atomic.DockerStub.from_manifest(
    "examples/docker_json_task/manifest.toml",
    "demo.sum.json.v1",
)
print(f"stub = {stub!r}")

# map_via dispatches each partition to the Docker container.
# The container sums the numbers and returns [total] (a one-element JSON array).
# execute_partitions flattens results, so partition_sums has one number per partition.
partition_sums = data.map_via(stub).collect()

print(f"partition sums = {partition_sums}")

# Each partition sum is a float (JSON numbers are f64 by default)
total = sum(partition_sums)
print(f"Sum of 1..12 = {total:.0f}")  # 78
