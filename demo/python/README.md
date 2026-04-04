# Python Examples

Spark-like jobs written in Python using the `atomic` extension module
(built from `crates/atomic-python` via PyO3).

## Setup

```bash
# Install maturin if needed
pip install maturin

# Build and install the atomic Python module in dev mode
cd crates/atomic-python
maturin develop --release
cd ../..
```

## Examples

### `local_word_count.py` — Pure local job

Classic word count using `PyRdd` transformations. No Docker, no WASM —
everything runs in-process.

```bash
python examples/python/local_word_count.py
```

Expected output:
```
word                 count
----------------------------
and                  1
atomic               2
bar                  1
fast                 3
foo                  1
hello                3
is                   2
need                 1
small                1
systems              1
tools                1
world                2
```

### `docker_sum_job.py` — Docker dispatch via `PyDockerStub`

Partitions a range and dispatches each partition to a Docker container that
sums the numbers. Demonstrates how Python closures **are not sent to Docker** —
only the data crosses the wire (as JSON).

```bash
# Build the Docker image first:
docker build -t docker-json-task:latest \
  -f examples/docker_json_task/Dockerfile .

python examples/python/docker_sum_job.py
```

Expected output:
```
stub = DockerStub(operation_id="demo.sum.json.v1", image="docker-json-task:latest")
partition sums = [10.0, 26.0, 42.0]
Sum of 1..12 = 78
```

## API overview

```python
import atomic

ctx = atomic.Context(default_parallelism=4)

rdd = ctx.parallelize([1, 2, 3, 4])        # from list
rdd = ctx.range(0, 100, step=1)            # from range
rdd = ctx.text_file("/path/to/file.txt")   # line-by-line file

# Transformations (lazy, return new Rdd)
rdd.map(f)                     # apply f to each element
rdd.filter(pred)               # keep elements where pred(x) is True
rdd.flat_map(f)                # f returns iterable; results are flattened
rdd.key_by(f)                  # produce (f(x), x)
rdd.map_values(f)              # for (k, v): produce (k, f(v))
rdd.group_by(f)                # group by f(x)
rdd.group_by_key()             # group (k, v) pairs by k
rdd.reduce_by_key(f)           # aggregate values per key using f
rdd.union(other)               # concatenate two Rdds
rdd.zip(other)                 # element-wise (x, y) pairs
rdd.cartesian(other)           # all (x, y) combinations
rdd.coalesce(n)                # reduce to n partitions
rdd.repartition(n)             # change partition count

# Docker dispatch
stub = atomic.DockerStub.from_manifest(path, operation_id)
rdd.map_via(stub)              # dispatch partitions to Docker container

# Actions (eager, return a value)
rdd.collect()                  # all elements as a Python list
rdd.count()                    # number of elements
rdd.first()                    # first element
rdd.take(n)                    # first n elements
rdd.reduce(f)                  # fold with f(acc, elem)
rdd.fold(zero, f)              # fold with initial value
```
