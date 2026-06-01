from typing import Callable, Generic, Iterable, Iterator, List, Optional, Tuple, TypeVar

T = TypeVar("T")
U = TypeVar("U")
K = TypeVar("K")
V = TypeVar("V")


class Rdd(Generic[T]):
    """An in-memory distributed dataset (RDD) of Python objects.

    In local mode all transformations execute eagerly in the calling thread.
    In distributed mode transformations build a lazy pipeline that is dispatched
    to workers as a single TaskEnvelope per partition when an action is called.

    Example (local)::

        import atomic_compute
        ctx = atomic_compute.Context()
        result = ctx.parallelize([1, 2, 3, 4]).map(lambda x: x * 2).collect()
        # [2, 4, 6, 8]
    """

    # ── Transformations ──────────────────────────────────────────────────────

    def map(self, f: Callable[[T], U]) -> "Rdd[U]":
        """Apply ``f`` to each element, returning a new RDD."""
        ...

    def filter(self, f: Callable[[T], bool]) -> "Rdd[T]":
        """Keep only elements for which ``f`` returns truthy."""
        ...

    def flat_map(self, f: Callable[[T], Iterable[U]]) -> "Rdd[U]":
        """Apply ``f`` to each element and flatten the results (``f`` must return an iterable)."""
        ...

    def map_values(self, f: Callable[[V], U]) -> "Rdd[Tuple[K, U]]":
        """Apply ``f`` only to the value in each ``(key, value)`` pair."""
        ...

    def flat_map_values(self, f: Callable[[V], Iterable[U]]) -> "Rdd[Tuple[K, U]]":
        """Apply ``f`` to each value in ``(key, value)`` pairs and flatten."""
        ...

    def key_by(self, f: Callable[[T], K]) -> "Rdd[Tuple[K, T]]":
        """Produce ``(f(element), element)`` pairs."""
        ...

    def group_by(self, f: Callable[[T], K]) -> "Rdd[Tuple[K, List[T]]]":
        """Group elements by ``f(element)`` → ``(key, [elements])`` pairs."""
        ...

    def group_by_key(self) -> "Rdd[Tuple[K, List[V]]]":
        """Group ``(key, value)`` pairs by key → ``(key, [values])`` pairs."""
        ...

    def reduce_by_key(self, f: Callable[[V, V], V]) -> "Rdd[Tuple[K, V]]":
        """Aggregate values with the same key using ``f(acc, value) -> acc``."""
        ...

    def union(self, other: "Rdd[T]") -> "Rdd[T]":
        """Merge two RDDs with the same element type into one."""
        ...

    def zip(self, other: "Rdd[U]") -> "Rdd[Tuple[T, U]]":
        """Zip two RDDs element-wise into an RDD of ``(a, b)`` tuples."""
        ...

    def cartesian(self, other: "Rdd[U]") -> "Rdd[Tuple[T, U]]":
        """Compute the Cartesian product of two RDDs."""
        ...

    def coalesce(self, num_partitions: int) -> "Rdd[T]":
        """Reduce to ``num_partitions`` logical partitions."""
        ...

    def repartition(self, num_partitions: int) -> "Rdd[T]":
        """Change logical partition count (alias for ``coalesce``)."""
        ...

    # ── Actions ──────────────────────────────────────────────────────────────

    def collect(self) -> List[T]:
        """Return all elements as a Python list.

        In distributed mode with a staged pipeline, dispatches to workers and
        returns the combined results.
        """
        ...

    def count(self) -> int:
        """Return the number of elements."""
        ...

    def first(self) -> T:
        """Return the first element. Raises ``StopIteration`` if the RDD is empty."""
        ...

    def take(self, n: int) -> List[T]:
        """Return the first ``n`` elements as a Python list."""
        ...

    def reduce(self, f: Callable[[T, T], T]) -> T:
        """Aggregate all elements using ``f(acc, element) -> acc``.

        Raises ``ValueError`` if the RDD is empty.
        """
        ...

    def fold(self, zero: T, f: Callable[[T, T], T]) -> T:
        """Aggregate with an initial ``zero`` value using ``f(acc, element) -> acc``."""
        ...

    def distinct(self) -> "Rdd[T]":
        """Remove duplicate elements."""
        ...

    def subtract(self, other: "Rdd[T]") -> "Rdd[T]":
        """Return elements in ``self`` that are not in ``other``."""
        ...

    def intersection(self, other: "Rdd[T]") -> "Rdd[T]":
        """Return elements present in both ``self`` and ``other`` (no duplicates)."""
        ...

    def map_partitions(self, f: Callable[[List[T]], Iterable[U]]) -> "Rdd[U]":
        """Apply ``f`` to each logical partition (Python list), returning a flattened RDD."""
        ...

    def keys(self) -> "Rdd[K]":
        """Extract the key from each ``(key, value)`` tuple."""
        ...

    def values(self) -> "Rdd[V]":
        """Extract the value from each ``(key, value)`` tuple."""
        ...

    def lookup(self, key: K) -> List[V]:
        """Return all values associated with ``key`` in a pair RDD."""
        ...

    def for_each(self, f: Callable[[T], None]) -> None:
        """Apply ``f`` to each element for side effects."""
        ...

    def for_each_partition(self, f: Callable[[List[T]], None]) -> None:
        """Apply ``f`` to each logical partition for side effects."""
        ...

    def count_by_value(self) -> "dict[T, int]":
        """Count occurrences of each distinct element. Returns a dict keyed by items."""
        ...

    def count_by_key(self) -> "dict[K, int]":
        """Count elements per key in a pair RDD. Returns a dict keyed by key objects."""
        ...

    def is_empty(self) -> bool:
        """Return ``True`` if the RDD has no elements."""
        ...

    def max(self, key: Optional[Callable[[T], Any]] = None) -> T:
        """Return the maximum element. Optional ``key`` function for comparison."""
        ...

    def min(self, key: Optional[Callable[[T], Any]] = None) -> T:
        """Return the minimum element. Optional ``key`` function for comparison."""
        ...

    def top(self, n: int, key: Optional[Callable[[T], Any]] = None) -> List[T]:
        """Return the top ``n`` elements (largest first). Optional ``key`` function."""
        ...

    def take_ordered(self, n: int, key: Optional[Callable[[T], Any]] = None) -> List[T]:
        """Return the ``n`` smallest elements. Optional ``key`` function."""
        ...

    def save_as_text_file(self, path: str) -> None:
        """Write each element as a line to ``path``."""
        ...

    # ── New actions (Phase 1) ─────────────────────────────────────────────────

    def to_local_iterator(self) -> Iterator[T]:
        """Stream elements partition-by-partition without loading all into memory."""
        ...

    def collect_as_map(self) -> "dict[K, V]":
        """Collect a pair RDD to a dict. Last value wins on duplicate keys."""
        ...

    def to_debug_string(self) -> str:
        """Return a multi-line description of the RDD's DAG lineage."""
        ...

    def right_outer_join(self, other: "Rdd[Tuple[K, U]]") -> "Rdd[Tuple[K, Tuple[Optional[V], U]]]":
        """Right outer join: all right-side keys preserved."""
        ...

    def full_outer_join(self, other: "Rdd[Tuple[K, U]]") -> "Rdd[Tuple[K, Tuple[Optional[V], Optional[U]]]]":
        """Full outer join: all keys from both sides preserved."""
        ...

    def fold_by_key(self, zero: V, f: Callable[[V, V], V], num_partitions: int) -> "Rdd[Tuple[K, V]]":
        """Fold values for each key with an initial zero value."""
        ...

    def aggregate_by_key(
        self,
        zero: U,
        seq_fn: Callable[[U, V], U],
        comb_fn: Callable[[U, U], U],
        num_partitions: int,
    ) -> "Rdd[Tuple[K, U]]":
        """Aggregate values per key with different combiner type."""
        ...

    def subtract_by_key(self, other: "Rdd[Tuple[K, U]]") -> "Rdd[Tuple[K, V]]":
        """Return pairs whose key does NOT appear in ``other``."""
        ...

    def tree_reduce(self, f: Callable[[T, T], T], depth: int = 2) -> T:
        """Balanced tree reduce — more numerically stable than linear reduce."""
        ...

    def tree_aggregate(
        self,
        zero: U,
        seq_fn: Callable[[U, T], U],
        comb_fn: Callable[[U, U], U],
        depth: int = 2,
    ) -> U:
        """Balanced tree aggregation."""
        ...

    def count_approx(self, confidence: float) -> int:
        """Approximate count by sampling ``confidence`` fraction of partitions."""
        ...

    def aggregate(
        self,
        zero: U,
        seq_fn: Callable[[U, T], U],
        comb_fn: Callable[[U, U], U],
    ) -> U:
        """Two-phase aggregation: ``seq_fn(acc, elem)`` within partitions,
        ``comb_fn(acc, acc)`` across partition results."""
        ...

    # ── Properties ───────────────────────────────────────────────────────────

    def num_partitions(self) -> int:
        """Return the logical number of partitions."""
        ...

    def __len__(self) -> int: ...
    def __repr__(self) -> str: ...
    def __iter__(self) -> Iterator[T]: ...


class Context:
    """The Atomic execution context.

    Entry point for creating RDDs. In local mode (default) transformations run
    eagerly in the calling thread. In distributed mode (set
    ``VEGA_DEPLOYMENT_MODE=distributed``) the context connects to remote workers
    and dispatches pipeline ops over TCP.

    Example (local mode)::

        import atomic_compute
        ctx = atomic_compute.Context()
        result = ctx.parallelize([1, 2, 3, 4]).map(lambda x: x + 1).collect()
        # [2, 3, 4, 5]

    Example (distributed mode)::

        import os, atomic
        os.environ["VEGA_DEPLOYMENT_MODE"] = "distributed"
        os.environ["VEGA_LOCAL_IP"] = "127.0.0.1"
        ctx = atomic_compute.Context()
        result = ctx.parallelize(range(100), num_partitions=4).map(lambda x: x * 2).collect()
    """

    def __init__(self, default_parallelism: Optional[int] = None) -> None:
        """Create an Atomic context.

        Args:
            default_parallelism: Number of partitions used when not specified
                on individual operations. Defaults to the number of logical CPUs.
        """
        ...

    def parallelize(
        self,
        data: Iterable[T],
        num_partitions: Optional[int] = None,
    ) -> Rdd[T]:
        """Distribute a Python iterable as an RDD across ``num_partitions`` partitions.

        Args:
            data: Any Python iterable (list, generator, range, …).
            num_partitions: Number of partitions. Defaults to ``default_parallelism``.

        Example::

            rdd = ctx.parallelize([1, 2, 3, 4], num_partitions=2)
        """
        ...

    def text_file(self, path: str) -> Rdd[str]:
        """Create an RDD of lines from a text file.

        Args:
            path: Absolute or relative path to the text file.
        """
        ...

    def range(
        self,
        start: int,
        end: int,
        step: Optional[int] = None,
        num_partitions: Optional[int] = None,
    ) -> Rdd[int]:
        """Create an RDD of integers in ``[start, end)`` with optional ``step``.

        Args:
            start: Range start (inclusive).
            end: Range end (exclusive).
            step: Increment between values. Defaults to 1.
            num_partitions: Number of partitions. Defaults to ``default_parallelism``.

        Example::

            rdd = ctx.range(0, 100, step=2, num_partitions=4)
        """
        ...

    def default_parallelism(self) -> int:
        """Return the default number of partitions (CPU count or constructor value)."""
        ...

    def stop(self) -> None:
        """Stop the context and send shutdown signals to workers (distributed mode)."""
        ...

    def cancel_job(self, job_id: int) -> None:
        """Cancel a running distributed job by its run ID."""
        ...


class DataFrame:
    """Lazy result of a SQL query. Actions execute the query."""

    def collect(self) -> List[dict]: ...
    def show(self, n: int = 20) -> None: ...
    def show_limit(self, n: int) -> None: ...
    def count(self) -> int: ...
    def filter(self, expr: str) -> "DataFrame": ...
    def select(self, columns: List[str]) -> "DataFrame": ...
    def limit(self, n: int) -> "DataFrame": ...
    def sort(self, col: str, ascending: bool = True) -> "DataFrame": ...
    def schema(self) -> dict: ...
    def write_parquet(self, path: str) -> None: ...
    def write_csv(self, path: str) -> None: ...
    def to_arrow(self) -> "pyarrow.Table": ...  # requires pyarrow installed


class SqlContext:
    """SQL execution context backed by DataFusion."""

    def __init__(self) -> None: ...

    def sql(self, query: str) -> DataFrame: ...
    def register_csv(self, name: str, path: str) -> None: ...
    def register_parquet(self, name: str, path: str) -> None: ...
    def register_json(self, name: str, path: str) -> None: ...
    def deregister_table(self, name: str) -> None: ...

    def register_rdd(
        self,
        name: str,
        rdd: "Rdd",
        schema: "dict[str, str]",
    ) -> None:
        """Register a Python RDD as a SQL table.

        ``schema`` maps column names to Arrow type strings
        (``"int64"``, ``"float64"``, ``"utf8"``, …).
        """
        ...

    def register_udf(
        self,
        name: str,
        func: Callable,
        input_types: "List[str]",
        return_type: str,
    ) -> None:
        """Register a Python callable as a SQL scalar UDF."""
        ...
