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

        import atomic
        ctx = atomic.Context()
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

        import atomic
        ctx = atomic.Context()
        result = ctx.parallelize([1, 2, 3, 4]).map(lambda x: x + 1).collect()
        # [2, 3, 4, 5]

    Example (distributed mode)::

        import os, atomic
        os.environ["VEGA_DEPLOYMENT_MODE"] = "distributed"
        os.environ["VEGA_LOCAL_IP"] = "127.0.0.1"
        ctx = atomic.Context()
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
