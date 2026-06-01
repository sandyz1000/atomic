/**
 * @atomic-compute/js — TypeScript declarations
 *
 * Spark-like distributed RDD API for Node.js, backed by Rust via napi-rs.
 * Worker-side JavaScript execution uses V8 (deno_core JsRuntime).
 *
 * @example
 * ```typescript
 * import { Context } from '@atomic-compute/js';
 *
 * const ctx = new Context();
 * const result = ctx
 *   .parallelize([1, 2, 3, 4, 5, 6, 7, 8], 4)
 *   .filter((x: number) => x % 2 === 0)
 *   .map((x: number) => x * x)
 *   .collect();
 * // [4, 16, 36, 64]
 * ```
 */

export = AtomicJs;
export as namespace AtomicJs;

declare namespace AtomicJs {

  /**
   * A distributed dataset of elements of type `T`.
   *
   * In local mode (default) all transformations execute eagerly in the Node.js thread.
   * In distributed mode (`ATOMIC_DEPLOYMENT_MODE=distributed`) transformations are staged
   * lazily and dispatched to workers as a single batch when an action is called.
   *
   * Key-value operations (`reduceByKey`, `groupByKey`, `mapValues`, `flatMapValues`) are
   * typed with conditional `this` types — they are only callable when the element type
   * is a `readonly [K, V]` tuple.
   */
  class RDD<T> {
    // ── Transformations ──────────────────────────────────────────────────────

    /**
     * Apply `f` to each element, returning a new RDD of the transformed type.
     *
     * @example
     * ```typescript
     * ctx.parallelize([1, 2, 3]).map(x => x * 2)  // RDD<number>
     * ctx.parallelize(['a', 'b']).map(s => s.length)  // RDD<number>
     * ```
     */
    map<U>(f: (x: T) => U): RDD<U>;

    /**
     * Keep only elements for which `predicate` returns truthy.
     */
    filter(predicate: (x: T) => boolean): RDD<T>;

    /**
     * Apply `f` to each element and flatten the resulting arrays.
     *
     * @example
     * ```typescript
     * ctx.parallelize(['hello world', 'foo bar'])
     *   .flatMap(line => line.split(' '))  // RDD<string>
     * ```
     */
    flatMap<U>(f: (x: T) => U[]): RDD<U>;

    /**
     * Apply `f` to the value in each `[key, value]` pair, keeping the key unchanged.
     * Only callable on `RDD<readonly [K, V]>`.
     *
     * @example
     * ```typescript
     * const pairs: RDD<readonly [string, number]> = ...;
     * pairs.mapValues(v => v * 2)  // RDD<readonly [string, number]>
     * ```
     */
    mapValues<K, V, W>(
      this: RDD<readonly [K, V]>,
      f: (value: V) => W,
    ): RDD<readonly [K, W]>;

    /**
     * Apply `f` to the value in each `[key, value]` pair and flatten, keeping the key.
     * Only callable on `RDD<readonly [K, V]>`.
     */
    flatMapValues<K, V, W>(
      this: RDD<readonly [K, V]>,
      f: (value: V) => W[],
    ): RDD<readonly [K, W]>;

    /**
     * Produce `[f(x), x]` pairs — attaches a key to each element.
     *
     * @example
     * ```typescript
     * ctx.parallelize(['apple', 'banana', 'cherry'])
     *   .keyBy(s => s.length)  // RDD<readonly [number, string]>
     * ```
     */
    keyBy<K>(f: (x: T) => K): RDD<readonly [K, T]>;

    /**
     * Group elements by `f(element)` → `[key, elements[]]` pairs.
     *
     * @example
     * ```typescript
     * ctx.parallelize([1, 2, 3, 4])
     *   .groupBy(x => x % 2 === 0 ? 'even' : 'odd')
     *   // RDD<readonly [string, readonly number[]]>
     * ```
     */
    groupBy<K>(f: (x: T) => K): RDD<readonly [K, readonly T[]]>;

    /**
     * Group `[key, value]` pairs by key → `[key, values[]]` pairs.
     * Only callable on `RDD<readonly [K, V]>`.
     *
     * @example
     * ```typescript
     * const pairs: RDD<readonly [string, number]> = ...;
     * pairs.groupByKey()  // RDD<readonly [string, readonly number[]]>
     * ```
     */
    groupByKey<K, V>(
      this: RDD<readonly [K, V]>,
    ): RDD<readonly [K, readonly V[]]>;

    /**
     * Aggregate values with the same key using `f(accumulator, value) => accumulator`.
     * Only callable on `RDD<readonly [K, V]>`.
     *
     * @example
     * ```typescript
     * ctx.parallelize([['a', 1], ['b', 2], ['a', 3]] as const)
     *   .reduceByKey((a, b) => a + b)
     *   // RDD<readonly [string, number]>
     *   // [['a', 4], ['b', 2]]
     * ```
     */
    reduceByKey<K, V>(
      this: RDD<readonly [K, V]>,
      f: (accumulator: V, value: V) => V,
    ): RDD<readonly [K, V]>;

    /**
     * Merge two RDDs of the same type into one.
     */
    union(other: RDD<T>): RDD<T>;

    /**
     * Zip two equal-length RDDs element-wise into `[a, b]` pairs.
     * Throws if the RDDs have different lengths.
     */
    zip<U>(other: RDD<U>): RDD<readonly [T, U]>;

    /**
     * Compute the Cartesian product of two RDDs as `[a, b]` pairs.
     * Result has `this.length × other.length` elements.
     */
    cartesian<U>(other: RDD<U>): RDD<readonly [T, U]>;

    /**
     * Reduce to `n` logical partitions (no data movement in local mode).
     */
    coalesce(n: number): RDD<T>;

    /**
     * Change partition count (alias for `coalesce`).
     */
    repartition(n: number): RDD<T>;

    /**
     * Remove duplicate elements from the RDD.
     */
    distinct(): RDD<T>;

    /**
     * Return elements in this RDD that are not in `other`.
     */
    subtract(other: RDD<T>): RDD<T>;

    /**
     * Return elements present in both this RDD and `other` (no duplicates).
     */
    intersection(other: RDD<T>): RDD<T>;

    /**
     * Apply `f` to each logical partition (an array of elements) and flatten the results.
     *
     * @example
     * ```typescript
     * ctx.parallelize([1, 2, 3, 4], 2)
     *   .mapPartitions(partition => partition.map(x => x * 2))
     *   // [2, 4, 6, 8]
     * ```
     */
    mapPartitions<U>(f: (partition: T[]) => U[]): RDD<U>;

    /**
     * Extract the key from each `[key, value]` pair.
     * Only meaningful on pair RDDs.
     */
    keys<K, V>(this: RDD<readonly [K, V]>): RDD<K>;

    /**
     * Extract the value from each `[key, value]` pair.
     * Only meaningful on pair RDDs.
     */
    values<K, V>(this: RDD<readonly [K, V]>): RDD<V>;

    /**
     * Return all values associated with `key` in a pair RDD.
     * Only meaningful on pair RDDs.
     */
    lookup<K, V>(this: RDD<readonly [K, V]>, key: K): V[];

    // ── Actions ──────────────────────────────────────────────────────────────

    /**
     * Return all elements as an array.
     * In distributed mode this triggers dispatch to workers.
     */
    collect(): T[];

    /**
     * Return the number of elements.
     * In distributed mode this triggers dispatch to workers.
     */
    count(): number;

    /**
     * Return the first element. Throws if the RDD is empty.
     */
    first(): T;

    /**
     * Return the first `n` elements.
     */
    take(n: number): T[];

    /**
     * Aggregate all elements using `f(accumulator, element) => accumulator`.
     * In distributed mode each partition is reduced independently, then the per-partition
     * results are merged on the driver using the same function.
     * Throws if the RDD is empty.
     */
    reduce(f: (accumulator: T, element: T) => T): T;

    /**
     * Aggregate with an initial value using `f(accumulator, element) => accumulator`.
     * The zero value must be JSON-serializable.
     */
    fold(zero: T, f: (accumulator: T, element: T) => T): T;

    /**
     * Apply `f` to each element for side effects. Returns void.
     */
    forEach(f: (x: T) => unknown): void;

    /**
     * Apply `f` to each logical partition for side effects. Returns void.
     */
    forEachPartition(f: (partition: T[]) => unknown): void;

    /**
     * Count occurrences of each distinct element.
     * Returns a `Record<string, number>` where keys are JSON-stringified elements.
     */
    countByValue(): Record<string, number>;

    /**
     * Count the number of elements per key in a pair RDD.
     * Returns a `Record<string, number>`.
     * Only meaningful on pair RDDs.
     */
    countByKey<K, V>(this: RDD<readonly [K, V]>): Record<string, number>;

    /**
     * Return `true` if the RDD has no elements.
     */
    isEmpty(): boolean;

    /**
     * Return the maximum element. Without a comparator, uses natural JSON ordering
     * (numeric for numbers, lexicographic for strings).
     *
     * @param comparator - Optional `f(a, b) => number`: positive means `a > b`.
     */
    max(comparator?: (a: T, b: T) => number): T;

    /**
     * Return the minimum element. Without a comparator, uses natural JSON ordering.
     *
     * @param comparator - Optional `f(a, b) => number`: negative means `a < b`.
     */
    min(comparator?: (a: T, b: T) => number): T;

    /**
     * Return the top `n` elements (largest first).
     *
     * @param n - Number of elements to return.
     * @param comparator - Optional `f(a, b) => number`: positive means `a > b`.
     */
    top(n: number, comparator?: (a: T, b: T) => number): T[];

    /**
     * Return the `n` smallest elements (ascending order).
     *
     * @param n - Number of elements to return.
     * @param comparator - Optional `f(a, b) => number`: positive means `a > b`.
     */
    takeOrdered(n: number, comparator?: (a: T, b: T) => number): T[];

    /**
     * Write each element as a line to the file at `path`.
     * String elements are written as-is; others are JSON-stringified.
     */
    saveAsTextFile(path: string): void;

    /**
     * Two-phase aggregation across partitions.
     *
     * - `seqFn(acc, elem) => acc` reduces elements within a partition.
     * - `combFn(acc, acc) => acc` combines per-partition results on the driver.
     *
     * @param zero - Initial accumulator value (must be JSON-serializable).
     * @param seqFn - Per-element reducer within a partition.
     * @param combFn - Combines two partition accumulators.
     *
     * @example
     * ```typescript
     * const sum = ctx.parallelize([1, 2, 3, 4], 2)
     *   .aggregate(0, (acc, x) => acc + x, (a, b) => a + b);
     * // 10
     * ```
     */
    aggregate<U>(zero: U, seqFn: (acc: U, x: T) => U, combFn: (a: U, b: U) => U): U;

    // ── Properties ───────────────────────────────────────────────────────────

    /**
     * Number of logical partitions.
     */
    readonly numPartitions: number;
  }

  /**
   * The Atomic execution context for JavaScript / TypeScript.
   *
   * Create one context per program. It initialises the scheduler (local thread-pool
   * or distributed TCP, depending on `ATOMIC_DEPLOYMENT_MODE`) and manages the
   * worker connection pool.
   *
   * @example
   * ```typescript
   * import { Context } from '@atomic-compute/js';
   *
   * const ctx = new Context();   // local mode, parallelism = CPU count
   * const ctx2 = new Context(8); // local mode, 8 partitions by default
   * ```
   */
  class Context {
    /**
     * Create a new Atomic context.
     *
     * @param defaultParallelism - Default number of partitions used when `numPartitions`
     *   is not specified on an operation. Defaults to the logical CPU count.
     */
    constructor(defaultParallelism?: number);

    /**
     * Distribute a JavaScript array as an RDD.
     *
     * The array must contain only JSON-serializable values (numbers, strings, booleans,
     * arrays, plain objects). Functions, `undefined`, `Symbol`, and `BigInt` are not
     * supported.
     *
     * @param data - Any array of JSON-serializable values.
     * @param numPartitions - Number of partitions. Defaults to `defaultParallelism`.
     *
     * @example
     * ```typescript
     * ctx.parallelize([1, 2, 3, 4], 2)  // RDD<number>, 2 partitions
     * ctx.parallelize(['hello', 'world'])  // RDD<string>
     * ```
     */
    parallelize<T>(data: T[], numPartitions?: number): RDD<T>;

    /**
     * Create an RDD of lines from a text file (one string per line).
     *
     * @param path - Absolute or relative path to the file.
     */
    textFile(path: string): RDD<string>;

    /**
     * Create an RDD of integers in `[start, end)` with optional step.
     *
     * @param start - Range start (inclusive).
     * @param end - Range end (exclusive).
     * @param step - Increment per element (default `1`). Must not be `0`.
     * @param numPartitions - Number of partitions. Defaults to `defaultParallelism`.
     *
     * @example
     * ```typescript
     * ctx.range(0, 10)         // RDD<number>: [0,1,2,3,4,5,6,7,8,9]
     * ctx.range(0, 10, 2)      // RDD<number>: [0,2,4,6,8]
     * ctx.range(0, 100, 1, 4)  // 4 partitions
     * ```
     */
    range(start: number, end: number, step?: number, numPartitions?: number): RDD<number>;

    /**
     * Return the default number of partitions (CPU count or constructor value).
     */
    defaultParallelism(): number;

    /**
     * Broadcast a JSON-serializable value to all tasks.
     *
     * The returned `BroadcastVar` can be passed into `map`, `forEach`, etc.
     * and its `.value()` method returns the deserialized copy.
     *
     * @param value - Any JSON-serializable value.
     */
    broadcast(value: unknown): BroadcastVar;

    /**
     * Create an accumulator initialized to `zero`.
     *
     * The accumulator supports numeric addition (`number`), array append (`array`),
     * and string concatenation (`string`) via `.add(delta)`.
     *
     * @param zero - Initial value (number, array, or string).
     */
    accumulator(zero: unknown): Accumulator;
  }

  /**
   * A value broadcast to all tasks.  Read the value via `.value()`.
   */
  class BroadcastVar {
    /**
     * Deserialize and return the broadcast value.
     */
    value(): unknown;
  }

  /**
   * A mutable accumulator that can be updated from within RDD operations.
   *
   * Supports numeric addition, array append, and string concatenation.
   */
  class Accumulator {
    /**
     * Add `delta` to the accumulator.
     *
     * - `number + number` → numeric addition.
     * - `array + array` → concatenation.
     * - `array + single` → append.
     * - `string + string` → concatenation.
     */
    add(delta: unknown): void;

    /** Return the current accumulated value. */
    value(): unknown;

    /** Reset the accumulator to its initial value. */
    reset(): void;
  }

  /**
   * A directed graph with `f64` vertex and edge attributes.
   *
   * Vertex IDs are integers; returned record keys are their string representations.
   */
  class Graph {
    /**
     * Create a graph from vertex and edge lists.
     *
     * @param vertices - Array of `[vertexId, weight]` pairs.
     * @param edges    - Array of `[srcId, dstId, weight]` triples.
     */
    constructor(
      vertices: [number, number][],
      edges: [number, number, number][]
    );

    /** Number of vertices. */
    numVertices(): number;

    /** Number of edges. */
    numEdges(): number;

    /**
     * Run fixed-iteration PageRank.
     *
     * @param numIter   - Number of iterations.
     * @param resetProb - Teleportation probability (typically 0.15).
     * @returns Map from vertex ID (as string) to PageRank score.
     */
    pageRank(numIter: number, resetProb: number): Record<string, number>;

    /**
     * Compute weakly connected components.
     *
     * @returns Map from vertex ID (as string) to component representative ID.
     */
    connectedComponents(): Record<string, number>;

    /**
     * Compute strongly connected components (Tarjan's algorithm).
     *
     * @returns Map from vertex ID (as string) to SCC representative ID.
     */
    stronglyConnectedComponents(): Record<string, number>;

    /**
     * Label propagation community detection.
     *
     * @param maxIter - Maximum number of supersteps.
     * @returns Map from vertex ID (as string) to community label.
     */
    labelPropagation(maxIter: number): Record<string, number>;

    /**
     * Count triangles per vertex.
     *
     * @returns Map from vertex ID (as string) to triangle count.
     */
    triangleCount(): Record<string, number>;

    /**
     * Compute shortest-path distances from landmark vertices.
     *
     * @param landmarks - Array of source vertex IDs.
     * @returns Map from vertex ID (as string) to map of landmark ID → distance.
     */
    shortestPath(landmarks: number[]): Record<string, Record<string, number>>;
  }
}
