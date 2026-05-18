/**
 * Map-reduce pipeline showcase using the Atomic RDD API (TypeScript).
 *
 * Demonstrates every transformation and action: map, filter, flatMap,
 * keyBy, groupByKey, reduceByKey, union, zip, cartesian, range, aggregate,
 * distinct, take, first, count, min, max, top, forEach, and more.
 *
 * Prerequisites:
 *   cd examples/ts-demo && npm install && npm run build:native
 *
 * Run:
 *   npm run mapreduce
 */

import Atomic = require('@atomic-compute/js');

const ctx = new Atomic.Context();

// ── map / filter / collect ────────────────────────────────────────────────────
const doubled: number[] = ctx
  .parallelize([1, 2, 3, 4, 5])
  .map((x: number) => x * 2)
  .filter((x: number) => x > 4)
  .collect();
console.log('doubled (>4):', doubled); // [6, 8, 10]

// ── flatMap ───────────────────────────────────────────────────────────────────
const chars: string[] = ctx
  .parallelize(['ab', 'cd', 'ef'])
  .flatMap((s: string): string[] => s.split(''))
  .collect();
console.log('chars:', chars); // ['a','b','c','d','e','f']

// ── reduce ────────────────────────────────────────────────────────────────────
const total: number = ctx
  .parallelize([1, 2, 3, 4, 5])
  .reduce((a: number, b: number) => a + b);
console.log('sum:', total); // 15

// ── fold (with initial value) ─────────────────────────────────────────────────
const product: number = ctx
  .parallelize([1, 2, 3, 4, 5])
  .fold(1, (a: number, b: number) => a * b);
console.log('product:', product); // 120

// ── aggregate (two-phase) ─────────────────────────────────────────────────────
const aggSum: number = ctx
  .parallelize([1, 2, 3, 4], 2)
  .aggregate(0, (acc: number, x: number) => acc + x, (a: number, b: number) => a + b);
console.log('aggregate sum:', aggSum); // 10

// ── keyBy / groupByKey ────────────────────────────────────────────────────────
const grouped: Array<readonly [string, readonly string[]]> = ctx
  .parallelize(['apple', 'banana', 'avocado', 'blueberry'])
  .keyBy((s: string): string => s[0])
  .groupByKey()
  .collect();
console.log('grouped by first letter:');
[...grouped]
  .sort((a, b) => a[0].localeCompare(b[0]))
  .forEach(([letter, words]) => {
    console.log(' ', letter, '->', [...words]);
  });

// ── reduceByKey ───────────────────────────────────────────────────────────────
const counts: Array<readonly [string, number]> = ctx
  .parallelize<readonly [string, number]>([
    ['a', 1],
    ['b', 2],
    ['a', 3],
    ['b', 4],
    ['c', 5],
  ])
  .reduceByKey((a: number, b: number) => a + b)
  .collect();
console.log(
  'counts:',
  [...counts].sort((x, y) => x[0].localeCompare(y[0])),
);

// ── mapValues ─────────────────────────────────────────────────────────────────
const doubled2: Array<readonly [string, number]> = ctx
  .parallelize<readonly [string, number]>([
    ['alice', 85],
    ['bob', 72],
    ['carol', 91],
  ])
  .mapValues((score: number) => score * 2)
  .collect();
console.log('doubled scores:', [...doubled2].sort((a, b) => a[0].localeCompare(b[0])));

// ── union ─────────────────────────────────────────────────────────────────────
const rdd1 = ctx.parallelize([1, 2, 3]);
const rdd2 = ctx.parallelize([4, 5, 6]);
const merged: number[] = rdd1.union(rdd2).collect();
console.log('union:', merged); // [1,2,3,4,5,6]

// ── zip ───────────────────────────────────────────────────────────────────────
const zipped: Array<readonly [string, number]> = ctx
  .parallelize(['a', 'b', 'c'])
  .zip(ctx.parallelize([1, 2, 3]))
  .collect();
console.log('zipped:', zipped); // [['a',1],['b',2],['c',3]]

// ── distinct ─────────────────────────────────────────────────────────────────
const unique: number[] = ctx
  .parallelize([1, 2, 2, 3, 3, 3])
  .distinct()
  .collect()
  .sort((a, b) => a - b);
console.log('distinct:', unique); // [1,2,3]

// ── range ─────────────────────────────────────────────────────────────────────
const rangeSum: number = ctx
  .range(1, 11, 1)
  .reduce((a: number, b: number) => a + b);
console.log('sum 1..10:', rangeSum); // 55

// ── take / first / count ──────────────────────────────────────────────────────
const first3: number[] = ctx.parallelize([10, 20, 30, 40, 50]).take(3);
console.log('take(3):', first3); // [10,20,30]

const head: number = ctx.parallelize([99, 1, 2]).first();
console.log('first:', head); // 99

const n: number = ctx.parallelize([1, 2, 3, 4, 5]).count();
console.log('count:', n); // 5

// ── min / max / top ───────────────────────────────────────────────────────────
const nums = ctx.parallelize([3, 1, 4, 1, 5, 9, 2, 6]);
console.log('min:', nums.min()); // 1
console.log('max:', nums.max()); // 9
console.log('top(3):', nums.top(3)); // [9,6,5]

// ── isEmpty / numPartitions ───────────────────────────────────────────────────
const empty = ctx.parallelize<number>([]);
console.log('isEmpty:', empty.isEmpty()); // true
console.log('numPartitions:', ctx.parallelize([1, 2, 3, 4], 2).numPartitions); // 2
