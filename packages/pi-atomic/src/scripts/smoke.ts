import { ctx } from "../context";

const result = ctx.parallelize([1, 2, 3]).map((x: number) => x * 2).collect();
console.log(result);
