declare module "node:os" {
  export function availableParallelism(): number;
  export function cpus(): readonly unknown[];
}
