/**
 * Minimal typings for `node:async_hooks` without @types/node.
 */
declare module "node:async_hooks" {
  export interface AsyncLocalStorageOptions {
    defaultValue?: unknown;
    name?: string | undefined;
  }

  export class AsyncLocalStorage<T> {
    constructor(options?: AsyncLocalStorageOptions);
    getStore(): T | undefined;
    run<R>(store: T, callback: () => R): R;
    run<R, TArgs extends unknown[]>(store: T, callback: (...args: TArgs) => R, ...args: TArgs): R;
  }
}
