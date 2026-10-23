import { defineConfig } from 'vitest/config';

// forks 池：better-sqlite3 原生句柄 + 真实 socket 集成测试
// 在 worker 线程析构有 NAPI 竞态前科（momo-studio 同教训）。
export default defineConfig({
  test: {
    include: ['tests/**/*.test.ts'],
    pool: 'forks',
    testTimeout: 15_000,
    passWithNoTests: true,
  },
});
