// RPC/事件目录与版本协商契约锁。
import { describe, expect, it } from 'vitest';
import { PROTOCOL_VERSION, isCompatible, majorOf, RPC, EVT } from 'momo-hub-protocol';

describe('版本协商', () => {
  it('major 相等即兼容；不等不兼容；畸形版本号不兼容', () => {
    expect(isCompatible('1.0.0', '1.9.9')).toBe(true);
    expect(isCompatible('1.0.0', '2.0.0')).toBe(false);
    expect(isCompatible('abc', '1.0.0')).toBe(false);
    expect(majorOf('3.2.1')).toBe(3);
    expect(majorOf('x')).toBeNull();
  });
  it('PROTOCOL_VERSION 起点 1.x', () => {
    expect(majorOf(PROTOCOL_VERSION)).toBe(1);
  });
});

describe('目录常量', () => {
  it('RPC/EVT 方法名稳定（wire 契约，改名即破坏性）', () => {
    expect(RPC.hello).toBe('hello');
    expect(RPC.ping).toBe('ping');
    expect(EVT.nodeOnline).toBe('node.online');
    expect(EVT.nodeOffline).toBe('node.offline');
  });
});
