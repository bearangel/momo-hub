// src/envelope.ts
// 消息信封——hub 与 studio 之间全部 wire 格式的最小容器。
// 纪律：结构校验严格（字段缺失/类型不符/evt 带 id 一律 null），未知字段容忍（minor 向前兼容）。
import { randomUUID } from 'node:crypto';

export const ENVELOPE_VERSION = 1 as const;
export type EnvelopeKind = 'req' | 'res' | 'evt';

export interface RequestEnvelope<T = unknown> { v: 1; kind: 'req'; id: string; method: string; payload: T }
export interface ResponseEnvelope<T = unknown> { v: 1; kind: 'res'; id: string; method: string; ok: boolean; payload: T }
export interface EventEnvelope<T = unknown> { v: 1; kind: 'evt'; method: string; payload: T }
export type Envelope<T = unknown> = RequestEnvelope<T> | ResponseEnvelope<T> | EventEnvelope<T>;

const isStr = (x: unknown): x is string => typeof x === 'string';

/** 严格结构校验：任何已知字段缺失/类型不符/非法组合返回 null；未知字段忽略。 */
export function parseEnvelope(raw: unknown): Envelope | null {
  if (typeof raw !== 'string') return null;
  let obj: unknown;
  try { obj = JSON.parse(raw); } catch { return null; }
  if (typeof obj !== 'object' || obj === null) return null;
  const e = obj as Record<string, unknown>;
  if (e.v !== ENVELOPE_VERSION) return null;
  switch (e.kind) {
    case 'req':
      if (!isStr(e.id) || e.id.length === 0 || !isStr(e.method) || e.method.length === 0) return null;
      return { v: 1, kind: 'req', id: e.id, method: e.method, payload: e.payload ?? null };
    case 'res':
      if (!isStr(e.id) || e.id.length === 0 || !isStr(e.method) || typeof e.ok !== 'boolean') return null;
      return { v: 1, kind: 'res', id: e.id, method: e.method, ok: e.ok, payload: e.payload ?? null };
    case 'evt':
      if ('id' in e || !isStr(e.method) || e.method.length === 0) return null;
      return { v: 1, kind: 'evt', method: e.method, payload: e.payload ?? null };
    default:
      return null;
  }
}

export function encodeEnvelope(e: Envelope): string {
  return JSON.stringify(e);
}

/** 构造请求信封（id 单点生成——与 momo 系「跨模块 ID 单点生成」纪律一致）。 */
export function makeRequest(method: string, payload: unknown): RequestEnvelope {
  return { v: ENVELOPE_VERSION, kind: 'req', id: randomUUID(), method, payload };
}
