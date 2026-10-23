// 信封编解码契约锁：round-trip + 全部畸形输入拒绝。
import { describe, expect, it } from 'vitest';
import {
  encodeEnvelope, parseEnvelope, makeRequest,
  type EventEnvelope, type ResponseEnvelope,
} from 'momo-hub-protocol';

describe('信封编解码', () => {
  it('req/res/evt round-trip 保持结构', () => {
    const req = makeRequest('hello', { protoVer: '1.0.0' });
    expect(parseEnvelope(encodeEnvelope(req))).toEqual(req);

    const res: ResponseEnvelope<{ ok2: true }> = { v: 1, kind: 'res', id: req.id, method: 'hello', ok: true, payload: { ok2: true } };
    expect(parseEnvelope(encodeEnvelope(res))).toEqual(res);

    const evt: EventEnvelope<{ a: 1 }> = { v: 1, kind: 'evt', method: 'node.online', payload: { a: 1 } };
    expect(parseEnvelope(encodeEnvelope(evt))).toEqual(evt);
  });

  it('畸形输入一律 null（非 JSON / 缺字段 / 错类型 / 错版本号 / evt 带 id / 未知 kind）', () => {
    expect(parseEnvelope('not json')).toBeNull();
    expect(parseEnvelope(null)).toBeNull();
    expect(parseEnvelope('{"v":1,"kind":"req","method":"hello","payload":{}}')).toBeNull(); // 缺 id
    expect(parseEnvelope('{"v":1,"kind":"req","id":"x","method":123,"payload":{}}')).toBeNull(); // method 非字符串
    expect(parseEnvelope('{"v":2,"kind":"req","id":"x","method":"m","payload":{}}')).toBeNull(); // v≠1
    expect(parseEnvelope('{"v":1,"kind":"res","id":"x","method":"m","payload":{}}')).toBeNull(); // res 缺 ok
    expect(parseEnvelope('{"v":1,"kind":"evt","id":"x","method":"m","payload":{}}')).toBeNull(); // evt 不允许 id
    expect(parseEnvelope('{"v":1,"kind":"other","id":"x","method":"m","payload":{}}')).toBeNull(); // 未知 kind
  });

  it('makeRequest 每次生成唯一 id', () => {
    expect(makeRequest('a', {}).id).not.toBe(makeRequest('a', {}).id);
  });
});
