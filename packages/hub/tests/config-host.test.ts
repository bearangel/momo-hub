// 监听地址解析三态契约（M3.0 T4 MOMO_HUB_HOST 硬化）：缺省回环 / 显式生效 /
// 非法回退+invalid——listen 调用方（index.ts）据 warn 打启动日志，端口行为不变。
import { describe, expect, it } from 'vitest';
import { resolveListenHost } from '../src/config';

describe('resolveListenHost 三态', () => {
  it('缺省（未设置）→ 默认 127.0.0.1，静默无警告', () => {
    expect(resolveListenHost(undefined)).toEqual({ host: '127.0.0.1', warn: 'none' });
  });

  it('显式合法 host：IPv4 / IPv6 / 主机名原样生效，无警告', () => {
    expect(resolveListenHost('192.168.1.10')).toEqual({ host: '192.168.1.10', warn: 'none' });
    expect(resolveListenHost('10.0.0.8 ')).toEqual({ host: '10.0.0.8', warn: 'none' });
    expect(resolveListenHost('::1')).toEqual({ host: '::1', warn: 'none' });
    expect(resolveListenHost('fd00::1%lo0')).toEqual({ host: 'fd00::1%lo0', warn: 'none' });
    expect(resolveListenHost('hub.internal')).toEqual({ host: 'hub.internal', warn: 'none' });
    expect(resolveListenHost('localhost')).toEqual({ host: 'localhost', warn: 'none' });
  });

  it('显式 0.0.0.0 → 生效且标记 wildcard（跨主机接入风险，启动日志 WARN）', () => {
    expect(resolveListenHost('0.0.0.0')).toEqual({ host: '0.0.0.0', warn: 'wildcard' });
  });

  it('非法值 → 回退默认 127.0.0.1 并标记 invalid', () => {
    for (const bad of [
      '',            // 空串：与 MOMO_HUB_PORT 同约定——在场即参与校验，非缺省
      '   ',         // 纯空白：trim 后为空，同样非法
      'not a host!', // 含空格与标点
      'http://hub.example.com', // URL 形态（含协议与斜杠）不是裸主机
      '-bad.example.com',       // 标签以 - 开头（RFC 1123 违例）
      '999.999.999.999',        // 点分四段形态：按 IPv4 严格判定，八位组越界 → 非法
      'a'.repeat(254),          // 超 hostname 总长 253
    ]) {
      expect(resolveListenHost(bad), `配置值：${JSON.stringify(bad)}`).toEqual({ host: '127.0.0.1', warn: 'invalid' });
    }
  });
});
