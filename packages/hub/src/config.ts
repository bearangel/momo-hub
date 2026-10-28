// 运行配置：环境变量 → HubConfig。
import os from 'node:os';
import path from 'node:path';

export interface HubConfig { port: number; dataDir: string; publicOrigin?: string }

/** 公网 origin 归一（RM1-11）：必须是 http(s) 绝对形态——非法即启动报错。
 *  绝不静默降级：拼错的 origin 会让 catalog 的 downloadUrl 指向错误主机
 *  （安装链按 origin 白名单放行，错值=装到别处或全被拒）。 */
export function normalizePublicOrigin(raw: string): string {
  const trimmed = raw.trim();
  let parsed: URL;
  try {
    parsed = new URL(trimmed);
  } catch {
    throw new Error(`非法 MOMO_HUB_PUBLIC_ORIGIN（须为 http(s) 绝对地址）：${raw}`);
  }
  if (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') {
    throw new Error(`非法 MOMO_HUB_PUBLIC_ORIGIN（协议须为 http/https）：${raw}`);
  }
  if (parsed.hostname === '') {
    throw new Error(`非法 MOMO_HUB_PUBLIC_ORIGIN（缺主机名）：${raw}`);
  }
  if (parsed.username !== '' || parsed.password !== '') {
    throw new Error(`非法 MOMO_HUB_PUBLIC_ORIGIN（不得含凭证）：${raw}`);
  }
  if (parsed.search !== '' || parsed.hash !== '') {
    throw new Error(`非法 MOMO_HUB_PUBLIC_ORIGIN（不得含查询串或片段）：${raw}`);
  }
  return trimmed.replace(/\/+$/, '');
}

export function loadHubConfig(env: NodeJS.ProcessEnv): HubConfig {
  const port = Number(env.MOMO_HUB_PORT ?? 8787);
  if (!Number.isInteger(port) || port < 1 || port > 65535) {
    throw new Error(`非法 MOMO_HUB_PORT: ${String(env.MOMO_HUB_PORT)}`);
  }
  const rawOrigin = env.MOMO_HUB_PUBLIC_ORIGIN;
  const publicOrigin = rawOrigin === undefined ? undefined : normalizePublicOrigin(rawOrigin);
  return {
    port,
    dataDir: env.MOMO_HUB_DATA_DIR ?? path.join(os.homedir(), '.momo-hub'),
    ...(publicOrigin === undefined ? {} : { publicOrigin }),
  };
}

/** 监听地址解析结果：host 为最终监听地址；warn 标记启动日志级别（none=静默）。 */
export interface ListenHostResolution { host: string; warn: 'none' | 'wildcard' | 'invalid' }

const HOSTNAME_RE = /^[a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(\.[a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$/;
const IPV4_RE = /^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$/;

function isValidHost(value: string): boolean {
  if (value === '' || value.length > 253) return false;
  if (IPV4_RE.test(value)) {
    return value.split('.').every((octet) => Number(octet) <= 255);
  }
  // IPv6 松校验：含冒号，主体限 hex/冒号/点（IPv4 映射段）；区域符（%接口名，如 %lo0）单列
  if (value.includes(':')) return /^[0-9a-fA-F:.]+(%[a-zA-Z0-9]+)?$/.test(value);
  return HOSTNAME_RE.test(value);
}

/**
 * MOMO_HUB_HOST 解析（M3.0 T4 监听地址硬化）：缺省回环 127.0.0.1（默认只接受本机
 * 接入，防疏忽暴露到局域网）；显式 0.0.0.0 生效但标记 wildcard（跨主机接入风险，
 * 启动日志 WARN 提示）；非法值（语法不成立）回退默认并标记 invalid（同样 WARN）。
 * 纯函数——listen 调用方消费 host 与 warn，端口行为完全不变。
 */
export function resolveListenHost(raw: string | undefined): ListenHostResolution {
  if (raw === undefined) return { host: '127.0.0.1', warn: 'none' };
  const value = raw.trim();
  if (value === '0.0.0.0') return { host: value, warn: 'wildcard' };
  if (isValidHost(value)) return { host: value, warn: 'none' };
  return { host: '127.0.0.1', warn: 'invalid' };
}
