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
