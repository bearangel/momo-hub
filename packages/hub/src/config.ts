// 运行配置：环境变量 → HubConfig。
import os from 'node:os';
import path from 'node:path';

export interface HubConfig { port: number; dataDir: string }

export function loadHubConfig(env: NodeJS.ProcessEnv): HubConfig {
  const port = Number(env.MOMO_HUB_PORT ?? 8787);
  if (!Number.isInteger(port) || port < 1 || port > 65535) {
    throw new Error(`非法 MOMO_HUB_PORT: ${String(env.MOMO_HUB_PORT)}`);
  }
  return { port, dataDir: env.MOMO_HUB_DATA_DIR ?? path.join(os.homedir(), '.momo-hub') };
}
