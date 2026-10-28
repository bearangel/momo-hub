#!/usr/bin/env node
// momo-hub 服务入口：env 配置 → db + 身份 → listen；SIGINT/SIGTERM 优雅退出。
import { loadHubConfig, resolveListenHost } from './config';
import { openHubDatabase } from './db';
import { ensureHubIdentity } from './identity';
import { createHubServer } from './server';
import { logger } from './logger';

async function main(): Promise<void> {
  const config = loadHubConfig(process.env);
  const db = openHubDatabase(config.dataDir);
  const identity = ensureHubIdentity(db);
  const server = createHubServer({
    db, identity, dataDir: config.dataDir, publicOrigin: config.publicOrigin,
  });
  // 监听地址硬化（M3.0 T4）：默认回环；显式 0.0.0.0 / 非法值均 WARN 提示后按解析结果监听
  const listenHost = resolveListenHost(process.env.MOMO_HUB_HOST);
  if (listenHost.warn === 'wildcard') {
    logger.warn('MOMO_HUB_HOST=0.0.0.0：hub 正在监听全部网卡，同网段任意主机均可尝试接入（enrollment token 即接入凭证），请确认网络边界可信');
  } else if (listenHost.warn === 'invalid') {
    logger.warn(`非法 MOMO_HUB_HOST「${String(process.env.MOMO_HUB_HOST)}」，已回退默认 127.0.0.1（仅本机可接入）`);
  }
  const url = await server.listen(config.port, listenHost.host);
  logger.info(`momo-hub 已启动 ${url}（数据目录 ${config.dataDir}）`);
  logger.info(`hub id: ${identity.hubId}`);
  logger.info(`加入凭证（studio 首次接入用，可轮换）：${identity.enrollmentToken}`);

  let closing = false;
  const shutdown = (): void => {
    if (closing) return;
    closing = true;
    logger.info('正在关闭…');
    void server.close().finally(() => { db.close(); process.exit(0); });
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

if (require.main === module) {
  void main().catch((err: unknown) => {
    logger.error('启动失败', err instanceof Error ? err.message : String(err));
    process.exit(1);
  });
}

export { main };
