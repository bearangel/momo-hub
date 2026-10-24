#!/usr/bin/env node
// momo-hub 服务入口：env 配置 → db + 身份 → listen；SIGINT/SIGTERM 优雅退出。
import { loadHubConfig } from './config';
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
  const url = await server.listen(config.port);
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
