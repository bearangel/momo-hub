// 极简分级日志——零依赖（对齐 momo-studio 自研 logger 的取舍）。
const ts = (): string => new Date().toISOString();
export const logger = {
  info: (msg: string, meta?: unknown) => console.log(`[INFO ${ts()}] ${msg}`, meta ?? ''),
  warn: (msg: string, meta?: unknown) => console.warn(`[WARN ${ts()}] ${msg}`, meta ?? ''),
  error: (msg: string, meta?: unknown) => console.error(`[ERR ${ts()}] ${msg}`, meta ?? ''),
};
