#!/usr/bin/env node
// momo-hub 便携包（Windows x64）标准化构建 + 门禁脚本
//
// 背景（2026-11-17 事故）：便携包 app/package.json 以 `file:./momo-hub-protocol`
// 声明协议依赖，包管理器会把它装成 `app/node_modules/momo-hub-protocol` 的**独立副本**。
// 手工改包时若只替换 `app/momo-hub-protocol`（file: 源目录），Node 解析
// `require('momo-hub-protocol')` 会**优先命中 node_modules 里的旧副本** ——
// 于是「源码是协议 1.12.0、发布包里却是 1.10.0」的静默错配出现，跨节点能力全被版本门拒。
//
// 本脚本因此不做「在上一版包上打补丁」，而是：
//   1) 从源码重建协议 dist + hub dist；
//   2) 干净暂存目录里用 pnpm hoisted 模式重装生产依赖（node_modules 为实体目录，可直接打包）；
//   3) 六道门禁：版本一致 / 三份协议副本同版本且为实体目录 / 原生绑定实载 / 真机 hello 握手 / 落地包复验；
//   4) 任一门禁失败即退出非零，且不产出 zip。
//
// 用法：
//   node scripts/build-portable.mjs                    # 全量构建 + 门禁
//   node scripts/build-portable.mjs --no-build         # 跳过 pnpm build（复用既有 dist）
//   node scripts/build-portable.mjs --out D:\some\dir  # 自定义输出目录（默认 dist-release）
//   node scripts/build-portable.mjs --node C:\nodejs\node.exe
//   node scripts/build-portable.mjs --keep-extract     # 保留解压校验目录（排查用）
//
// 退出码：0 成功；1 构建/门禁失败。

import { spawn, spawnSync } from 'node:child_process';
import crypto from 'node:crypto';
import fs from 'node:fs';
import http from 'node:http';
import net from 'node:net';
import os from 'node:os';
import path from 'node:path';
import { createRequire } from 'node:module';
import { fileURLToPath } from 'node:url';

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');

// ─── 参数 ────────────────────────────────────────────────────────────────────
const argv = process.argv.slice(2);
const arg = (name) => {
  const i = argv.indexOf(name);
  return i >= 0 ? argv[i + 1] : undefined;
};
const opts = {
  out: arg('--out') ?? path.join(repoRoot, 'dist-release'),
  nodeExe: arg('--node'),
  skipBuild: argv.includes('--no-build'),
  keepExtract: argv.includes('--keep-extract'),
};

// ─── 基础工具 ────────────────────────────────────────────────────────────────
const log = (msg) => console.log(`[build-portable] ${msg}`);
const die = (msg) => {
  console.error(`\n[build-portable] ✗ ${msg}`);
  process.exit(1);
};
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/** 同步执行；shell 默认关闭（Windows 下 npx 用 npx.cmd 显式指定即可，避免参数拼接风险） */
function run(cmd, args, cwd, useShell = false) {
  const r = spawnSync(cmd, args, { cwd, stdio: 'inherit', shell: useShell });
  if (r.status !== 0) die(`命令失败（exit ${r.status}）：${cmd} ${args.join(' ')}`);
}

const NPX = process.platform === 'win32' ? 'npx.cmd' : 'npx';

/**
 * 执行 npx 命令。Windows 下 npx 是 .cmd，Node 18+ 禁止无 shell 直接 spawn .cmd（EINVAL），
 * 而 `shell:true + args 数组` 会触发 DEP0190 且有参数拼接风险 —— 故显式经 cmd.exe /c 转发。
 */
function runNpx(args, cwd) {
  const cmdline = ['npx', ...args].join(' ');
  const r =
    process.platform === 'win32'
      ? spawnSync('cmd.exe', ['/d', '/s', '/c', cmdline], { cwd, stdio: 'inherit' })
      : spawnSync('npx', args, { cwd, stdio: 'inherit' });
  if (r.status !== 0) die(`命令失败（exit ${r.status}）：${cmdline}`);
}

/** 生成 zip：优先用 Windows 自带 bsdtar（`tar -a`），避免 PowerShell Archive 模块的版本坑 */
function createZip(srcDir, zipPath) {
  const entries = fs.readdirSync(srcDir);
  run('tar', ['-a', '-c', '-f', zipPath, ...entries], srcDir);
}

/** 解压 zip（同一套 tar，保证与生成端一致） */
function extractZip(zipPath, destDir) {
  fs.mkdirSync(destDir, { recursive: true });
  run('tar', ['-x', '-f', zipPath, '-C', destDir], path.dirname(zipPath));
}

const readJson = (file) => JSON.parse(fs.readFileSync(file, 'utf8'));

/** 协议版本唯一真相源：协议包 dist/version.js 的 PROTOCOL_VERSION */
function protocolVersionOf(pkgDir) {
  const file = path.join(pkgDir, 'dist', 'version.js');
  if (!fs.existsSync(file)) die(`找不到协议版本文件：${file}`);
  const m = fs.readFileSync(file, 'utf8').match(/PROTOCOL_VERSION = '([^']+)'/);
  if (!m) die(`无法从 ${file} 解析 PROTOCOL_VERSION`);
  return m[1];
}

/** 实体目录门禁：symlink/junction 在 zip 解压后会断，必须拦在打包前 */
function assertRealDir(dir, label) {
  if (!fs.existsSync(dir)) die(`${label} 不存在：${dir}`);
  if (fs.lstatSync(dir).isSymbolicLink()) die(`${label} 是链接而非实体目录（解压后会断）：${dir}`);
  if (!fs.statSync(dir).isDirectory()) die(`${label} 不是目录：${dir}`);
}

function copyDir(src, dst) {
  fs.mkdirSync(dst, { recursive: true });
  for (const entry of fs.readdirSync(src, { withFileTypes: true })) {
    const s = path.join(src, entry.name);
    const d = path.join(dst, entry.name);
    if (entry.isDirectory()) copyDir(s, d);
    else fs.copyFileSync(s, d);
  }
}

function freePort() {
  return new Promise((resolve, reject) => {
    const srv = net.createServer();
    srv.unref();
    srv.on('error', reject);
    srv.listen(0, '127.0.0.1', () => {
      const { port } = srv.address();
      srv.close(() => resolve(port));
    });
  });
}

function httpJson(port, pathname, payload) {
  return new Promise((resolve, reject) => {
    const data = payload === undefined ? null : JSON.stringify(payload);
    const req = http.request(
      {
        host: '127.0.0.1',
        port,
        path: pathname,
        method: payload === undefined ? 'GET' : 'POST',
        headers: data ? { 'content-type': 'application/json', 'content-length': Buffer.byteLength(data) } : {},
        timeout: 5000,
      },
      (res) => {
        let body = '';
        res.on('data', (c) => (body += c));
        res.on('end', () => resolve({ status: res.statusCode, body }));
      },
    );
    req.on('timeout', () => req.destroy(new Error('http timeout')));
    req.on('error', reject);
    if (data) req.write(data);
    req.end();
  });
}

/** 异步 spawn，收集输出，便于失败时打印 */
function spawnAsync(cmd, args, options) {
  const child = spawn(cmd, args, { ...options, stdio: ['ignore', 'pipe', 'pipe'] });
  child.out = '';
  child.err = '';
  child.stdout.setEncoding('utf8');
  child.stderr.setEncoding('utf8');
  child.stdout.on('data', (d) => (child.out += d));
  child.stderr.on('data', (d) => (child.err += d));
  return child;
}

function stopChild(child) {
  try {
    child.kill('SIGTERM');
  } catch {
    /* ignore */
  }
}

/** 等子进程真正退出（Windows 上句柄未释放就删目录会 EPERM） */
async function waitExit(child, timeoutMs = 5000) {
  const deadline = Date.now() + timeoutMs;
  while (child.exitCode === null && Date.now() < deadline) await sleep(100);
}

/** 删除目录：带重试；清理失败只告警，不让构建因收尾问题失败 */
function rmrf(dir, label) {
  if (!fs.existsSync(dir)) return;
  try {
    fs.rmSync(dir, { recursive: true, force: true, maxRetries: 10, retryDelay: 200 });
  } catch (e) {
    log(`⚠ 清理失败（可手动删除）：${label ?? dir} — ${e.code}`);
  }
}

/** 异步版删除（子进程刚退出时 Windows 句柄释放有延迟，需要留重试窗口） */
async function rmrfAsync(dir, label) {
  for (let i = 0; i < 20; i += 1) {
    if (!fs.existsSync(dir)) return;
    try {
      fs.rmSync(dir, { recursive: true, force: true, maxRetries: 5, retryDelay: 200 });
      return;
    } catch {
      await sleep(700);
    }
  }
  log(`⚠ 清理失败（可手动删除）：${label ?? dir}`);
}

/**
 * 裁剪 better-sqlite3 的原生编译中间产物（源码编译后 build/ 可达 45MB，占安装包近一半）。
 * 运行期只需要 lib/ + build/Release/better_sqlite3.node —— 裁剪后由后续门禁实载验证。
 */
function pruneBetterSqlite3(appDir) {
  const pkgDir = path.join(appDir, 'node_modules', 'better-sqlite3');
  if (!fs.existsSync(pkgDir)) die('better-sqlite3 未安装，无法裁剪');
  const keep = [];
  const walk = (dir) => {
    for (const e of fs.readdirSync(dir, { withFileTypes: true })) {
      const p = path.join(dir, e.name);
      if (e.isDirectory()) walk(p);
      else if (e.name.endsWith('.node')) keep.push(p);
    }
  };
  walk(pkgDir);
  const mainBinding = keep.find((p) => path.basename(p) === 'better_sqlite3.node');
  if (!mainBinding) die('better-sqlite3 内未找到 better_sqlite3.node（编译失败？）');
  const buffer = fs.readFileSync(mainBinding);
  for (const sub of ['build', 'obj', 'out', 'deps', 'src', 'prebuilds']) {
    fs.rmSync(path.join(pkgDir, sub), { recursive: true, force: true });
  }
  for (const f of fs.readdirSync(pkgDir)) {
    if (/\.(vcxproj|sln|recipe|lastbuildstate)$/.test(f) || f === 'binding.gyp' || f === 'config.gypi') {
      fs.rmSync(path.join(pkgDir, f), { force: true });
    }
  }
  const releaseDir = path.join(pkgDir, 'build', 'Release');
  fs.mkdirSync(releaseDir, { recursive: true });
  fs.writeFileSync(path.join(releaseDir, 'better_sqlite3.node'), buffer);
  log(`✓ 裁剪 better-sqlite3 编译中间产物（保留 binding ${(buffer.length / 1024 / 1024).toFixed(2)} MB）`);
}

async function waitHealth(port, timeoutMs = 30000) {
  const deadline = Date.now() + timeoutMs;
  let lastErr;
  while (Date.now() < deadline) {
    try {
      const r = await httpJson(port, '/health');
      if (r.status === 200) return JSON.parse(r.body);
    } catch (e) {
      lastErr = e;
    }
    await sleep(200);
  }
  throw new Error(`hub 未在 ${timeoutMs}ms 内就绪（${lastErr?.message ?? 'no error'}）`);
}

// ─── 门禁：真机引导 + 真实 WS hello 握手 ─────────────────────────────────────
async function handshakeGate(pkgDir, expectedProtocol, label) {
  const nodeExe = path.join(pkgDir, 'node.exe');
  const dataDir = path.join(os.tmpdir(), `momo-hub-data-${process.pid}-${Date.now()}`);
  fs.mkdirSync(dataDir, { recursive: true });
  const port = await freePort();
  const child = spawnAsync(nodeExe, [path.join(pkgDir, 'app', 'dist', 'index.js')], {
    cwd: pkgDir,
    env: {
      ...process.env,
      MOMO_HUB_HOST: '127.0.0.1',
      MOMO_HUB_PORT: String(port),
      MOMO_HUB_DATA_DIR: dataDir,
    },
  });
  try {
    const health = await waitHealth(port);
    if (!health.ok) die(`${label}：/health 返回异常 ${JSON.stringify(health)}`);

    // 接入凭证明文存 hub_meta（读库比解析控制台输出稳，且控制台是系统代码页）
    const requireApp = createRequire(path.join(pkgDir, 'app', 'package.json'));
    const Database = requireApp('better-sqlite3');
    const db = new Database(path.join(dataDir, 'hub.db'), { readonly: true });
    const row = db.prepare("SELECT value FROM hub_meta WHERE key = 'enrollment_token'").get();
    db.close();
    if (!row?.value) die(`${label}：hub_meta 内无 enrollment_token`);

    const enrolled = await httpJson(port, '/enroll', { enrollmentToken: row.value, nodeName: `probe-${Date.now()}` });
    if (enrolled.status !== 200) die(`${label}：/enroll 失败 ${enrolled.status} ${enrolled.body}`);
    const { deviceToken } = JSON.parse(enrolled.body);

    const WebSocket = requireApp('ws');
    const { RPC, encodeEnvelope } = requireApp('momo-hub-protocol');
    const protoVer = await new Promise((resolve, reject) => {
      const ws = new WebSocket(`ws://127.0.0.1:${port}/ws`, {
        headers: { authorization: `Bearer ${deviceToken}` },
      });
      const timer = setTimeout(() => {
        ws.terminate();
        reject(new Error('hello 超时'));
      }, 10000);
      ws.on('open', () => {
        ws.send(
          encodeEnvelope({
            v: 1,
            kind: 'req',
            id: 'gate-hello',
            method: RPC.hello,
            payload: { protoVer: expectedProtocol },
          }),
        );
      });
      ws.on('message', (raw) => {
        const env = JSON.parse(String(raw));
        if (env.kind === 'res' && env.method === RPC.hello) {
          clearTimeout(timer);
          ws.close();
          if (env.ok) resolve(env.payload.protoVer);
          else reject(new Error(`hello 被拒：${JSON.stringify(env.payload)}`));
        }
      });
      ws.on('error', (e) => {
        clearTimeout(timer);
        reject(e);
      });
    });

    if (protoVer !== expectedProtocol) {
      die(`${label}：hub 宣告 protoVer=${protoVer}，期望 ${expectedProtocol}（协议副本陈旧？）`);
    }
    log(`✓ ${label}：/health ok，WS hello 宣告 protoVer=${protoVer}`);
  } finally {
    stopChild(child);
    await waitExit(child);
    await sleep(500);
    rmrf(dataDir, '握手数据目录');
  }
}

// ─── 主流程 ──────────────────────────────────────────────────────────────────
const rootPkg = readJson(path.join(repoRoot, 'package.json'));
const hubPkg = readJson(path.join(repoRoot, 'packages', 'hub', 'package.json'));
if (rootPkg.version !== hubPkg.version) {
  die(`版本不一致：根 package.json=${rootPkg.version}、packages/hub=${hubPkg.version}（先对齐再打包）`);
}
const version = rootPkg.version;
const pkgName = `momo-hub-${version}-win-x64`;
const zipPath = path.join(opts.out, `${pkgName}.zip`);
const stageRoot = path.join(os.tmpdir(), `momo-hub-stage-${process.pid}`);
const appDir = path.join(stageRoot, 'app');
const nodeExe = opts.nodeExe ?? process.execPath;

if (process.platform !== 'win32') die('便携包依赖 Compress-Archive/Expand-Archive，仅支持在 Windows 上构建');

try {
  // 0) 构建（协议 dist 是 gitignored 的编译产物，必须与 hub dist 一起重建）
  if (!opts.skipBuild) {
    log('构建：npx pnpm build（tsconfig.protocol.json + packages/hub）');
    runNpx(['pnpm@9.0.0', 'build'], repoRoot);
  }

  const sourceProtocolDir = path.join(repoRoot, 'dist');
  // 仓库根的协议包布局是 <root>/package.json + <root>/dist → 以仓库根为包目录读取
  const expectedProtocol = protocolVersionOf(repoRoot);
  log(`目标：hub ${version} / 协议 ${expectedProtocol}`);

  // 1) 暂存 app/：hub dist + 协议包 + 依赖清单
  fs.rmSync(stageRoot, { recursive: true, force: true });
  fs.mkdirSync(appDir, { recursive: true });
  copyDir(path.join(repoRoot, 'packages', 'hub', 'dist'), path.join(appDir, 'dist'));
  const stagedProtoSource = path.join(appDir, 'momo-hub-protocol');
  copyDir(sourceProtocolDir, path.join(stagedProtoSource, 'dist'));
  fs.copyFileSync(path.join(repoRoot, 'package.json'), path.join(stagedProtoSource, 'package.json'));
  const appPkgTemplate = path.join(repoRoot, 'packaging', 'portable', 'app-package.json');
  fs.writeFileSync(
    path.join(appDir, 'package.json'),
    fs.readFileSync(appPkgTemplate, 'utf8').replace(/__VERSION__/g, version),
  );

  // 2) 干净安装生产依赖（hoisted：node_modules 为实体目录 + 自行编译原生绑定）
  log('安装生产依赖：npx pnpm install --node-linker=hoisted --prod（原生绑定需编译，约 2-4 分钟）');
  runNpx(['pnpm@9.0.0', 'install', '--node-linker=hoisted', '--prod'], appDir);
  fs.rmSync(path.join(appDir, 'pnpm-lock.yaml'), { force: true });
  // .bin 内是 CLI 垫片（可能是链接），运行期用不到，去掉避免 zip 里出现断链
  fs.rmSync(path.join(appDir, 'node_modules', '.bin'), { recursive: true, force: true });
  // hoisted 模式仍会留一份 node_modules/.pnpm（与顶层扁平副本互为硬链接）：
  // 扁平副本本身是完整的，删掉 .pnpm 可省体积；后续门禁即验证「删后仍可运行」
  fs.rmSync(path.join(appDir, 'node_modules', '.pnpm'), { recursive: true, force: true });
  // 原生绑定按源码编译时会留大量中间产物（build/ 可达 45MB）
  pruneBetterSqlite3(appDir);

  // 3) 门禁 G1：三份协议副本必须同版本，且都是实体目录（本次事故的根因）
  const stagedProtoInstalled = path.join(appDir, 'node_modules', 'momo-hub-protocol');
  assertRealDir(stagedProtoSource, '暂存 app/momo-hub-protocol');
  assertRealDir(stagedProtoInstalled, '暂存 app/node_modules/momo-hub-protocol');
  for (const [label, dir] of [
    ['app/momo-hub-protocol', stagedProtoSource],
    ['app/node_modules/momo-hub-protocol', stagedProtoInstalled],
  ]) {
    const v = protocolVersionOf(dir);
    if (v !== expectedProtocol) die(`协议版本错配：${label} = ${v}，源码 = ${expectedProtocol}`);
    const pv = readJson(path.join(dir, 'package.json')).version;
    if (pv !== version) die(`协议包版本错配：${label} = ${pv}，期望 ${version}`);
    log(`✓ ${label}：包 ${pv} / 协议 ${v}（实体目录）`);
  }

  // 4) 组装便携包根目录：内置运行时 + 启动器 + 说明 + 构建信息
  const probeNode = spawnSync(nodeExe, ['--version'], { encoding: 'utf8' });
  if (probeNode.status !== 0) die(`无法执行 --node 指定的运行时：${nodeExe}`);
  const nodeVersion = probeNode.stdout.trim();
  if (!/^v24\./.test(nodeVersion)) {
    log(`⚠ 内置运行时为 ${nodeVersion}：便携包约定 Node 24（依赖需与之 ABI 匹配）`);
  }
  fs.copyFileSync(nodeExe, path.join(stageRoot, 'node.exe'));
  for (const f of ['start.cmd', 'lan-ip.js']) {
    fs.copyFileSync(path.join(repoRoot, 'packaging', 'portable', f), path.join(stageRoot, f));
  }
  fs.writeFileSync(
    path.join(stageRoot, 'README.md'),
    fs
      .readFileSync(path.join(repoRoot, 'packaging', 'portable', 'README.md'), 'utf8')
      .replace(/__VERSION__/g, version)
      .replace(/__PROTOCOL__/g, expectedProtocol),
  );
  fs.writeFileSync(
    path.join(stageRoot, 'BUILD-INFO.json'),
    `${JSON.stringify(
      {
        hub: { version, protocol: expectedProtocol },
        runtime: { node: nodeVersion, exe: path.basename(nodeExe), arch: process.arch },
        builtAt: new Date().toISOString(),
        builder: 'scripts/build-portable.mjs',
      },
      null,
      2,
    )}\n`,
  );
  log(`内置运行时：${nodeVersion}（${nodeExe}）`);

  // 5) 门禁 G2：原生绑定在**内置运行时**下实载（ABI 匹配的硬证据）
  {
    const r = spawnSync(
      path.join(stageRoot, 'node.exe'),
      ['-e', "const D=require('better-sqlite3');new D(':memory:');console.log('modules='+process.versions.modules)"],
      { cwd: appDir, encoding: 'utf8' },
    );
    if (r.status !== 0) die(`原生绑定加载失败（ABI 不匹配？）：\n${(r.stderr || r.stdout).trim()}`);
    log(`✓ better-sqlite3 实载：${r.stdout.trim()}`);
  }

  // 6) 门禁 G3：暂存包真机引导 + hello 握手
  await handshakeGate(stageRoot, expectedProtocol, '暂存包');

  // 7) 打 zip
  fs.mkdirSync(opts.out, { recursive: true });
  fs.rmSync(zipPath, { force: true });
  fs.rmSync(`${zipPath}.sha256`, { force: true });
  createZip(stageRoot, zipPath);

  // 8) 门禁 G4：落地包复验——解压「真正会发出去的那份」再跑一遍协议与握手
  {
    const extractDir = path.join(os.tmpdir(), `momo-hub-verify-${process.pid}`);
    rmrf(extractDir, '解压校验目录');
    extractZip(zipPath, extractDir);
    const landedProto = path.join(extractDir, 'app', 'node_modules', 'momo-hub-protocol');
    assertRealDir(landedProto, '落地包 app/node_modules/momo-hub-protocol');
    const landedVersion = protocolVersionOf(landedProto);
    if (landedVersion !== expectedProtocol) {
      die(`落地包协议版本 ${landedVersion}，期望 ${expectedProtocol}`);
    }
    await handshakeGate(extractDir, expectedProtocol, '落地包');
    if (opts.keepExtract) log(`保留解压目录：${extractDir}`);
    else await rmrfAsync(extractDir, '解压校验目录');
  }

  // 9) 摘要 + 校验和
  const sha256 = crypto.createHash('sha256').update(fs.readFileSync(zipPath)).digest('hex');
  fs.writeFileSync(`${zipPath}.sha256`, `${sha256}  ${pkgName}.zip\n`);
  const sizeMb = (fs.statSync(zipPath).size / 1024 / 1024).toFixed(2);
  log('');
  log(`✓ 产物：${zipPath}（${sizeMb} MB）`);
  log(`✓ SHA256：${sha256}`);
  log(`✓ 版本：hub ${version} / 协议 ${expectedProtocol} / 运行时 ${nodeVersion}`);
} finally {
  await rmrfAsync(stageRoot, '暂存目录');
}
