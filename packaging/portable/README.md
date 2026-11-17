# momo-hub __VERSION__ — Windows 便携运行包（x64）

内置 Node 24 运行时，解压即用，无需安装 Node.js。**协议版本 __PROTOCOL__**（与 momo-studio 经版本门协商）。

## 快速开始

1. 解压整个目录到任意位置（路径不要含中文以外的特殊字符亦可正常工作）。
2. 启动（三种形态任选其一）：
   - `start.cmd` —— 仅本机，监听 `http://127.0.0.1:8787`
   - `start.cmd lan` —— **局域网多机接入**：自动探测本机局域网 IPv4 并绑定，控制台会打印其他节点该填的地址
   - `start.cmd 0.0.0.0` 或 `start.cmd 192.168.1.10` —— 绑定全部网卡 / 指定地址
3. 控制台会打印启动信息，其中包含：
   - 监听地址（以及「其他节点应该填的 URL」）
   - `hub id`
   - **加入凭证（enrollment token）** —— momo-studio 首次接入 hub 时填写，请妥善保管。

健康检查：浏览器打开 `http://<监听地址>:8787/health`，应返回 `{"ok":true,"hubId":"..."}`。

> ⚠️ PowerShell 里 `set MOMO_HUB_HOST=1.2.3.4` **不会**设置环境变量（那只是 PowerShell 的变量），
> 正确写法是 `$env:MOMO_HUB_HOST="1.2.3.4"`，或者直接用上面的参数形式 `start.cmd 1.2.3.4`。
> `cmd.exe` 里 `set` 才是设置环境变量。

## 目录说明

| 路径 | 说明 |
|---|---|
| `start.cmd` | 启动脚本（`lan` / IP / `0.0.0.0` 三种参数形态） |
| `lan-ip.js` | 局域网 IPv4 探测（过滤回环/169.254 链路本地/Hyper-V/WSL/Mihomo 等虚拟网卡） |
| `node.exe` | 内置 Node 24 运行时 |
| `app/` | hub 程序本体（`dist/` + 生产依赖 `node_modules/`） |
| `BUILD-INFO.json` | 构建溯源（hub 版本 / 协议版本 / 运行时 / 构建时间） |
| `data/` | 首次启动自动创建：SQLite 库、资源 blob、hub 身份与加入凭证 |

备份 = 备份整个 `data/` 目录；迁移机器时整目录搬走即可。

## 环境变量

| 变量 | 默认 | 说明 |
|---|---|---|
| `MOMO_HUB_PORT` | `8787` | 监听端口 |
| `MOMO_HUB_HOST` | `127.0.0.1` | 监听地址。`start.cmd lan` 自动探测局域网 IP；`0.0.0.0` 监听全部网卡（启动日志 WARN）。非法值会回退 `127.0.0.1` 并 WARN |
| `MOMO_HUB_DATA_DIR` | `<本目录>\data` | 数据目录 |
| `MOMO_HUB_PUBLIC_ORIGIN` | 未设置 | 反代场景下对外 origin（如 `https://hub.example.com`），影响 catalog 下载链接拼装 |

在命令行里临时覆盖：

```bat
rem cmd.exe（set 设的是环境变量）
set MOMO_HUB_HOST=0.0.0.0
start.cmd
```

```powershell
# PowerShell（注意 $env: 前缀；直接 set NAME=值 只创建 PowerShell 变量，不会传给子进程）
$env:MOMO_HUB_HOST = "0.0.0.0"
.\start.cmd
```

## 局域网 / 公网访问

- **局域网（推荐）**：`start.cmd lan` —— 自动探测本机局域网 IPv4 并只绑该网卡，控制台会直接打印
  「other nodes should use: http://x.x.x.x:8787」，studio 设置 → momo-hub 里填这个地址即可。
  - 机器有 DHCP 地址漂移或多网卡时才考虑 `start.cmd 0.0.0.0`（等于对所有网卡监听，含 Docker/WSL 虚拟网段，启动日志会 WARN）。
  - **Windows 防火墙**要放行入站，且建议只放给局域网：

    ```powershell
    New-NetFirewallRule -DisplayName "momo-hub 8787" -Direction Inbound -Protocol TCP `
      -LocalPort 8787 -Action Allow -Profile Private -RemoteAddress LocalSubnet
    ```

- **公网**：**不要让 hub 直接裸奔**（明文 HTTP，`/enroll` 仅靠 enrollment token，无失败限流）。
  用 Caddy/Nginx 反代并终结 TLS（此时 hub 自己应绑回 `127.0.0.1`），并设置 `MOMO_HUB_PUBLIC_ORIGIN`
  （否则 catalog 里的 `downloadUrl` 会按请求 Host 拼成内网地址，对端下载失败）：

  ```
  hub.example.com {
      reverse_proxy 127.0.0.1:8787
  }
  ```

- 只允许本机 / 想零暴露：保持默认 `127.0.0.1`，让对端经隧道（Tailscale / WireGuard / SSH 端口转发）进来。

## 升级

用新版便携包覆盖 `node.exe` 与 `app/` 目录，**保留 `data/`** 即可；数据库迁移在启动时自动执行。

## 排错

- 端口被占用：改 `MOMO_HUB_PORT`。
- 想重置接入凭证：删除 `data/` 内 SQLite 库中的 `hub_meta.enrollment_token`（或整套 `data/` 重来，但节点需重新 enroll）。
- 启动即退出：控制台会打印错误原因；也可在命令行手动运行 `node.exe app\dist\index.js` 查看完整输出。
