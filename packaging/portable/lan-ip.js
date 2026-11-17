// 局域网地址探测：挑一个对同网段其他机器可达的本机 IPv4。
// 过滤：回环 / 链路本地（169.254.*，APIPA 说明该网卡没拿到 DHCP）/ 虚拟网卡
// （Hyper-V/WSL/Mihomo/VMware/VirtualBox 这类只对虚拟网络有意义）。
// 优先级：192.168.* > 10.* > 172.16-31.* > 其余（更像家庭/办公网段）。
// 输出纯地址（无换行噪声），探测不到时输出空串由调用方决定回退策略。
'use strict';
const os = require('node:os');

const VIRTUAL = /vEthernet|WSL|Hyper-?V|Mihomo|Clash|VirtualBox|VMware|Loopback|TAP|TUN|Tailscale|ZeroTier/i;

const candidates = [];
for (const [name, list] of Object.entries(os.networkInterfaces())) {
  for (const addr of list || []) {
    if (addr.family !== 'IPv4' || addr.internal) continue;
    if (addr.address.startsWith('169.254.')) continue;
    if (VIRTUAL.test(name)) continue;
    candidates.push(addr.address);
  }
}

const pick =
  candidates.find((a) => a.startsWith('192.168.')) ??
  candidates.find((a) => a.startsWith('10.')) ??
  candidates.find((a) => /^172\.(1[6-9]|2\d|3[01])\./.test(a)) ??
  candidates[0] ??
  '';

process.stdout.write(pick);
