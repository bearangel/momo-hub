// 服务端推送事件目录（ws 控制面，evt 信封）。
export const EVT = { nodeOnline: 'node.online', nodeOffline: 'node.offline' } as const;
export type EventMethodName = (typeof EVT)[keyof typeof EVT];

export interface NodeOnlineEvent { nodeId: string; nodeName: string; at: number }
export interface NodeOfflineEvent { nodeId: string; at: number }
