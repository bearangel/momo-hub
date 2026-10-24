// 服务端推送事件目录（ws 控制面，evt 信封）。
import type { HubResourceType } from './resources';

export const EVT = {
  nodeOnline: 'node.online',
  nodeOffline: 'node.offline',
  resourcePublished: 'resource.published',
} as const;
export type EventMethodName = (typeof EVT)[keyof typeof EVT];

export interface NodeOnlineEvent { nodeId: string; nodeName: string; at: number }
export interface NodeOfflineEvent { nodeId: string; at: number }

/** resource.published：发布成功后广播全部在线节点（spec M1 §3.3/§5）。 */
export interface ResourcePublishedEvent {
  slug: string;
  type: HubResourceType;
  name: string;
  version: string;
  author: string;
  at: number;
}
