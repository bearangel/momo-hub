# momo-hub 服务镜像——Node 24 Alpine，数据卷 /data。
FROM node:24-alpine AS build
WORKDIR /app
RUN corepack enable
COPY package.json pnpm-workspace.yaml tsconfig.base.json tsconfig.protocol.json ./
COPY packages/hub/package.json packages/hub/
RUN pnpm install --no-frozen-lockfile
COPY src ./src
COPY packages/hub/tsconfig.json packages/hub/
COPY packages/hub/src packages/hub/src
RUN pnpm build

FROM node:24-alpine
WORKDIR /app
ENV NODE_ENV=production MOMO_HUB_DATA_DIR=/data
COPY --from=build /app/package.json /app/pnpm-workspace.yaml ./
COPY --from=build /app/node_modules ./node_modules
COPY --from=build /app/packages/hub/node_modules ./packages/hub/node_modules
COPY --from=build /app/packages/hub/dist ./packages/hub/dist
# 协议包 dist——momo-hub-protocol 经 link:../.. 符号链接解析到仓库根，main 指向 ./dist/index.js，漏拷则启动即崩
COPY --from=build /app/dist ./dist
EXPOSE 8787
VOLUME /data
CMD ["node", "packages/hub/dist/index.js"]
