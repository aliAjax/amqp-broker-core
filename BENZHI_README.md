基于 Go 实现的 AMQP 1.0 消息代理后端服务项目，用于服务间可靠消息传输、投递恢复与集群租约管理。

# amqp-broker-core__007

## 构建镜像

请从**仓库根目录**执行；`benzhi.Dockerfile`、`build_benzhi_docker.sh`、`BENZHI_README.md` 均固定在该目录：

```bash
./build_benzhi_docker.sh <image-name> [linux/amd64|linux/arm64]
```

## 标准命令

```bash
go build ./...     # 编译
go run ./cmd/broker   # 启动
go test ./...      # 测试（如有）
```

```bash
cd frontend && npm install   # 前端依赖（镜像构建阶段已预装）
```

## 环境

- 基础镜像: golang:1.22
- Go 模块目录: `.`
- 依赖已在镜像构建阶段预下载，容器内离线可用。
- 容器内工作目录: `/app`
- 前端目录: `frontend`（Node.js 20，npm 依赖已在镜像构建阶段预下载）
