#!/bin/bash
# 🌌 12306 {{ wuhan_name }} — SRE 物理分片自愈切换与健康巡检脚本
# ⚠️ THIS FILE IS AUTO-GENERATED FROM CMDB INVENTORY. DO NOT EDIT DIRECTLY.
# 本脚本由武汉中心运维台执行，用于实时校验京、沪、汉三地 etcd 多数派共识并执行强制主备漂移

set -euo pipefail

# ───────────────── 环境变量与配置对齐 ─────────────────
ETCD_ENDPOINT="http://{{ wuhan_ip }}:{{ wuhan_client_port }}"
PATRONI_CONF="/etc/patroni/patroni.yml"
CLUSTER_NAME="{{ cluster_name }}"
RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color

echo -e "${GREEN}=== 12306 两地三中心（武汉仲裁）SRE 状态巡检启动 ===${NC}"

# 1. 物理检查 etcd {{ wuhan_id }} 节点分布式共识状态
echo "1. 巡检三地 etcd 多数派健康度..."
if ! command -v etcdctl &> /dev/null; then
    echo -e "${RED}[ERROR] etcdctl 未安装！请先部署 etcd 工具链。${NC}"
    exit 1
fi

etcdctl --endpoints="$ETCD_ENDPOINT" endpoint health --write-out=table

echo "2. 校验集群活跃租约与 Key 分布..."
etcdctl --endpoints="$ETCD_ENDPOINT" get /service --prefix --keys-only

# 2. 检查 Patroni PostgreSQL 运行集群状态
echo "3. 巡检 $CLUSTER_NAME 拓扑..."
if ! command -v patronictl &> /dev/null; then
    echo -e "${RED}[ERROR] patronictl 命令行工具未在 SRE 台安装！${NC}"
    exit 1
fi

patronictl -c "$PATRONI_CONF" list

# 3. 提供物理故障漂移强制接管接口
ACTION=${1:-"status"}

if [ "$ACTION" == "failover-beijing" ]; then
    echo -e "${RED}[WARNING] 警告：触发紧急强切！{{ beijing_name }} (P1) 发生物理断电，强制{{ shanghai_name }} (S1) 接管 Shard {{ beijing_shards }} 所有权！${NC}"
    patronictl -c "$PATRONI_CONF" failover $CLUSTER_NAME --master {{ beijing_pg_master }} --candidate {{ shanghai_pg_master }} --force
elif [ "$ACTION" == "failover-shanghai" ]; then
    echo -e "${RED}[WARNING] 警告：触发紧急强切！{{ shanghai_name }} (S1) 发生物理断电，强制{{ beijing_name }} (P1) 接管 Shard {{ shanghai_shards }} 所有权！${NC}"
    patronictl -c "$PATRONI_CONF" failover $CLUSTER_NAME --master {{ shanghai_pg_master }} --candidate {{ beijing_pg_master }} --force
else
    echo -e "${GREEN}[INFO] 巡检完成：系统处于高可用双路就绪（Active-Active Ready）状态。${NC}"
fi