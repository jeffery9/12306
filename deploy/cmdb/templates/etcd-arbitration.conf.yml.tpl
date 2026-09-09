# 🌌 12306 {{ wuhan_name }} etcd (Node {{ wuhan_id }}) 生产级物理配置规范
# ⚠️ THIS FILE IS AUTO-GENERATED FROM CMDB INVENTORY. DO NOT EDIT DIRECTLY.
name: 'etcd{{ wuhan_id }}'
data-dir: '/var/lib/etcd/etcd{{ wuhan_id }}.etcd'
listen-peer-urls: 'http://{{ wuhan_ip }}:{{ wuhan_peer_port }}' # 武汉内部物理网卡对等专线上联监听
listen-client-urls: 'http://{{ wuhan_ip }}:{{ wuhan_client_port }},http://127.0.0.1:{{ wuhan_client_port }}' # 监听本地及京沪专线客户端请求

# ───────────────── 跨地域集群化配额宣告 ─────────────────
initial-advertise-peer-urls: 'http://{{ wuhan_ip }}:{{ wuhan_peer_port }}' # 宣告本对等体地址
advertise-client-urls: 'http://{{ wuhan_ip }}:{{ wuhan_client_port }}'      # 宣告本客户端寻址地址

# ───────────────── 奇数对齐集群初始化信息 ─────────────────
initial-cluster: 'etcd1=http://{{ beijing_ip }}:{{ beijing_peer_port }},etcd2=http://{{ shanghai_ip }}:{{ shanghai_peer_port }},etcd3=http://{{ wuhan_ip }}:{{ wuhan_peer_port }}'
initial-cluster-token: '{{ cluster_token }}'
initial-cluster-state: 'new'

# ───────────────── SRE 高频心跳与选主时延参数 ─────────────────
# 考虑到跨地域专线的物理距离限制（时延 RTT 约 11~15ms），高频心跳时延参数需要适当调大，防止误判
heartbeat-interval: {{ wuhan_heartbeat }}   # 毫秒。常态心跳时延，默认 100ms
election-timeout: {{ wuhan_election }}    # 毫秒。选举超时判定，默认 1000ms

# ───────────────── 物理性能与安全防护 ─────────────────
quota-backend-bytes: {{ wuhan_quota }} # 限制 etcd KV 存储引擎配额为 8GB，防止租约锁过度积累溢出
auto-compaction-retention: '1' # 每小时自动执行一次数据压缩碎片整理 (Compaction)