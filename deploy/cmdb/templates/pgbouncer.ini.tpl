# 🌌 PgBouncer Enterprise-Grade Connection Pooler Config Spec
# ⚠️ THIS FILE IS AUTO-GENERATED FROM CMDB INVENTORY. DO NOT EDIT DIRECTLY.
[databases]
* = host={{ haproxy_host }} port={{ haproxy_write }} auth_user=postgres

[pgbouncer]
logfile = /var/log/postgresql/pgbouncer.log
pidfile = /var/run/postgresql/pgbouncer.pid

# ─────────────────────────────────────────────────────────────────────────────
# Network Port & Bind Addresses
# ─────────────────────────────────────────────────────────────────────────────
listen_addr = *
listen_port = {{ pgbouncer_listen }}

# Authentication
auth_type = md5
auth_file = /etc/pgbouncer/userlist.txt

# ─────────────────────────────────────────────────────────────────────────────
# Connection Pooling Resource Control (Tuned for 12306)
# ─────────────────────────────────────────────────────────────────────────────
pool_mode = transaction          # 核心：事务级分配模式 (保证最快速连接池释放)
max_client_conn = 10000          # 允许万级并发客户端建立虚拟连接
default_pool_size = 50           # 每个物理数据库最多打开的真实连接数
min_pool_size = 10               # 保持的最少空闲连接
reserve_pool_size = 5            # 突发状况额外池空间
server_idle_timeout = 600        # 真实物理连接空闲清理时间 (10分钟)
max_db_connections = 200         # 强制数据库实例最高并发限制，完美抗压

# System Logs
log_connections = 0
log_disconnections = 0
log_pooler_errors = 1
