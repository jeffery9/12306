global
    maxconn 10000
    log stdout format raw local0

defaults
    log global
    mode tcp
    retries 3
    timeout queue 1m
    timeout connect 10s
    timeout client 30m
    timeout server 30m
    timeout check 2s

# ─────────────────────────────────────────────────────────────────────────────
# HAProxy Statistics & Native Prometheus Exporter (Port {{ haproxy_stats }})
# 可在浏览器通过 http://localhost:{{ haproxy_stats }} 查看大屏，同时支持 Prometheus /metrics 抓取
# ─────────────────────────────────────────────────────────────────────────────
listen stats
    mode http
    bind 0.0.0.0:{{ haproxy_stats }}
    stats enable
    stats uri /
    stats refresh 5s
    http-request use-service promo-service if { path /metrics }

# ─────────────────────────────────────────────────────────────────────────────
# 1. PostgreSQL HA Write Pool (Port {{ haproxy_write }})
# 强制仅路由至处于 Active Primary 状态的 Patroni 节点
# ─────────────────────────────────────────────────────────────────────────────
backend pg_write_backend
    mode tcp
    option httpchk GET /primary
    http-check expect status 200
    default-server inter 3s fall 3 rise 2 on-marked-down shutdown-sessions
    server pg-node1 pg-node1:{{ postgres_port }} check port {{ patroni_rest_port }}
    server pg-node2 pg-node2:{{ postgres_port }} check port {{ patroni_rest_port }}
    server pg-node3 pg-node3:{{ postgres_port }} check port {{ patroni_rest_port }}

frontend pg_write_front
    bind 0.0.0.0:{{ haproxy_write }}
    mode tcp
    default_backend pg_write_backend

# ─────────────────────────────────────────────────────────────────────────────
# 2. PostgreSQL HA Read-Only Pool (Port {{ haproxy_read }})
# 负载均衡路由至处于健康从节点（Replica）状态的节点
# ─────────────────────────────────────────────────────────────────────────────
backend pg_read_backend
    mode tcp
    balance roundrobin
    option httpchk GET /replica
    http-check expect status 200
    default-server inter 3s fall 3 rise 2
    server pg-node1 pg-node1:{{ postgres_port }} check port {{ patroni_rest_port }}
    server pg-node2 pg-node2:{{ postgres_port }} check port {{ patroni_rest_port }}
    server pg-node3 pg-node3:{{ postgres_port }} check port {{ patroni_rest_port }}

frontend pg_read_front
    bind 0.0.0.0:{{ haproxy_read }}
    mode tcp
    default_backend pg_read_backend
