# 🌌 Prometheus HA Monitoring Configuration Template
# ⚠️ THIS FILE IS AUTO-GENERATED FROM CMDB INVENTORY. DO NOT EDIT DIRECTLY.
global:
  scrape_interval: 10s
  evaluation_interval: 10s

scrape_configs:
  # ───────────────────────────────────────────────────────────────────────────
  # 1. HAProxy Load Balancer Metrics
  # ───────────────────────────────────────────────────────────────────────────
  - job_name: 'haproxy'
    static_configs:
      - targets: ['{{ haproxy_host }}:{{ haproxy_stats }}']

  # ───────────────────────────────────────────────────────────────────────────
  # 2. etcd DCS Consensus Metrics
  # ───────────────────────────────────────────────────────────────────────────
  - job_name: 'etcd'
    static_configs:
      - targets:
          - 'etcd1:{{ beijing_client_port }}'
          - 'etcd2:{{ shanghai_client_port }}'
          - 'etcd3:{{ wuhan_client_port }}'

  # ───────────────────────────────────────────────────────────────────────────
  # 3. PostgreSQL Database Metrics (Scraped via Postgres Exporters)
  # ───────────────────────────────────────────────────────────────────────────
  - job_name: 'postgres'
    static_configs:
      - targets:
          - 'pg-exporter-node1:{{ exporter_port }}'
          - 'pg-exporter-node2:{{ exporter_port }}'
          - 'pg-exporter-node3:{{ exporter_port }}'
