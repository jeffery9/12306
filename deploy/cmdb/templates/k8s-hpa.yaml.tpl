# 🌌 针对无状态 Web 查询/预占核心的 HPA (HorizontalPodAutoscaler)
# ⚠️ THIS FILE IS AUTO-GENERATED FROM CMDB INVENTORY. DO NOT EDIT DIRECTLY.
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: ticketing-web-api-hpa
  namespace: {{ k8s_namespace }}
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: ticketing-web-api
  minReplicas: {{ k8s_hpa_min_replicas }}
  maxReplicas: {{ k8s_hpa_max_replicas }}
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: {{ k8s_hpa_cpu_utilization }} # CPU 平均水位超阈值即刻触发极速横向扩容
  - type: Resource
    resource:
      name: memory
      target:
        type: Utilization
        averageUtilization: {{ k8s_hpa_memory_utilization }} # 内存平均水位超阈值触发扩容
  behavior:
    scaleUp:
      stabilizationWindowSeconds: 0 # 立即扩容，抢票瞬间不允许有任何延迟！
      policies:
      - type: Percent
        value: 100
        periodSeconds: 15 # 允许每 15 秒实例数最高翻倍，支持灾难级流量突涌
    scaleDown:
      stabilizationWindowSeconds: 300 # 300s 稳定窗口，防止流量波动导致的抖动（Thrashing）
      policies:
      - type: Percent
        value: 10
        periodSeconds: 60 # 每分钟缓慢缩容 10%，保全物理大后方
