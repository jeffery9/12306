# 🌌 针对投影器消费端的 KEDA (Kubernetes Event-driven Autoscaling)
# ⚠️ THIS FILE IS AUTO-GENERATED FROM CMDB INVENTORY. DO NOT EDIT DIRECTLY.
# 基于 Kafka 消费者组积压量 (Lag) 动态扩缩容 Projector
# 如果高峰期退票/支付事件堆积在 Kafka 中，投影器自动横向拉起，自愈时差压缩至 100ms
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: ticketing-projector-keda
  namespace: {{ k8s_namespace }}
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: ticketing-projector # 目标是我们的投影器消费端 Deployment
  minReplicas: {{ k8s_keda_min_replicas }} # 至少双活热备
  maxReplicas: {{ k8s_keda_max_replicas }} # 配合 Kafka Topic Partitions 最大扩至 32 个并行消费者
  cooldownPeriod: 120 # 2分钟无积压后缩容
  triggers:
  - type: kafka
    metadata:
      bootstrapServers: {{ k8s_kafka_bootstrap_servers }}
      consumerGroup: 12306-projector-group
      topic: ticket_events
      lagThreshold: "{{ k8s_keda_lag_threshold }}" # 只要该消费者组积压消息数超阈值，代表投影自愈时差变大，立刻启动快速弹性扩容！
