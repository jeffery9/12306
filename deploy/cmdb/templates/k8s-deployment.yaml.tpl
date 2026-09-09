apiVersion: apps/v1
kind: Deployment
metadata:
  name: ticketing-web-api
  namespace: {{ k8s_namespace }}
  labels:
    app: ticketing-web-api
spec:
  replicas: {{ k8s_web_replicas }}
  selector:
    matchLabels:
      app: ticketing-web-api
  template:
    metadata:
      labels:
        app: ticketing-web-api
    spec:
      containers:
      - name: web-api
        image: {{ k8s_web_image }}
        imagePullPolicy: IfNotPresent
        ports:
        - containerPort: 8000
        env:
        - name: DATABASE_URL
          value: "{{ k8s_database_url }}"
        - name: REDIS_URL
          value: "{{ k8s_redis_url }}"
        - name: KAFKA_BOOTSTRAP_SERVERS
          value: "{{ k8s_kafka_bootstrap_servers }}"
        resources:
          requests:
            cpu: "{{ k8s_cpu_request }}"
            memory: "{{ k8s_memory_request }}"
          limits:
            cpu: "{{ k8s_cpu_limit }}"
            memory: "{{ k8s_memory_limit }}"
        readinessProbe: # SRE 就绪性检测探针，确保 Pod 处于真正可用状态才接入 Service 流量
          httpGet:
            path: /api/v1/ops/health
            port: 8000
          initialDelaySeconds: 5
          periodSeconds: 5
        livenessProbe: # SRE 存活性检测探针，故障自动重启
          httpGet:
            path: /api/v1/ops/health
            port: 8000
          initialDelaySeconds: 10
          periodSeconds: 10
