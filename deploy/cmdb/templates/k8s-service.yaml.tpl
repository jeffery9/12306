apiVersion: v1
kind: Service
metadata:
  name: ticketing-web-api-svc
  namespace: {{ k8s_namespace }}
spec:
  selector:
    app: ticketing-web-api
  ports:
  - protocol: TCP
    port: 80
    targetPort: 8000
  type: ClusterIP
