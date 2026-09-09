FROM python:3.11-slim

# 创建非特权系统用户组及系统用户，锁死特权逃逸
RUN groupadd -g 10001 ticketing_sre \
    && useradd -u 10001 -g ticketing_sre -m -s /bin/bash 12306sre

WORKDIR /workspace

# 拷贝并物理移交所有权给非特权 SRE 用户
COPY --chown=12306sre:ticketing_sre requirements.txt .

# 优化依赖安装，不产生任何物理本地缓存
RUN pip install --no-cache-dir -r requirements.txt

# 拷贝全量业务代码并移交所有权
COPY --chown=12306sre:ticketing_sre . .

# 设定运行环境变量
ENV PYTHONPATH=/workspace/src

# 物理切换至非特权用户，启动安全硬隔离
USER 12306sre

EXPOSE 8000

CMD ["uvicorn", "src.app.main:app", "--host", "0.0.0.0", "--port", "8000"]
