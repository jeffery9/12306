import contextvars
import logging
import sys

# 1. W3C Task-Local Context Storage
trace_id_var = contextvars.ContextVar("trace_id", default="")
traceparent_var = contextvars.ContextVar("traceparent", default="")

class SREStructuredLogFilter(logging.Filter):
    """Automatically intercepts log records and injects the current task-local W3C trace_id."""
    def filter(self, record):
        trace_id = trace_id_var.get()
        # Fallback to no-trace if trace context is not active (e.g. idle cron workers)
        record.trace_id = trace_id if trace_id else "no-trace"
        return True

def setup_telemetry_logging():
    """Configures the root logger with SRE structured formats to automatically propagate trace_ids."""
    root_logger = logging.getLogger()
    
    # Remove existing pre-configured handlers to avoid duplicate output formatting
    for h in root_logger.handlers[:]:
        root_logger.removeHandler(h)
        
    # Standard Stream handler directing formatted logs to stdout
    handler = logging.StreamHandler(sys.stdout)
    handler.addFilter(SREStructuredLogFilter())
    
    # SRE-Compliant Standard log format integrating the task-safe trace_id
    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] [trace_id=%(trace_id)s] %(name)s: %(message)s"
    )
    handler.setFormatter(formatter)
    root_logger.addHandler(handler)
    root_logger.setLevel(logging.INFO)
    
    # Bridge Uvicorn internal logs into our structured, trace-carrying root format
    for uvicorn_logger_name in ["uvicorn", "uvicorn.error", "uvicorn.access"]:
        ul = logging.getLogger(uvicorn_logger_name)
        ul.handlers = []
        ul.propagate = True
