import os
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

app = FastAPI(
    title="12306 Ticketing Frontend Web App Interface",
    description="Dedicated independent Frontend static server for the 12306 Ticket Allocation MVP Dashboard.",
    version="1.0.0"
)

# Physically resolve the absolute path to 'static' assets subdirectory
static_dir = os.path.join(os.path.dirname(__file__), "static")

# Mount '/static' route to serve static assets (style.css, app.js) via high-performance StaticFiles
app.mount("/static", StaticFiles(directory=static_dir), name="static")

@app.get("/", response_class=HTMLResponse)
async def serve_dashboard():
    """Load and serve the fully-featured responsive HTML5 Single Page Application (SPA)."""
    html_path = os.path.join(static_dir, "index.html")
    
    # Read the static single-page asset from disk
    with open(html_path, "r", encoding="utf-8") as f:
        return f.read()
