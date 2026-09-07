import os
from fastapi import FastAPI
from fastapi.responses import HTMLResponse

app = FastAPI(
    title="12306 Ticketing Frontend Web App Interface",
    description="Dedicated independent Frontend static server for the 12306 Ticket Allocation MVP Dashboard.",
    version="1.0.0"
)

@app.get("/", response_class=HTMLResponse)
async def serve_dashboard():
    """Load and serve the fully-featured responsive HTML5 Single Page Application (SPA)."""
    html_path = os.path.join(os.path.dirname(__file__), "static", "index.html")
    
    # Read the static single-page asset from disk
    with open(html_path, "r", encoding="utf-8") as f:
        return f.read()
