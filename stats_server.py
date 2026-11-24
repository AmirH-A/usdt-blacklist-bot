"""Read-only accuracy API for the freeze detector panel.

    python stats_server.py
"""

import os

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.accuracy import build_report

app = FastAPI(title="Freeze Detector Accuracy", docs_url=None, redoc_url=None)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


@app.get("/api/accuracy")
def accuracy():
    return build_report()


@app.get("/health")
def health():
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "stats_server:app",
        host=os.environ.get("STATS_HOST", "127.0.0.1"),
        port=int(os.environ.get("STATS_PORT", "8010")),
        reload=False,
        timeout_keep_alive=75,
        ws="none",
    )
