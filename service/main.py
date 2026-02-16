from __future__ import annotations

import uvicorn

from app.config import settings
from app.web import app


def main() -> None:
    uvicorn.run(app, host=settings.host, port=settings.port)


if __name__ == "__main__":
    main()

