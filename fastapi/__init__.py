"""Lightweight FastAPI stubs required for unit tests."""
from __future__ import annotations

import types
from typing import Any, Callable, Dict, List, Optional

__all__ = [
    "APIRouter",
    "Depends",
    "FastAPI",
    "File",
    "HTTPException",
    "Query",
    "Response",
    "UploadFile",
    "status",
]


class HTTPException(Exception):
    def __init__(self, status_code: int, detail: Any = None) -> None:
        super().__init__(detail if detail is not None else status_code)
        self.status_code = status_code
        self.detail = detail


class Response:
    def __init__(self, content: Any = None, status_code: int = 200) -> None:
        self.content = content
        self.status_code = status_code


class UploadFile:
    def __init__(self, filename: str = "", content: bytes | None = None) -> None:
        self.filename = filename
        self._content = content or b""

    async def read(self) -> bytes:
        return self._content


class _RouteRegistry:
    def __init__(self) -> None:
        self.routes: List[Dict[str, Any]] = []

    def _register(self, method: str, path: str, endpoint: Callable[..., Any], options: Dict[str, Any]) -> Callable[..., Any]:
        self.routes.append({"method": method, "path": path, "endpoint": endpoint, "options": options})
        return endpoint

    def get(self, path: str, **options: Any) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
            return self._register("GET", path, func, options)

        return decorator

    def post(self, path: str, **options: Any) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
            return self._register("POST", path, func, options)

        return decorator

    def delete(self, path: str, **options: Any) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
            return self._register("DELETE", path, func, options)

        return decorator

    def include_router(self, router: "APIRouter", **kwargs: Any) -> None:
        for route in router._registry.routes:
            entry = dict(route)
            entry.update(kwargs)
            self.routes.append(entry)


class APIRouter:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self._registry = _RouteRegistry()

    def get(self, path: str, **options: Any) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        return self._registry.get(path, **options)

    def post(self, path: str, **options: Any) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        return self._registry.post(path, **options)

    def delete(self, path: str, **options: Any) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        return self._registry.delete(path, **options)

    def include_router(self, router: "APIRouter", **kwargs: Any) -> None:
        self._registry.include_router(router, **kwargs)


class FastAPI(_RouteRegistry):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__()


def Depends(dependency: Callable[..., Any]) -> Callable[..., Any]:
    return dependency


def File(default: Any = None, **kwargs: Any) -> Any:
    return default


def Query(default: Any = None, **kwargs: Any) -> Any:
    return default


_status = types.SimpleNamespace(
    HTTP_200_OK=200,
    HTTP_201_CREATED=201,
    HTTP_202_ACCEPTED=202,
    HTTP_204_NO_CONTENT=204,
    HTTP_400_BAD_REQUEST=400,
    HTTP_404_NOT_FOUND=404,
    HTTP_409_CONFLICT=409,
)
status = _status


params = types.ModuleType("fastapi.params")
params.Query = Query

middleware = types.ModuleType("fastapi.middleware")
cors = types.ModuleType("fastapi.middleware.cors")


class CORSMiddleware:
    def __init__(self, app: Any, **kwargs: Any) -> None:
        self.app = app
        self.options = kwargs


cors.CORSMiddleware = CORSMiddleware
middleware.cors = cors

import sys as _sys

_sys.modules.setdefault("fastapi.params", params)
_sys.modules.setdefault("fastapi.middleware", middleware)
_sys.modules.setdefault("fastapi.middleware.cors", cors)

__all__ += ["CORSMiddleware"]
