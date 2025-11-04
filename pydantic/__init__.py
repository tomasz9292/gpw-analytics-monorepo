"""Lightweight stubs for the subset of Pydantic used in tests."""
from __future__ import annotations

import inspect
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple, Type, TypeVar, Union, get_args, get_origin

__all__ = [
    "BaseModel",
    "Field",
    "ValidationError",
    "ConfigDict",
    "PrivateAttr",
    "field_validator",
    "model_validator",
]

_T = TypeVar("_T")


class ValidationError(ValueError):
    """Minimal stand-in for Pydantic's ValidationError."""

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self._message = message

    def errors(self) -> List[Dict[str, str]]:
        return [{"msg": self._message}]


class _Unset:
    pass


UNSET = _Unset()


def _is_optional(annotation: Any) -> bool:
    origin = get_origin(annotation)
    if origin is Union:
        return type(None) in get_args(annotation)
    return False


@dataclass
class FieldInfo:
    annotation: Any
    default: Any = UNSET
    default_factory: Optional[Callable[[], Any]] = None
    metadata: Dict[str, Any] = None

    def __post_init__(self) -> None:
        if self.metadata is None:
            self.metadata = {}


def Field(default: Any = UNSET, *, default_factory: Optional[Callable[[], Any]] = None, **kwargs: Any) -> FieldInfo:
    return FieldInfo(annotation=Any, default=default, default_factory=default_factory, metadata=dict(kwargs))


def ConfigDict(**kwargs: Any) -> Dict[str, Any]:
    return dict(kwargs)


class PrivateAttr:
    """Simplified descriptor mirroring pydantic PrivateAttr."""

    def __init__(self, default: Any = UNSET) -> None:
        self.default = default
        self.name: Optional[str] = None

    def __set_name__(self, owner: Type[Any], name: str) -> None:
        self.name = f"__private_{name}"

    def __get__(self, instance: Any, owner: Type[Any]) -> Any:
        if instance is None:
            return self
        if self.name is None:
            raise AttributeError("Private attribute not initialised")
        if self.name not in instance.__dict__:
            if self.default is UNSET:
                value = None
            elif callable(self.default):
                value = self.default()
            else:
                value = self.default
            instance.__dict__[self.name] = value
        return instance.__dict__[self.name]

    def __set__(self, instance: Any, value: Any) -> None:
        if self.name is None:
            raise AttributeError("Private attribute not initialised")
        instance.__dict__[self.name] = value


@dataclass
class _FieldValidator:
    fields: Tuple[str, ...]
    mode: str
    func: Any


@dataclass
class _ModelValidator:
    mode: str
    func: Any


def field_validator(*fields: str, mode: str = "after") -> Callable[[Any], Any]:
    def decorator(func: Any) -> Any:
        setattr(func, "__pydantic_field_validator__", _FieldValidator(fields, mode, func))
        return func

    return decorator


def model_validator(*, mode: str = "after") -> Callable[[Any], Any]:
    def decorator(func: Any) -> Any:
        setattr(func, "__pydantic_model_validator__", _ModelValidator(mode, func))
        return func

    return decorator


class BaseModel:
    """Very small subset of Pydantic's BaseModel."""

    __fields__: Dict[str, FieldInfo] = {}
    __field_validators__: Dict[str, Dict[str, List[Any]]] = {}
    __model_validators__: List[_ModelValidator] = []

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        fields: Dict[str, FieldInfo] = {}

        # inherit validators from parents
        field_validators: Dict[str, Dict[str, List[Any]]] = {}
        model_validators: List[_ModelValidator] = []
        for base in reversed(cls.__mro__[1:]):
            if hasattr(base, "__fields__"):
                for name, info in getattr(base, "__fields__", {}).items():
                    fields.setdefault(name, info)
            if hasattr(base, "__field_validators__"):
                parent_validators = getattr(base, "__field_validators__")
                for name, data in parent_validators.items():
                    entry = field_validators.setdefault(name, {"before": [], "after": []})
                    entry["before"].extend(data.get("before", []))
                    entry["after"].extend(data.get("after", []))
            if hasattr(base, "__model_validators__"):
                model_validators.extend(getattr(base, "__model_validators__"))

        annotations = {}
        for base in reversed(cls.__mro__):
            annotations.update(getattr(base, "__annotations__", {}))

        for name, annotation in annotations.items():
            raw_default = cls.__dict__.get(name, UNSET)
            if isinstance(raw_default, FieldInfo):
                info = FieldInfo(
                    annotation=annotation,
                    default=raw_default.default,
                    default_factory=raw_default.default_factory,
                    metadata=dict(raw_default.metadata),
                )
            else:
                info = FieldInfo(annotation=annotation, default=raw_default)
            fields[name] = info
            if isinstance(raw_default, FieldInfo):
                if info.default is not UNSET:
                    setattr(cls, name, info.default)
                elif hasattr(cls, name):
                    delattr(cls, name)

        collected_validators: List[_FieldValidator] = []
        for attr in cls.__dict__.values():
            validator = getattr(attr, "__pydantic_field_validator__", None)
            if isinstance(validator, _FieldValidator):
                collected_validators.append(validator)
            validator_model = getattr(attr, "__pydantic_model_validator__", None)
            if isinstance(validator_model, _ModelValidator):
                model_validators.append(validator_model)

        for validator in collected_validators:
            for field_name in validator.fields:
                entry = field_validators.setdefault(field_name, {"before": [], "after": []})
                entry.setdefault("before", [])
                entry.setdefault("after", [])
                entry[validator.mode].append(validator.func)

        cls.__fields__ = fields
        cls.__field_validators__ = field_validators
        cls.__model_validators__ = model_validators

    def __init__(self, **data: Any) -> None:
        field_values: Dict[str, Any] = {}
        errors: List[str] = []
        for name, info in self.__class__.__fields__.items():
            if name in data:
                value = data.pop(name)
            else:
                if info.default is not UNSET:
                    value = info.default
                elif info.default_factory is not None:
                    value = info.default_factory()
                elif _is_optional(info.annotation):
                    value = None
                else:
                    errors.append(f"Missing required field: {name}")
                    continue
            validators = self.__class__.__field_validators__.get(name, {})
            for fn in validators.get("before", []):
                bound = _bind_validator(fn, self.__class__)
                value = bound(value)
            field_values[name] = value
        if errors:
            raise ValidationError("; ".join(errors))
        for name, value in field_values.items():
            validators = self.__class__.__field_validators__.get(name, {})
            for fn in validators.get("after", []):
                bound = _bind_validator(fn, self.__class__)
                value = bound(value)
            setattr(self, name, value)
        for extra_name, extra_value in data.items():
            setattr(self, extra_name, extra_value)
        for validator in self.__class__.__model_validators__:
            bound = _bind_validator(validator.func, self.__class__)
            result = bound(self)
            if result is not None:
                if validator.mode == "after" and isinstance(result, self.__class__):
                    for key, value in result.model_dump().items():
                        setattr(self, key, value)
        self.__post_init__()

    def __post_init__(self) -> None:
        pass

    def model_dump(self) -> Dict[str, Any]:
        result: Dict[str, Any] = {}
        for name in self.__class__.__fields__:
            result[name] = getattr(self, name, None)
        return result

    def model_copy(self: _T, update: Optional[Dict[str, Any]] = None) -> _T:
        data = self.model_dump()
        if update:
            data.update(update)
        return self.__class__(**data)

    @classmethod
    def model_validate(cls: Type[_T], payload: Dict[str, Any]) -> _T:
        return cls(**payload)

    def dict(self) -> Dict[str, Any]:
        return self.model_dump()

    def __repr__(self) -> str:
        parts = ", ".join(f"{name}={getattr(self, name, None)!r}" for name in self.__class__.__fields__)
        return f"{self.__class__.__name__}({parts})"


def _bind_validator(fn: Any, cls: Type[Any]) -> Callable[[Any], Any]:
    if isinstance(fn, (staticmethod, classmethod)):
        return fn.__get__(cls, cls)
    if inspect.ismethod(fn):
        return fn
    return fn.__get__(None, cls)
