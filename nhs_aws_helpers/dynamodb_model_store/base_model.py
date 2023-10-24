import inspect
from dataclasses import fields, is_dataclass
from functools import lru_cache
from typing import Any, Dict, Generic, List, Mapping, Tuple, Type, TypeVar, cast

TModelKey = TypeVar("TModelKey", bound=Mapping[str, Any])


class serialised_property(property):
    pass


class BaseModel(Generic[TModelKey]):
    _model_key_type: Type[TModelKey]

    @serialised_property
    def model_type(self) -> str:
        return self.__class__.__name__

    def model_key(self) -> TModelKey:
        return cast(TModelKey, {k: getattr(self, k) for k in self.model_key_fields()})

    @classmethod
    def model_key_fields(cls) -> List[str]:
        model_key_fields = _MODEL_KEY_FIELDS.get(cls._model_key_type)
        if not model_key_fields:
            model_key_fields = list(cls._model_key_type.__annotations__.keys())
            if len(model_key_fields) not in (1, 2):
                raise ValueError(f"{cls.__name__} does not seem to have a valid model key type expect 1 or 2 keys")
            _MODEL_KEY_FIELDS[cls._model_key_type] = model_key_fields
        return model_key_fields

    @classmethod
    def model_key_from_item(cls, item: Dict[str, Any]) -> TModelKey:
        return cast(TModelKey, {k: item.get(k) for k in cls.model_key_fields()})


_MODEL_KEY_FIELDS: Dict[type, List[str]] = {}


@lru_cache
def model_properties_cache(model_type: Type[BaseModel]) -> List[Tuple[str, type, Mapping[str, Any]]]:
    model_fields: List[Tuple[str, type, Mapping[str, Any]]] = []

    if is_dataclass(model_type):
        model_fields.extend([(field.name, field.type, field.metadata) for field in fields(model_type)])

    for name, member in inspect.getmembers(model_type, lambda o: isinstance(o, serialised_property)):
        field_type = member.fget.__annotations__.get("return")
        assert field_type, f"serialised_property {model_type.__name__}.{name} requires a type annotation"
        model_fields.append((name, field_type, {}))

    return model_fields
