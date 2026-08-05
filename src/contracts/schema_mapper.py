import pyarrow as pa
from pydantic import BaseModel
from typing import Type, get_origin, get_args
import typing
import types

# Dicionário de mapeamento: tipo Python → tipo PyArrow
_TYPE_MAP = {
    str: pa.string(),
    int: pa.int64(),
    float: pa.float64(),
    bool: pa.bool_(),
}


def _resolve_type(annotation) -> tuple:
    origin = get_origin(annotation)
    # Optional[T] é Union[T, None] — origin é types.UnionType ou typing.Union
    if origin in (typing.Union, getattr(types, 'UnionType', None)):
        args = get_args(annotation)
        # args = (float, type(None)) — o tipo real é o primeiro que não é None
        non_none = [a for a in args if a is not type(None)]
        if non_none:
            return (non_none[0], True)
    return (annotation, False)


def to_pyarrow_schema(model_class: Type[BaseModel]) -> pa.Schema:
    fields = []
    for field_name, field_info in model_class.model_fields.items():
        raw_type, is_optional = _resolve_type(field_info.annotation)
        arrow_type = _TYPE_MAP.get(raw_type)
        if arrow_type is None:
            raise TypeError(f"Tipo não mapeado: {raw_type}")
        fields.append(pa.field(field_name, arrow_type, nullable=is_optional))
    return pa.schema(fields)
