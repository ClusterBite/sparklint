"""Generate spec/pyspark_api.json from an installed pyspark.

Usage:  PYTHONPATH=<dir containing pyspark> python scripts/gen_api.py > spec/pyspark_api.json

Records, for every modelled class, each public method/property and the *kind* of value it
returns (one of the sparklint type names below, or "other"). The linter compiles this file in;
building sparklint never needs pyspark. Semantics the annotations do not carry (action,
shuffle, Spark Connect support ...) live in spec/overlay.toml.
"""

from __future__ import annotations

import inspect
import json
import re
import sys
import typing

import pyspark
from pyspark.sql import (
    Column,
    DataFrame,
    DataFrameNaFunctions,
    DataFrameReader,
    DataFrameStatFunctions,
    DataFrameWriter,
    DataFrameWriterV2,
    GroupedData,
    Row,
    SparkSession,
    Window,
    WindowSpec,
)
from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql.catalog import Catalog
from pyspark.sql.conf import RuntimeConfig
from pyspark.sql.streaming import DataStreamReader, DataStreamWriter, StreamingQuery
from pyspark import RDD, SparkContext

try:
    from pyspark.sql.merge import MergeIntoWriter
except ImportError:  # pragma: no cover - older pyspark
    MergeIntoWriter = None
try:
    from pyspark.sql.pandas.group_ops import PandasCogroupedOps
except ImportError:  # pragma: no cover
    PandasCogroupedOps = None

CLASSES = {
    "SparkSession": SparkSession,
    "SparkContext": SparkContext,
    "DataFrame": DataFrame,
    "Column": Column,
    "GroupedData": GroupedData,
    "Window": Window,
    "WindowSpec": WindowSpec,
    "RDD": RDD,
    "Row": Row,
    "DataFrameReader": DataFrameReader,
    "DataFrameWriter": DataFrameWriter,
    "DataFrameWriterV2": DataFrameWriterV2,
    "DataStreamReader": DataStreamReader,
    "DataStreamWriter": DataStreamWriter,
    "StreamingQuery": StreamingQuery,
    "DataFrameNaFunctions": DataFrameNaFunctions,
    "DataFrameStatFunctions": DataFrameStatFunctions,
    "Catalog": Catalog,
    "RuntimeConfig": RuntimeConfig,
}
if MergeIntoWriter is not None:
    CLASSES["MergeIntoWriter"] = MergeIntoWriter
if PandasCogroupedOps is not None:
    CLASSES["PandasCogroupedOps"] = PandasCogroupedOps

TYPE_NAMES = set(CLASSES) | {"PandasDataFrame", "PandasOnSparkDataFrame", "Builder"}


def kind_of(annotation: object) -> str:
    """Map a return annotation (object or string) to a sparklint type name."""
    if annotation is inspect.Signature.empty or annotation is None:
        return "other"
    for cls_name, cls in CLASSES.items():
        if annotation is cls:
            return cls_name
    if isinstance(annotation, type):
        text = annotation.__name__
    else:
        text = annotation if isinstance(annotation, str) else repr(annotation)
    text = text.replace("typing.", "").replace("'", "").replace('"', "").replace(" ", "")
    if text.startswith("RDD["):
        return "RDD"
    if text in ("Union[Row,None,List[Row]]", "Union[NoneType,Row,List[Row]]", "Union[Row,NoneType,List[Row]]"):
        return "Row|list[Row]"
    # Strip module prefixes: pyspark.sql.dataframe.DataFrame -> DataFrame
    text = re.sub(r"[A-Za-z_][A-Za-z0-9_]*\.", "", text)
    if re.fullmatch(r"(Optional\[)?(ParentDataFrame|DataFrame)\]?", text):
        return "DataFrame"
    if text in ("PandasDataFrameLike", "pd.DataFrame", "DataFrameLike"):
        return "PandasDataFrame"
    if re.fullmatch(r"List\[Row\]|list\[Row\]", text):
        return "list[Row]"
    if re.fullmatch(r"(Optional\[)?Row\]?|Union\[Row, None\]", text):
        return "Row"
    for name in TYPE_NAMES:
        if text == name or text == f"Optional[{name}]":
            return name
    if text in ("int", "float", "bool", "str", "None", "NoneType"):
        return text.replace("NoneType", "None")
    if text.startswith(("List[", "list[")):
        return "list"
    return "other"


def deprecated(obj: object) -> str | None:
    doc = inspect.getdoc(obj) or ""
    m = re.search(r"\.\. deprecated::\s*([0-9.]+)\s*\n?(.*)", doc)
    if not m:
        return None
    return m.group(1)


def members(cls: type) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for name, raw in inspect.getmembers(cls):
        if name.startswith("_"):
            continue
        attr = inspect.getattr_static(cls, name, None)
        entry: dict = {}
        if isinstance(attr, property):
            entry["property"] = True
            try:
                ann = typing.get_type_hints(attr.fget)["return"]
            except Exception:  # noqa: BLE001
                ann = getattr(attr.fget, "__annotations__", {}).get("return", inspect.Signature.empty)
            entry["returns"] = kind_of(ann)
            dep = deprecated(attr.fget)
        elif callable(raw):
            try:
                sig = inspect.signature(raw)
                ann = getattr(raw, "__annotations__", {}).get("return", sig.return_annotation)
                try:
                    ann = typing.get_type_hints(raw).get("return", ann)
                except Exception:  # noqa: BLE001  # unresolvable forward refs: keep the string
                    pass
            except (TypeError, ValueError):
                ann = inspect.Signature.empty
            entry["returns"] = kind_of(ann)
            if isinstance(attr, (classmethod, staticmethod)):
                entry["static"] = True
            dep = deprecated(raw)
        else:
            continue
        if dep:
            entry["deprecated"] = dep
        out[name] = entry
    return out


def functions_module() -> dict[str, dict]:
    out: dict[str, dict] = {}
    for name in sorted(getattr(F, "__all__", dir(F))):
        if name.startswith("_"):
            continue
        obj = getattr(F, name, None)
        if not callable(obj):
            continue
        ann = getattr(obj, "__annotations__", {}).get("return", inspect.Signature.empty)
        try:
            ann = typing.get_type_hints(obj).get("return", ann)
        except Exception:  # noqa: BLE001  # unresolvable forward refs: keep the string
            pass
        entry = {"returns": kind_of(ann)}
        dep = deprecated(obj)
        if dep:
            entry["deprecated"] = dep
        out[name] = entry
    return out


def main() -> None:
    spec = {
        "pyspark_version": pyspark.__version__,
        "classes": {name: members(cls) for name, cls in CLASSES.items()},
        "functions": functions_module(),
    }
    # The builder is a nested class.
    spec["classes"]["Builder"] = members(SparkSession.Builder)
    json.dump(spec, sys.stdout, indent=1, sort_keys=True)
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
