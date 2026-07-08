"""Validate the shipped manifests against the JSON schema (if jsonschema present)."""

import glob
import json

import pytest
import yaml

jsonschema = pytest.importorskip("jsonschema")

SCHEMA_PATH = "manifests/manifest.schema.json"


def _load_schema():
    with open(SCHEMA_PATH) as fh:
        return json.load(fh)


@pytest.mark.parametrize("path", sorted(glob.glob("manifests/*.yml")))
def test_shipped_manifests_match_schema(path):
    schema = _load_schema()
    with open(path) as fh:
        data = yaml.safe_load(fh)
    jsonschema.validate(instance=data, schema=schema)
