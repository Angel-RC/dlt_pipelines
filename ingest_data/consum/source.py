from typing import Any
from requests.models import Response
import json
from loguru import logger
import dlt
from dlt.sources.rest_api import RESTAPIConfig, rest_api_resources
from dlt.sources.helpers.rest_client.paginators import OffsetPaginator


def _flatten_categories(node: dict, parent_id: int | None = None) -> list[dict]:
    children = node.pop("subcategories", [])
    node["parent_id"] = parent_id
    result = [node]
    for child in children:
        result.extend(_flatten_categories(child, parent_id=node["id"]))
    return result


def clean_categories(response: Response, *args, **kwargs) -> Response:
    payload = response.json()

    all_categories = []
    for record in payload:
        all_categories.extend(_flatten_categories(record, parent_id=None))
    all_categories.sort(key=lambda x: x["id"])
    modified_content: bytes = json.dumps({"results": all_categories}).encode("utf-8")
    response._content = modified_content
    return response


@dlt.source(name="consum")
def consum_source() -> Any:
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://tienda.consum.es/api/rest/V1.0/",
            "auth": None,
        },
        "resource_defaults": {
            "endpoint": {
                "response_actions": [
                    {"status_code": 404, "action": "ignore"},
                    {"status_code": 410, "action": "ignore"},
                ]
            },
        },
        "resources": [
            {
                "name": "categories",
                "write_disposition": "replace",
                "endpoint": {
                    "path": "shopping/category/menu/",
                    "data_selector": "results",
                    "method": "GET",
                    "response_actions": [
                        {"status_code": 200, "action": clean_categories},
                    ],
                },
            },
            {
                "name": "products",
                "write_disposition": {"disposition": "merge", "strategy": "scd2"},
                "primary_key": "id",
                "endpoint": {
                    "path": "catalog/product",
                    "data_selector": "products",
                    "method": "GET",
                    "params": {"showRecommendations": "false"},
                    "paginator": OffsetPaginator(
                        limit=20,
                        total_path="totalCount",
                    ),
                },
            },
        ],
    }

    yield from rest_api_resources(config)
