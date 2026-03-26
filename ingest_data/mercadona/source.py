from typing import Any, Optional
from requests.models import Response
import json
import time
from loguru import logger
import dlt
from dlt.common.pendulum import pendulum
from dlt.sources.rest_api import (
    RESTAPIConfig,
    check_connection,
    rest_api_resources,
    rest_api_source,
)

def clean_products_history(record):
    record.pop("unavailable_weekdays", None)
    return record

def clean_categories(response: Response, *args, **kwargs) -> Response:
    payload = response.json()

    all_categories = []
    for record in payload["results"]:
        categories = record["categories"]
        categories = [dict(item, **{'category_group_name':record["name"], 'category_group_id':record["id"]}) for item in categories]
        
        all_categories += categories
    all_categories.sort(key=lambda x: x["id"])
    payload["results"] = all_categories
    modified_content: bytes = json.dumps(payload).encode("utf-8")
    response._content = modified_content
    return response

def wait_to_continue(response: Response = None, *args, **kwargs):
    logger.debug("Waiting 5 segs due to 403, will retry...")
    time.sleep(5)
    response.raise_for_status()  # lanza excepción para que tenacity reintente



def clean_response_products_history(response: Response, *args, **kwargs) -> Response:
    payload = response.json()
    products = [
        {
            'category_name_level_1':payload["name"],
            'category_id_level_1':payload["id"], 
            "category_name_level_2": item["name"], 
            "category_id_level_2": item["id"],
            **product
        }
        for item in payload["categories"]
        for product in item["products"]
    ]

    for product in products:
        product["categories"] = product["categories"][0]

    products.sort(key=lambda x: (x["category_name_level_1"], x["category_id_level_2"]))
    payload = products
    modified_content: bytes = json.dumps(payload).encode("utf-8")
    response._content = modified_content
    return response

@dlt.source(name="mercadona")
def mercadona_source() -> Any:
    # Create a REST API configuration for the GitHub API
    # Use RESTAPIConfig to get autocompletion and type checking
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://tienda.mercadona.es/api/",
            "auth": None
        },
        # The default configuration for all resources and their endpoints
        "resource_defaults": {
            # "primary_key": "id",
            # "write_disposition": "merge",
            "endpoint": {
                "response_actions": [
                    {"status_code": 404, "action": "ignore"},
                    {"status_code": 410, "action": "ignore"},
                    {"status_code": 403, "action": wait_to_continue}
                ],
                "params": {
                    "lang": "es",
                    "wh": "vlc1",
                },
            },
        },
        "resources": [
            {
                "name": "categories",
                "write_disposition": 'replace',
                "endpoint": {
                    "path": "categories/",
                    "data_selector": "results",
                    "method": "GET",
                    "response_actions": [
                        {
                            "status_code": 200,
                            "action": clean_categories,
                        },
                    ],
                },
            },
            {
                "name": "products_history",
                "processing_steps": [
                    # {"filter": lambda x: x["id"] < 10},
                    {"map": clean_products_history},
                ],
                "write_disposition": {"disposition": "merge", "strategy": "scd2"},
                "endpoint": {
                    "path": "categories/{category_id}/",
                    "method": "GET",
                    "response_actions": [
                        {"status_code": 200, "action": clean_response_products_history},
                        {"status_code": 404, "action": "ignore"},
                        {"status_code": 410, "action": "ignore"},
                        {"status_code": 403, "action": wait_to_continue},
                    ],
                    "params": {
                        "category_id": {
                            "type": "resolve",
                            "resource": "categories",
                            "field": "id",
                        }
                    },
                },
            },
            {
                "name": "products",
                "write_disposition": 'replace',
                "endpoint": {
                    "path": "products/{product_id}/",
                    "method": "GET",
                    "paginator": "single_page",
                    "params": {
                        "product_id": {
                            "type": "resolve",
                            "resource": "products_history",
                            "field": "id"
                        }
                    },
                },
            },
        ],
    }

    yield from rest_api_resources(config)
