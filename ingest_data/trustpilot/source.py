import re
from typing import Any, Dict, List, Optional

import dlt
from dlt.common import jsonpath
from dlt.sources.helpers import requests
from dlt.sources.helpers.requests import Request, Response
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator, SinglePagePaginator
from dlt.sources.rest_api import RESTAPIConfig, rest_api_resources


class CustomPaginator(PageNumberPaginator):
    """
    This paginator does exactly the same as PageNumberPaginator,
    but with one difference: The first request hasn't to have the param 'page' and
    the next has to have page = 2.
    """

    def init_request(self, request: Request) -> None:
        super().init_request(request)
        request.params[self.param_name] = None


HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept-Language": "es-ES,es;q=0.9",
}


def get_build_id(url="https://www.trustpilot.com/review/singularu.com"):
    response = requests.get(url, headers=HEADERS)

    build_id = re.search(r'"buildId":\s?"([^"]+)"', response.text).group(1)
    return build_id


@dlt.source(name="trustpilot")
def source_trustpilot() -> Any:
    @dlt.resource(selected=False)
    def stores():
        """A seed list of stores to fetch"""
        a = ['singularu.com']
        b = [{"build_id": get_build_id(),"site": url} for url in a]
        yield b
    
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://www.trustpilot.com/_next/data/",
            "auth": None,
            "headers": HEADERS,
        },
        # The default configuration for all resources and their endpoints
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
                "name": "pagina",
                "endpoint": {
                    "path": "{build_id}/review/{store}.json",
                    "data_selector": "pageProps",
                    "method": "GET",
                    "params": {
                        "languages": "all",
                        "store": {
                            "type": "resolve",
                            "resource": "stores",
                            "field": "site",
                        },
                        "build_id": {
                            "type": "resolve",
                            "resource": "stores",
                            "field": "build_id",
                        },
                    },"paginator": SinglePagePaginator()
                },
            },
            {
                "name": "reviews",
                "include_from_parent": ["build_id"],
                "endpoint": {
                    "path": "{build_id}/review/{store}.json",
                    "data_selector": "pageProps.reviews",
                    "method": "GET",
                    "params": {
                        "languages": "all",
                        "store": {
                            "type": "resolve",
                            "resource": "stores",
                            "field": "site",
                        },
                        "build_id": {
                            "type": "resolve",
                            "resource": "stores",
                            "field": "build_id",
                        },
                    },
                    "paginator": CustomPaginator(
                        base_page=1,
                        total_path="pageProps.filters.pagination.totalPages",
                    ),
                },
            },
            stores(),
            {
                "name": "customers",
                "endpoint": {
                    "path": "{build_id}/users/{customer_id}.json",
                    "data_selector": "pageProps",
                    "method": "GET",
                    "params": {
                        "languages": "all",
                        "customer_id": {
                            "type": "resolve",
                            "resource": "reviews",
                            "field": "consumer.id",
                        },
                        "build_id": {
                            "type": "resolve",
                            "resource": "reviews",
                            "field": "_stores_build_id",
                        },
                    },
                    "paginator": SinglePagePaginator(),
                    "response_actions": [
                        {"status_code": 404, "action": "ignore"},
                        {"status_code": 410, "action": "ignore"},
                        {"status_code": 308, "action": "ignore"},
                    ],
                },
            },
        ],
    }

    yield from rest_api_resources(config)