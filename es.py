import json
import requests
import tornado.ioloop
import tornado.web
import concurrent.futures
from kwikapi.tornado import RequestHandler
from kwikapi import API
from typing import Dict, Any, List


class School(object):
    def __init__(self) -> None:
        self.base_url = "http://localhost:9200/"
        self.headers = {"Content-Type": "application/json"}
        self.responses = {}

    def student_crud_operations(
        self, index_name: str, id: str, body: Any, method: str
    ) -> Any:

        if method.lower() == "put":
            url = f"{self.base_url}{index_name}/_doc/{id}"
            response = requests.put(url, headers=self.headers, data=json.dumps(body))
            result = json.loads(response.content)
            return result

        elif method.lower() == "get":
            url = f"{self.base_url}{index_name}/_search"
            response = requests.get(url, headers=self.headers, data=json.dumps(body))
            result = json.loads(response.content)
            return result

        elif method.lower() == "delete":
            url = f"{self.base_url}{index_name}/_delete_by_query"
            response = requests.post(url, headers=self.headers, data=json.dumps(body))
            result = json.loads(response.content)
            return result

        elif method.lower() == "search":

            f"{self.base_url}{index_name}/_search"
            response = requests.post(
                self.base_url, headers=self.headers, data=json.dumps(body)
            )
            result = json.loads(response.content)
            return result

    def using_es_bulk_api(self, body: Any) -> Any:
        url = f"http://localhost:9200/_bulk"

        response = requests.put(url, headers=self.headers, data=json.dumps(body))
        return json.loads(response.content)

    def using_list_of_queries(self, body: Any) -> Any:
        response = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            threads = []
            for key, value in body.items():
                print(value)
                thread = executor.submit(
                    self.student_crud_operations,
                    index_name=value.get("index_name"),
                    id=value.get("id"),
                    body=value.get("body"),
                    method=value.get("method"),
                )
                threads.append(thread)
            for thread in threads:
                response.append(thread.result())
        return response


school_api = API()
school_api.register(School(), "v1")


def make_app():
    return tornado.web.Application(
        [
            (r"^/school_api/.*", RequestHandler, dict(api=school_api)),
        ]
    )


# Starting the application
if __name__ == "__main__":
    app = make_app()
    app.listen(8888)
    tornado.ioloop.IOLoop.current().start()
