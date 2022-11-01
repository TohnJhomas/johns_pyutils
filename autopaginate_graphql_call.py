from collections.abc import Generator
import json
from requests import HTTPError
import re


class AutoPaginateGraphQL(Generator):
    def __init__(self,
                 session,
                 url: str,
                 pagination_type: str,
                 data_path,
                 query,
                 paging_variable_name=None,
                 paging_param_path=None,
                 extra_params=None,
                 extra_headers=None,
                 query_variables=None,
                 step_size=None,
                 clean_query=True
                 ):
        self.url = url
        self.pagination_type = pagination_type
        self.session = session
        self.data_path = data_path
        self.query = query
        self.paging_param_path = paging_param_path
        self.paging_param = paging_variable_name
        self.raw_page = None
        self.is_last_page = False


        if query_variables is None:
            self.variables = {}
        else:
            self.variables = query_variables

        if extra_headers is None:
            self.extra_headers = {}
        else:
            self.extra_headers = extra_headers

        if extra_params is None:
            self.extra_params = {}
        else:
            self.extra_params = extra_params

        # offset pagination
        self.offset = 0
        self.step_size = step_size

        # both of the above
        self.num_returned = 9999

        # cursor pagination
        self.next_cursor = None

        # data to return
        self.page_data = []

        if type(self.data_path) == list and self.data_path[0] == "data":
            raise UserWarning("'data' in data_path")
        if clean_query:
            self.clean_gql_query()

    def send(self, ignore):
        # these conditionals look weird, but here's the reasoning:
        # for url pagination, you'll simply not get a next url when you hit the end often,
        # so trying to call the next page and stopping when you get nothing will error.
        # we also try and use it for page_number and limit/offset paging, but the 0 check
        # is needed for the case where it's only one page, since we set the expected page
        # size based on the first call in page_number paging
        if not self.page_data and self.is_last_page:
            raise StopIteration
        elif not self.page_data:
            self._next_page()
        if not self.page_data:
            raise StopIteration

        # remove the first entry in the list and return it
        return self.page_data.pop(0)

    def throw(self, typ, val=None, tb=None):
        super().throw(typ, val, tb)

    def _next_page(self):
        if self.pagination_type == "offset":
            self._get_offset_page()
        elif self.pagination_type == "cursor":
            self._get_cursor_page()
        else:
            raise ValueError('Unknown pagination type, try "offset" or "cursor"')

    def _get_offset_page(self):
        raise NotImplementedError

    def _get_cursor_page(self):
        headers = self.extra_headers
        params = self.extra_params
        if self.next_cursor:
            new_variables = {self.paging_param: self.next_cursor}
        else:
            new_variables = {}

        self.variables.update(new_variables)

        body = "{\"query\":\"" + self.query + "\", \"variables\":" + \
               str(self.variables).replace("'", "\"").replace("\"null\"", "null") + "}"

        raw_response = self.session.post(self.url, headers=headers, params=params, data=body)
        self.page_data = self.content_into_list(raw_response)

        parsed_response = json.loads(raw_response.content)["data"]

        try:
            self.next_cursor = self.get_value_from_path(parsed_response, self.paging_param_path)
        except KeyError:
            self.is_last_page = True

    def content_into_list(self, raw_page):
        # override this if you want something nuanced
        content = json.loads(raw_page.content)

        if "message" in content.keys():
            if content["message"] == "Problems parsing JSON":
                body = "{\"query\":\"" + self.query + "\", \"variables\":" + \
                       str(self.variables).replace("'", "\"") + "}"
                hint = "\nMake sure your query uses all double quotes, " \
                       "and that all double quotes are escaped with double backslash. " \
                       "(they should look like this below: `\\\\\"`)\n" + body
                raise HTTPError("Status {}. ".format(raw_page.status_code) + content["message"] + hint)
            else:
                raise HTTPError("Status {}. ".format(raw_page.status_code) + content["message"])

        if "errors" in content.keys():
            raise HTTPError("Status {}. ".format(raw_page.status_code) + str(content["errors"]))

        content = content["data"]

        output = self.get_value_from_path(content, self.data_path)
        return output

    def clean_gql_query(self):
        self.query = re.sub(r'(?<=\w\w\w)\s*\n\s*(?=\w+)', ",", self.query)
        self.query = self.query.replace("\n", "")
        self.query = self.query.replace(" ", "")
        self.query = re.sub(r'^query', "query ", self.query)
        self.query = re.sub(r'(?<!\\)"', "\\\"", self.query)

    @staticmethod
    def get_value_from_path(content, path):
        if type(path) == str:
            output = content[path]
        elif type(path) == list:
            output = content
            for item in path:
                output = output[item]
        else:
            raise ValueError("Unexpected data type for `path`: {}, use Str or List".format(type(path)))
        return output

