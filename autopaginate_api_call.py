from collections.abc import Generator
import json


class AutoPaginate(Generator):
    def __init__(self,
                 session,
                 url: str,
                 pagination_type: str,
                 data_path,
                 paging_param_name=None,
                 cursor_path=None,
                 extra_params=None,
                 extra_headers=None,
                 ):

        self.url = url
        self.pagination_type = pagination_type
        self.session = session
        self.data_path = data_path
        self.raw_page = None
        self.extra_headers = extra_headers
        self.extra_params = extra_params
        self.paging_param = paging_param_name
        self.is_last_page = False
        self.cursor_path = cursor_path

        # page number pagination
        self.next_page_number = 1

        # offset pagination
        self.offset = 0
        self.step_size = None

        # both of the above
        self.num_returned = 9999

        # url pagination
        self.next_url = None

        # cursor pagination
        self.next_cursor = None

        # data to return
        self.page_data = []

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
        elif self.pagination_type == "url":
            self._get_url_page()
        elif self.pagination_type == "page_number":
            self._get_num_page()
        else:
            raise ValueError('Unknown pagination type, try "page_number", "offset", "cursor", or "url"')

    def _get_num_page(self):
        if self.page_data:
            raise Exception("something went wrong, and it pulled a new page before running out of things to return")

        headers = self.extra_headers
        params = {self.paging_param: self.next_page_number}
        params.update(self.extra_params)
        raw_response = self.session.get(self.url, headers=headers, params=params)
        self.page_data = self.content_into_list(raw_response)
        self.num_returned = len(self.page_data)

        self.next_page_number += 1

        # if it's the first page, set expected page size
        if not self.step_size:
            self.step_size = self.num_returned

        if self.num_returned <= self.step_size // 1.25:
            self.is_last_page = True

    def _get_offset_page(self):
        raise NotImplementedError

    def _get_cursor_page(self):
        headers = self.extra_headers
        if self.next_cursor:
            params = {self.paging_param: self.next_cursor}
        else:
            params = {}
        params.update(self.extra_params)
        raw_response = self.session.get(self.url, headers=headers, params=params)
        self.page_data = self.content_into_list(raw_response)

        parsed_response = json.loads(raw_response.content)

        try:
            self.next_cursor = self.get_value_from_path(parsed_response, self.cursor_path)
        except KeyError:
            self.is_last_page = True

    def _get_url_page(self):
        raise NotImplementedError

    def content_into_list(self, raw_page):
        # override this if you want something nuanced
        content = json.loads(raw_page.content)

        output = self.get_value_from_path(content, self.data_path)
        return output

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
