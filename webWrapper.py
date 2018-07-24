import urllib.request
import logging

_logger = logging.getLogger()


class RestWrapper():
    def __init__(self, baseURL, requiredParameters=None):
        self.url = baseURL
        if requiredParameters is None:
            requiredParameters = {}
        self.parameters = requiredParameters

    def request(self, endpoint, keyList, timeout=25):
        url = self.url + endpoint

        keyList.update(self.parameters)
        if len(keyList) > 0:
            url += "?"

        url += urllib.parse.urlencode(keyList)
        
        _logger.info("http request to [" + url + "] with timeout " + str(timeout))
        with urllib.request.urlopen(url) as response:
            return response.read()