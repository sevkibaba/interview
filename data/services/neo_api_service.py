import logging
import os
import time
from typing import Optional, List, Dict, Any
from urllib.parse import urljoin

import requests

logger = logging.getLogger("tekmetric")


class NasaNeoClient:
    """
    A client for the NASA Near Earth Object API.
    """

    BASE_URL = "https://api.nasa.gov"
    DEMO_KEY = "DEMO_KEY"

    def __init__(self, url: str = BASE_URL, api_key: str = DEMO_KEY):
        self._url = url
        self._api_key = os.environ.get("API_KEY", api_key)
        self._session = requests.Session()
        self._last_request_time = 0
        self._min_request_interval = 0.5  # 2 requests per second max

    def __del__(self):
        """
        Close the session.
        """
        self._session.close()

    def _rate_limit(self):
        """
        Simple rate limiting to ensure max 2 requests per second.
        """
        current_time = time.time()
        time_since_last = current_time - self._last_request_time
        if time_since_last < self._min_request_interval:
            sleep_time = self._min_request_interval - time_since_last
            logger.debug(f"Rate limiting: sleeping for {sleep_time:.2f} seconds")
            time.sleep(sleep_time)
        self._last_request_time = time.time()

    def get(self, endpoint: str, params: Optional[dict] = None) -> dict:
        """
        Make a GET request to the NASA API with rate limiting.
        :param endpoint: The API endpoint to call.
        :param params: The query parameters to include in the request.
        :return: The JSON response from the API.
        """
        if params is None:
            params = {}
        params["api_key"] = self._api_key

        self._rate_limit()

        try:
            response = self._session.get(urljoin(self._url, endpoint), params=params)
            logger.debug("GET %s %s", response.url, response.status_code)
            response.raise_for_status()
        except requests.exceptions.RequestException as exc:
            logger.error("Request failed: %s", exc)
            return {'error': {'message': str(exc)}}
        return response.json()

    def fetch_neo_batch(self, page: int = 0, size: int = 20) -> Dict[str, Any]:
        """
        Fetch a batch of Near Earth Objects from the Browse API.
        :param page: Page number (0-based)
        :param size: Number of objects per page (max 20)
        :return: Dictionary containing the API response
        """
        endpoint = "/neo/rest/v1/neo/browse"
        params = {
            "page": page,
            "size": min(size, 20)  # API limit is 20 per page
        }
        
        logger.info(f"Fetching NEO batch: page={page}, size={size}")
        return self.get(endpoint, params)

    def fetch_all_neos(self, total_limit: int = 200, batch_size: int = 20) -> List[Dict[str, Any]]:
        """
        Fetch all Near Earth Objects up to the specified limit using batching.
        :param total_limit: Maximum number of NEOs to fetch
        :param batch_size: Number of objects per batch (max 20)
        :return: List of NEO objects
        """
        all_neos = []
        page = 0
        batch_size = min(batch_size, 20)  # API limit
        
        while len(all_neos) < total_limit:
            remaining = total_limit - len(all_neos)
            current_batch_size = min(batch_size, remaining)
            
            response = self.fetch_neo_batch(page, current_batch_size)
            
            if 'error' in response:
                logger.error(f"Error fetching batch {page}: {response['error']}")
                break
                
            if 'near_earth_objects' not in response:
                logger.warning(f"No 'near_earth_objects' key in response for page {page}")
                break
                
            neos = response['near_earth_objects']
            if not neos:
                logger.info(f"No more NEOs found at page {page}")
                break
                
            all_neos.extend(neos)
            logger.info(f"Fetched {len(neos)} NEOs from page {page}. Total: {len(all_neos)}")
            
            page += 1
            
            # If we got fewer objects than requested, we've reached the end
            if len(neos) < current_batch_size:
                break
        
        logger.info(f"Total NEOs fetched: {len(all_neos)}")
        return all_neos[:total_limit]  # Ensure we don't exceed the limit
