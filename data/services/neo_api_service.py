import logging
import os
import time
from typing import Optional, List, Dict, Any
from urllib.parse import urljoin

import requests

# Add utils to path for error handling
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'utils'))
from error_handling import (
    handle_errors, APIError, DataProcessingError, 
    handle_api_response, log_and_continue, ErrorSeverity
)
from retry_utils import retry_api_call, RetryConfig
from config import config

logger = logging.getLogger("tekmetric")


class NasaNeoClient:
    """
    A client for the NASA Near Earth Object API.
    """

    def __init__(self, url: Optional[str] = None, api_key: Optional[str] = None):
        self._url = url or config.NASA_API_BASE_URL
        self._api_key = api_key or os.environ.get("API_KEY", config.NASA_API_DEMO_KEY)
        self._session = requests.Session()
        self._last_request_time = 0
        self._min_request_interval = config.API_RATE_LIMIT_INTERVAL

    def __del__(self):
        """
        Close the session.
        """
        self._session.close()

    def _rate_limit(self):
        """
        Simple rate limiting to ensure max requests per second as configured.
        """
        current_time = time.time()
        time_since_last = current_time - self._last_request_time
        if time_since_last < self._min_request_interval:
            sleep_time = self._min_request_interval - time_since_last
            logger.debug(f"Rate limiting: sleeping for {sleep_time:.2f} seconds")
            time.sleep(sleep_time)
        self._last_request_time = time.time()

    @handle_errors("neo_api", ErrorSeverity.HIGH)
    @retry_api_call()
    def get(self, endpoint: str, params: Optional[dict] = None) -> dict:
        """
        Make a GET request to the NASA API with rate limiting and retry logic.
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
            raise APIError(f"Request failed for {endpoint}: {str(exc)}", 
                         status_code=getattr(exc.response, 'status_code', None), 
                         original_error=exc)
        
        return response.json()

    @handle_errors("neo_api", ErrorSeverity.HIGH)
    def fetch_neo_batch(self, page: int = 0, size: Optional[int] = None) -> Dict[str, Any]:
        """
        Fetch a batch of Near Earth Objects from the Browse API.
        :param page: Page number (0-based)
        :param size: Number of objects per page (uses config if None)
        :return: Dictionary containing the API response
        """
        if size is None:
            size = config.BATCH_SIZE
            
        endpoint = config.NASA_API_ENDPOINT
        params = {
            "page": page,
            "size": min(size, 20)  # API limit is 20 per page
        }
        
        logger.info(f"Fetching NEO batch: page={page}, size={size}")
        response = self.get(endpoint, params)
        return handle_api_response(response, endpoint)

    @handle_errors("neo_api", ErrorSeverity.HIGH)
    def fetch_all_neos(self, total_limit: Optional[int] = None, batch_size: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Fetch all Near Earth Objects up to the specified limit using batching.
        :param total_limit: Maximum number of NEOs to fetch (uses config if None)
        :param batch_size: Number of objects per batch (uses config if None)
        :return: List of NEO objects
        """
        if total_limit is None:
            total_limit = config.TOTAL_NEO_LIMIT
        if batch_size is None:
            batch_size = config.BATCH_SIZE
            
        all_neos = []
        page = 0
        batch_size = min(batch_size, 20)  # API limit
        
        while len(all_neos) < total_limit:
            remaining = total_limit - len(all_neos)
            current_batch_size = min(batch_size, remaining)
            
            try:
                response = self.fetch_neo_batch(page, current_batch_size)
                
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
                    
            except APIError as e:
                logger.error(f"API error fetching batch {page}: {e.message}")
                # For API errors, we'll stop fetching but return what we have
                break
            except Exception as e:
                log_and_continue(e, f"fetching batch {page}", ErrorSeverity.MEDIUM)
                # For other errors, continue to next batch
                page += 1
                continue
        
        logger.info(f"Total NEOs fetched: {len(all_neos)}")
        return all_neos[:total_limit]  # Ensure we don't exceed the limit
