import time
import logging
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from tenacity import retry, stop_after_attempt, wait_exponential_jitter, retry_if_exception_type

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Retry decorator: Retries on rate limiting errors (HttpError 429) with exponential backoff and jitter
@retry(
    stop=stop_after_attempt(5),  # Stop after 5 attempts
    wait=wait_exponential_jitter(),  # Exponential backoff with jitter
    retry=retry_if_exception_type(HttpError),  # Retry only for HttpError exceptions
)
def call_search_all_iam_policies(service, org_id, page_token=None):
    """
    Calls the `searchAllIamPolicies` method of the Google Cloud Asset API with retry logic.
    
    Args:
    - service: Authenticated Google API client service.
    - org_id: Google Cloud Organization ID.
    - page_token: Optional pagination token for large result sets.
    
    Returns:
    - The response from the API call (a dictionary with IAM policies).
    """
    try:
        # Requesting IAM policies for the given organization, with pagination if needed
        logger.info("Calling Cloud Asset API to search IAM policies...")
        request = service.organizations().searchAllIamPolicies(
            parent=f"organizations/{org_id}",
            pageToken=page_token
        )
        response = request.execute()  # Execute the API request
        
        logger.info(f"Fetched {len(response.get('policies', []))} IAM policies.")
        return response  # Return the response data

    except HttpError as e:
        if e.resp.status == 429:  # Handle rate limit exceeded (HTTP 429)
            logger.warning("Rate limit exceeded. Retrying...")
            raise  # Raise exception for Tenacity to handle retry
        else:
            logger.error(f"HTTP error occurred: {e}")
            raise  # Raise other HTTP errors for higher-level handling

def fetch_all_iam_policies(service, org_id):
    """
    Fetch all IAM policies for the given organization with pagination support.
    
    Args:
    - service: Authenticated Google API client service.
    - org_id: Google Cloud Organization ID.
    
    Returns:
    - A list of all IAM policies across all pages.
    """
    all_policies = []  # List to store all IAM policies
    next_page_token = None  # Pagination token
    
    while True:
        # Call the API with pagination support
        response = call_search_all_iam_policies(service, org_id, page_token=next_page_token)
        
        # Append fetched policies to the list
        all_policies.extend(response.get('policies', []))
        
        # Check if there's another page to fetch
        next_page_token = response.get('nextPageToken')
        if not next_page_token:
            break  # No more pages, exit the loop
    
    return all_policies

# Example Usage
if __name__ == "__main__":
    # Initialize the Google API service (Ensure authentication is handled)
    service = build('cloudasset', 'v1')  # Assuming you've already authenticated
    org_id = '123456789012'  # Replace with your actual Google Cloud Organization ID
    
    # Fetch all IAM policies for the organization
    try:
        policies = fetch_all_iam_policies(service, org_id)
        logger.info(f"Total IAM policies retrieved: {len(policies)}")
    except Exception as e:
        logger.error(f"Failed to retrieve IAM policies: {e}")
