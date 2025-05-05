import random
import time
import logging
from tenacity import retry, stop_after_attempt, wait_exponential_jitter, retry_if_exception_type
from googleapiclient.errors import HttpError

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def wait_with_jitter(retry_state):
    base = 2 ** retry_state.attempt_number
    jitter = random.uniform(0, 1)
    wait_time = min(base + jitter, 60) 
    logger.info(f"Retrying in {wait_time:.2f} seconds...")
    time.sleep(wait_time)

@retry(
    retry=retry_if_exception_type(HttpError),
    stop=stop_after_attempt(5),
    wait=wait_exponential_jitter(initial=1, max=60),  
    reraise=True
)
def call_search_all_iam_policies(service, organization_id, page_token=None):
    try:
        request = service.v1().organizations().searchAllIamPolicies(
            scope=f'organizations/{organization_id}',
            pageToken=page_token
        )
        return request.execute()
    except HttpError as e:
        if e.resp.status == 429:
            logger.warning(f"Rate limit hit: {e}")
            raise  
        else:
            logger.error(f"Non-retryable HttpError: {e}")
            raise


def fetch_all_iam_policies(service, organization_id):
    page_token = None
    all_results = []

    while True:
        response = call_search_all_iam_policies(service, organization_id, page_token)
        all_results.extend(response.get('results', []))

        page_token = response.get('nextPageToken')
        if not page_token:
            break

    return all_results
