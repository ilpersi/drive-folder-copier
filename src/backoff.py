# third parties libraries
import tenacity

from socket import timeout

# whre all the bad requests are tried again...
# https://developers.google.com/drive/api/v3/handle-errors#exponential-backoff
MAX_ATTEMPTS = 30
EXP_MULTIPLIER = 0.5
EXP_MAX_WAIT = 60

# In what case should tenacity try again?
retry_exceptions = (

    # https://developers.google.com/drive/api/v3/handle-errors#403_user_rate_limit_exceeded
    tenacity.retry_if_exception_message(match=r".+?User Rate Limit Exceeded\.")
    | tenacity.retry_if_exception_message(match=r".+?Rate limit exceeded\.")

    # https://developers.google.com/drive/api/v3/handle-errors#500_backend_error
    | tenacity.retry_if_exception_message(match=r".+?Internal Error")
    | tenacity.retry_if_exception_message(match=r".+?Transient failure")
    | tenacity.retry_if_exception_message(match=r".+?The read operation timed out")
    | tenacity.retry_if_exception_message(match=r".+?HttpError 500")

    | tenacity.retry_if_exception_type(timeout)
    | tenacity.retry_if_exception_type(ConnectionResetError)
)


def before_sleep_print(retry_state):
    """Before call strategy that logs to some logger the attempt."""

    if retry_state.outcome.failed:
        verb, value = 'raised', retry_state.outcome.exception()
    else:
        verb, value = 'returned', retry_state.outcome.result()

    print("Retrying in {} seconds as it {} {}.".format(getattr(retry_state.next_action, 'sleep'), verb, value))


@tenacity.retry(stop=tenacity.stop_after_attempt(MAX_ATTEMPTS),
                wait=tenacity.wait_exponential(multiplier=EXP_MULTIPLIER, max=EXP_MAX_WAIT),
                retry=retry_exceptions, before_sleep=before_sleep_print)
def call_endpoint(endpoint, params):
    return endpoint(**params).execute()


@tenacity.retry(stop=tenacity.stop_after_attempt(MAX_ATTEMPTS),
                wait=tenacity.wait_exponential(multiplier=EXP_MULTIPLIER, max=EXP_MAX_WAIT),
                retry=retry_exceptions, before_sleep=before_sleep_print)
def execute_request(request):
    return request.execute()


def execute_request_with_logger(request, logger, level):

    @tenacity.retry(stop=tenacity.stop_after_attempt(MAX_ATTEMPTS),
                    wait=tenacity.wait_exponential(multiplier=EXP_MULTIPLIER, max=EXP_MAX_WAIT),
                    retry=retry_exceptions, before_sleep=tenacity.before_sleep_log(logger, level))
    def _execute():
        return request.execute()

    return _execute()
