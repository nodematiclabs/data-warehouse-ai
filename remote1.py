import functions_framework

from google.cloud import translate_v2 as translate

translate_client = translate.Client()

@functions_framework.http
def entrypoint(request):
    """HTTP Cloud Function.
    Args:
        request (flask.Request): The request object.
        <https://flask.palletsprojects.com/en/1.1.x/api/#incoming-request-data>
    Returns:
        The response text, or any set of values that can be turned into a
        Response object using `make_response`
        <https://flask.palletsprojects.com/en/1.1.x/api/#flask.make_response>.
    """
    request_json = request.get_json(silent=True)

    if request_json and 'calls' in request_json:
      results = []
      for call in request_json['calls']:
        translation = translate_client.translate(call[0], target_language="en")
        results.append(translation['translatedText'])
      return {'replies': results}
    else:
      return ""