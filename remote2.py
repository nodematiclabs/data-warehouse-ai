import functions_framework

import vertexai
from vertexai.language_models import TextGenerationModel

vertexai.init(project="YOUR_PROJECT", location="us-central1")
parameters = {
    "temperature": 0.2,
    "max_output_tokens": 256,
    "top_p": 0.8,
    "top_k": 40
}
model = TextGenerationModel.from_pretrained("text-bison@001")

@functions_framework.http
def entrypoint(request):
    request_json = request.get_json(silent=True)

    if request_json and 'calls' in request_json:
        results = []
        for call in request_json['calls']:
            response = model.predict(
                call[0],
                **parameters
            )
            results.append(response.text)
        return {'replies': results}
    else:
        return ""