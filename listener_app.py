import json
from json_validation.schemas.object import OBJECT_SCHEMA
from panini import app as panini_app
from panini.validator import Validator

app = panini_app.App(
        service_name='listener_app',
        host='127.0.0.1',
        port=4222,
        enable_js=True
)


def validation_error_cb(msg, error):
    return {"success": False, "error": f"validation_error_cb:,"
                                       f"~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"
                                       f" {error}"
                                       f"~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"}
# async def get_schema(key):
#     schema = await json.loads(app.nats.js_client.key_value("json_schemas").get(key).value.decode('utf-8'))
#     return schema

@app.listen("some.subject.for.request",
            validator=Validator,
            validation_error_cb=validation_error_cb)
async def request_listener(msg):
    """ request endpoint """
    print(f"request {msg.data} from {msg.subject} has been processed")
    return {"success": True, "message": "request has been processed"}

@app.listen("some.subject.for.stream")
async def stream_listener(msg):
    """ stream endpoint """
    print(f"event {msg.data} from {msg.subject} has been processed")

if __name__ == "__main__":
    app.start()