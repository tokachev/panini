from json_validation.schemas.object import OBJECT_SCHEMA
from panini import app as panini_app
import json

app = panini_app.App(
    service_name="js_publish",
    host="127.0.0.1",
    port=4222,
    enable_js=True
)

log = app.logger
NUM = 0


@app.on_start_task()
async def on_start_task():
    print("Onstartup")
    await app.nats.js_client.add_stream(name="sample-stream-4", subjects=["test.*.stream4"])
    kv_bucket = await app.nats.js_client.create_key_value(bucket="json_schemas")
    schema_json = json.dumps(OBJECT_SCHEMA).encode('utf-8')
    await kv_bucket.put("some.subject.for.request", schema_json)




def get_message():
    return {
        "id": app.nats.client.client_id,
    }


@app.timer_task(interval=5)
async def publish_periodically():
    subject = "test.app2.stream"
    message = get_message()
    global NUM
    NUM+=1
    message['counter'] = NUM
    await app.publish(subject=subject, message=message)
    log.info(f"sent {message}")




if __name__ == "__main__":
    app.start()
