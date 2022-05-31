from examples.js.js_validator import Validator
from panini import app as panini_app

app = panini_app.App(
    service_name="js_listen_push",
    host="127.0.0.1",
    port=4222,
    enable_js=True
)

log = app.logger
NUM = 0


# @app.on_start_task()
# async def on_start_task():
#     # Persist messages on 'test.*.stream' subject.
#     await app.nats.js_client.add_stream(name="sample-stream-1", subjects=["test.*.stream"])
#
#
# def get_message():
#     return {
#         "id": app.nats.client.client_id,
#     }


# @app.timer_task(interval=1)
# async def publish_periodically():
#     subject = "test.app2.stream"
#     message = get_message()
#     global NUM
#     NUM += 1
#     message['counter'] = NUM
#     await app.publish(subject=subject, message=message)
#     # log.info(f"sent {message}")


def get_message():
    return {
        "id": app.nats.client.client_id,
    }


def validation_error_cb(msg, error):
    print("Message: ", msg, "\n\n Error: ", error)



# Multiple subscribers
@app.listen("test.*.stream", workers_count=10, validator=Validator, validation_error_cb=validation_error_cb)
async def print_msg(msg, worker_uuid):
    print(f"got JS message {worker_uuid}! {msg.subject}:{msg.data}")
    await msg.ack()


# One subscribers
# @app.listen("test.*.stream", validator=Validator, validator_schema = OBJECT_SCHEMA, validation_error_cb= validation_error_cb)
# async def subscribe_to_js_stream_push(msg):
#     async def cb(msg):
#         log.info(f"got JS message ! {msg.subject}:{msg.data}")
#         await msg.ack()
#     await app.nats.js_client.subscribe("test.*.stream", queue="consumer-3", cb=cb, durable='consumer-3', stream="sample-stream-1")
#

if __name__ == "__main__":
    app.start()
