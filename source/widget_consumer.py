from widget_app_base import WidgetAppBase

class WidgetConsumer(WidgetAppBase):
    def __init__(self) -> None:
        super().__init__()

    def get_requests_from_bucket():
        NotImplementedError()
        # time_running_ms:int = 0
        # DO
        #     TRY
        #         GET request from s3 bucket
        #         CONVERT request to json
        #
        #         IF request['type'] == 'create'
        #             delete_request(request)
        #             create_widget(request)
        #             continue
        #         ELSE IF request['type'] == 'update'
        #             delete_request(request)
        #             update_widget(request)
        #             continue
        #         ELSE IF request['delete'] == 'delete'
        #             delete_request(request)
        #             delete_widget(request)
        #             continue
        #         ELSE
        #             throw ERROR stating unknown request type
        # 
        #         time.sleep(100)
        #     EXCEPT ERROR about unknown request type
        #         log error and continue loop
        #     EXCEPT Ctrl+C 
        #         log user entered shutdown and shut down consumer
        #     EXCEPT anything else
        #         log error and shutdown
        # WHILE time_running_ms != 0 and time_running_ms < self.max_runtime

    def delete_request(request:dict) -> bool:
        NotImplementedError()

    def create_widget(request:dict) -> bool:
        NotImplementedError()

    def __create_widget_s3(request:dict) -> bool:
        NotImplementedError()

    def __create_widget_dynamodb(request:dict) -> bool:
        NotImplementedError()

    def update_widget(request:dict) -> bool:
        NotImplementedError()

    def delete_Widget(request:dict) -> bool:
        NotImplementedError()


def verify_args():
    NotImplementedError()

if __name__ == '__main__':
    print('Hello World!')