import logging as log
import time

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class PrintOperator(BaseOperator):
    @apply_defaults
    def __init__(self, my_argument, *args, **kwargs):
        super(PrintOperator, self).__init__(*args, **kwargs)
        self.my_argument = my_argument

    def execute(self, context):
        print(
            "Hello from MyFirstOperator! Argument passed is - ",
            self.my_argument,
        )


class SleepOperator(BaseOperator):
    @apply_defaults
    def __init__(self, time_sleep, *args, **kwargs):
        super(SleepOperator, self).__init__(*args, **kwargs)
        self.time_sleep = time_sleep

    def execute(self, context):
        print(
            "You need to sleep - ",
            self.time_sleep,
            " seconds."
        )
        time.sleep(int(self.time_sleep))
