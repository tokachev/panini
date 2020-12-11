import json
import logging
import logging.handlers
import logging.config
import os
from multiprocessing import Process, Queue, Event
from ..utils import helper


class Handler:
    @staticmethod
    def handle(record) -> None:
        if record.name == "root":
            logger = logging.getLogger()
        else:
            logger = logging.getLogger(record.name)

        if logger.isEnabledFor(record.levelno):
            logger.handle(record)


class Logger:
    """Generate logging systems which can be simply customized by adding config/log_config.json file to app_root_path
    self.name: string, name of the logger,
    self.app_root_path: string, path to the app folder with main script
    self.in_separate_process: Do logging in separate process? (with all pros and cons of that - advanced topic).
    return: logging object, contain rules for logging.
    """

    def __init__(self,
                 name: str,
                 app_root_path: str = None,
                 in_separate_process: bool = False,
                 ):
        self.name = name
        if "CLIENT_ID" in os.environ:
            self.client_id = os.environ["CLIENT_ID"]
        else:
            self.client_id = name

        self.in_separate_process = in_separate_process
        if app_root_path is not None:
            self.app_root_path = app_root_path
        else:
            self.app_root_path = ''

        self.log_root_path = os.path.join(self.app_root_path, 'logs')
        helper.create_dir_when_none(self.log_root_path)

        custom_config_path = os.path.join(self.app_root_path, 'config', 'log_config.json')
        if os.path.exists(custom_config_path):
            config = self._configure_logging_with_custom_config_file(custom_config_path)

        else:
            config = self._configure_default_logging()

        if self.in_separate_process:
            processes_queue, stop_event, listener_process = self._set_logger_in_separate_process(config)
            self.logger = self._get_process_logging_config(processes_queue, self.name)
        else:
            logging.config.dictConfig(config)
            self.logger = logging.getLogger(self.name)

    def get_logger(self, name: str = None):
        """Simply returns current logger, if name is None, or set self.logger as logger with @param:name"""
        if name is None:
            return self
        # TODO: add checking, that logger with @param:name exist
        self.logger = logging.getLogger(name)
        return self

    def debug(self, message, **extra):
        self.logger.debug(message, extra=extra)

    def info(self, message, **extra):
        self.logger.info(message, extra=extra)

    def warning(self, message, **extra):
        self.logger.warning(message, extra=extra)

    def error(self, message, **extra):
        self.logger.error(message, extra=extra)

    def exception(self, message, **extra):
        self.logger.exception(message, extra=extra)

    def _configure_default_logging(self):
        default_log_config = {
            "version": 1,
            "disable_existing_loggers": True,
            "formatters": {
                "detailed": {
                    "class": "pythonjsonlogger.jsonlogger.JsonFormatter",
                    "format": "%(created)s %(name)s %(levelname)s %(processName)s %(threadName)s %(message)s"
                },
                "simple": {
                    "class": "logging.Formatter",
                    "format": "%(asctime)s %(name)-15s %(levelname)-8s %(message)s"
                }
            },
            "handlers": {
                "console": {
                    "class": "logging.StreamHandler",
                    "level": "WARNING",
                    "formatter": "simple",
                    "stream": "ext://sys.stdout"
                },
                "anthill": {
                    "level": "DEBUG",
                    "class": "logging.handlers.RotatingFileHandler",
                    "filename": os.path.join(self.log_root_path, f"anthill.log"),
                    "mode": "a",
                    "formatter": "detailed",
                    "maxBytes": 1000000,
                    "backupCount": 10,
                },
                "inter_services_request": {
                    "level": "DEBUG",
                    "class": "logging.handlers.RotatingFileHandler",
                    "filename": os.path.join(self.log_root_path, f"inter_services_request.log"),
                    "mode": "a",
                    "formatter": "detailed",
                    "maxBytes": 1000000,
                    "backupCount": 10,
                },
                "errors": {
                    "class": "logging.FileHandler",
                    "filename": os.path.join(self.log_root_path, f"errors.log"),
                    "mode": "a",
                    "level": "ERROR",
                    "formatter": "detailed"
                }
            },
            "loggers": {
                "anthill": {
                    "handlers": ["anthill"]
                },
                "inter_services_request": {
                    "handlers": ["inter_services_request"]
                }
            },
            "root": {
                "level": "DEBUG",
                "handlers": ["console", "errors"]
            }
        }
        if self.name not in ("anthill", "inter_services_request"):
            default_log_config["handlers"][self.name] = {
                "level": "DEBUG",
                "class": "logging.handlers.RotatingFileHandler",
                "filename": os.path.join(self.log_root_path, f"{self.name}.log"),
                "mode": "a",
                "formatter": "detailed",
                "maxBytes": 1000000,
                "backupCount": 10
            }

            default_log_config["loggers"][self.name] = {
                "handlers": [self.name]
            }

        return default_log_config

    def _emergency_logging(self) -> None:
        logging.basicConfig(
            format='%(asctime)s %(name)s %(levelname)s %(message)s',
            handlers=[logging.FileHandler(os.path.join(self.log_root_path, 'logging_error.log'), mode='a'),
                      logging.StreamHandler()],
            level=logging.DEBUG)

    def _modify_custom_config(self, config):
        for handler in config['handlers']:
            if handler == 'console':
                continue

            if not os.path.isabs(config['handlers'][handler]['filename']):
                config['handlers'][handler]['filename'] = os.path.join(self.log_root_path,
                                                                       config['handlers'][handler]['filename'])

            # TODO: add here parsing with regex and selecting phrases as % or $MS_NAME and % or $CLIENT_ID..

    def _configure_logging_with_custom_config_file(self, custom_config_path) -> dict:
        try:
            with open(custom_config_path, mode='r', encoding='utf-8') as f:
                config = json.load(f)

                self._modify_custom_config(config)

            return config

        except Exception as e:
            self._emergency_logging()
            logging.exception(f'Error when loading the logging configuration: {e}')
            raise SystemExit()

    @staticmethod
    def _dedicated_listener_process(processes_queue: Queue, stop_event: Event, config: dict) -> None:
        logging.config.dictConfig(config)
        listener = logging.handlers.QueueListener(processes_queue, Handler())
        listener.start()
        stop_event.wait()
        listener.stop()

    def _set_logger_in_separate_process(self, config: dict) -> (Queue, Event, Process):
        try:
            processes_queue = Queue()
            stop_event = Event()
            listener_process = Process(target=self._dedicated_listener_process,
                                       name='listener',
                                       args=(processes_queue, stop_event, config))
            listener_process.start()

            return processes_queue, stop_event, listener_process

        except Exception as e:
            self._emergency_logging()
            logging.exception(f'Error when loading the logging configuration: {e}')
            raise SystemExit()

    @staticmethod
    def _get_process_logging_config(processes_queue: Queue, name: str):
        config = {
            'version': 1,
            'disable_existing_loggers': True,
            'handlers': {
                'queue': {
                    'class': 'logging.handlers.QueueHandler',
                    'queue': processes_queue
                }
            },
            "root": {
                'handlers': ['queue'],
            }
        }
        logging.config.dictConfig(config)
        return logging.getLogger(name)
