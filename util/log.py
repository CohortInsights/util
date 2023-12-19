import logging
import threading
import time
import numpy


class LogEvent:
    log_initialization = False
    """
    Class used for logging of actions, related properties, and time durations
    """

    def __init__(self, event_name, auto_push=False):
        self.event = event_name
        self.last_time = time.time()
        self.time_stack = []
        self.auto_push = auto_push
        if not LogEvent.log_initialization:
            logging.basicConfig(level=logging.WARNING)
            LogEvent.log_initialization = True
        self.logger = logging.getLogger(event_name)
        self.logger.setLevel(logging.INFO)
        self.push_timer()

    def thread_number(self):
        id = threading.get_ident()
        id = str(id)
        return id[len(id)-5:]

    def push_timer(self):
        self.time_stack.append(self.last_time)
        self.last_time = time.time()

    def pop_timer(self):
        self.last_time = self.time_stack[-1]
        del self.time_stack[-1]

    def action_duration(self):
        """
        Get duration since start of the action

        :param action: Name of the action
        :type action: str
        :return: Duration with units since action started
        :rtype: str
        """
        now = time.time()
        duration = numpy.round(now - self.last_time, decimals=2)
        units = ' sec'
        if duration > 60:
            duration /= 60.0
            duration = numpy.round(duration, decimals=2)
            units = ' minutes'
            if duration > 60:
                duration /= 60.0
                duration = numpy.round(duration, decimals=2)
                units = ' hours'
        return str(duration) + units

    def property(self, name, value):
        """
        Log the name value pair associated with an action

        :param name: property name
        :type name: str
        :param value: property value
        :type value: any
        :return: dictionary that was logged
        :rtype: dict
        """
        if self.auto_push:
            self.pop_timer()
        info = {
            'event': self.event,
            'thread' : self.thread_number(),
            name: value,
            'duration': self.action_duration()
        }
        self.logger.info(msg=str(info))
        if self.auto_push:
            self.push_timer()
        return info

    def properties(self, info):
        """
        Log a set of properties related to an action

        :param info: Dictionary of properties to log
        :type info: dict
        :return: info with new state that was logged
        :rtype: dict
        """
        if self.auto_push:
            self.pop_timer()
        event_info = info.copy()
        event_info['event'] = self.event
        event_info['duration'] = self.action_duration()
        event_info['thread'] = self.thread_number()
        self.logger.info(msg=str(event_info))
        if self.auto_push:
            self.push_timer()
        return event_info

    def warning(self, msg, context=None):
        """
        Post warning for an action

        :param context: Optional additional context with error
        :type context: dict
        :return:    Info that was logged with warning
        :rtype: dict
        """
        if self.auto_push:
            self.pop_timer()
        info = {'event': self.event, 'warning': msg, 'duration': self.action_duration()}
        if context is not None:
            info.update(context)
        self.logger.warning(msg=str(info))
        if self.auto_push:
            self.push_timer()
        return info

    def error(self, msg, e=None, context=None):
        """
        Post error condition with exception

        :param e: Exception that occurred in the course of the action
        :type e: Exception
        :param context: Optional additional context with error
        :type context: dict
        :return:    Info that was logged with warning
        :rtype: dict
        """
        if self.auto_push:
            self.pop_timer()
        info = {'event': self.event, 'error': msg, 'duration': self.action_duration()}
        if context is not None:
            info.update(context)
        self.logger.error(msg=str(info), exc_info=e)
        if self.auto_push:
            self.push_timer()
        return info

    def ended(self):
        info = {'ended': self.event, 'duration': self.action_duration(), 'thread' : self.thread_number()}
        self.logger.info(msg=str(info))
        return info
