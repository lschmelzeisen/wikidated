#
# Copyright 2021 Lukas Schmelzeisen
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from typing import TYPE_CHECKING, Any, Callable, MutableMapping, TypeVar

if TYPE_CHECKING:
    F = TypeVar("F", bound=Callable[..., Any])

    def JOverride(_f: F) -> F:  # noqa: N802
        ...


from logging import LoggerAdapter, getLogger

from jpype import JClass, JImplements, JObject, JOverride  # type: ignore # noqa: F811
from nasty_utils import ColoredBraceStyleAdapter


@JImplements("java.util.logging.Filter", deferred=True)
class JavaLoggingBridge:
    def __init__(self) -> None:
        JSimpleFormatter = JClass("java.util.logging.SimpleFormatter")  # noqa: N806
        self._formatter = JSimpleFormatter()
        self._loggers: MutableMapping[str, LoggerAdapter] = {}

    @JOverride
    def isLoggable(self, record: JObject) -> bool:  # noqa: N802
        name = str(record.getLoggerName())

        logger = self._loggers.get(name)
        if logger is None:
            logger = ColoredBraceStyleAdapter(getLogger(name))
            self._loggers[name] = logger

        logger.log(
            # Transform Java log level to Python log level:
            max(record.getLevel().intValue() / 10 - 60, 10),
            # Format message with potential parameters. (Don't know how to postpone this
            # until after we are sure the message will be displayed.)
            str(self._formatter.formatMessage(record)),
        )

        # Causes the Java-configured logging handler to discard all messages.
        return False


def setup_java_logging_bridge() -> None:
    log_manager = JClass("java.util.logging.LogManager").getLogManager()
    log_manager.reset()

    dummy_handler = JClass("java.util.logging.ConsoleHandler")()
    dummy_handler.setFilter(JavaLoggingBridge())

    root_logger = log_manager.getLogger("")
    root_logger.setLevel(JClass("java.util.logging.Level").ALL)
    root_logger.addHandler(dummy_handler)
