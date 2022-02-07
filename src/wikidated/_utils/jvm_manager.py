#
# Copyright 2021-2022 Lukas Schmelzeisen
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

from __future__ import annotations

from logging import DEBUG, FileHandler, Formatter, Handler, Logger, LogRecord, getLogger
from pathlib import Path
from types import TracebackType
from typing import TYPE_CHECKING, Any, Callable, MutableMapping, Optional, Type, TypeVar

if TYPE_CHECKING:
    _F = TypeVar("_F", bound=Callable[..., Any])

    def JOverride(_f: _F) -> _F:  # noqa: N802
        ...


from jpype import JOverride  # type: ignore # noqa: F811
from jpype import JClass, JImplements, JObject, shutdownJVM, startJVM

_LOGGER = getLogger(__name__)


class JvmManager:
    def __init__(self, *, jars_dir: Path) -> None:
        self._jars_dir = jars_dir

        _LOGGER.debug("Starting JVM.")
        startJVM(classpath=[str(self._jars_dir / "*")])

        self._java_logging_bridge = _JavaLoggingBridge()

    def close(self) -> None:
        assert self  # Stop PyCharm from suggesting to make this method static.

        _LOGGER.debug("Shutting down JVM.")
        shutdownJVM()

    def __enter__(self) -> JvmManager:
        return self

    def __exit__(
        self,
        _exc_type: Optional[Type[BaseException]],
        _exc_val: Optional[BaseException],
        _exc_tb: Optional[TracebackType],
    ) -> None:
        self.close()

    def set_logging_file_handler(
        self, file: Optional[Path], formatter: Optional[Formatter] = None
    ) -> None:
        if not file:
            self._java_logging_bridge.set_file_handler(None)
            return

        file_handler = FileHandler(file)
        file_handler.setFormatter(
            formatter
            or Formatter("{asctime} {levelname:.1} [{name}] {message}", style="{")
        )
        self._java_logging_bridge.set_file_handler(file_handler)


@JImplements("java.util.logging.Filter", deferred=True)
class _JavaLoggingBridge:
    def __init__(self) -> None:
        self._loggers: MutableMapping[str, Logger] = {}
        self._file_handler: Optional[Handler] = None
        self._formatter = JClass("java.util.logging.SimpleFormatter")()

        log_manager = JClass("java.util.logging.LogManager").getLogManager()
        log_manager.reset()

        dummy_handler = JClass("java.util.logging.ConsoleHandler")()
        dummy_handler.setFilter(self)

        root_logger = log_manager.getLogger("")
        root_logger.setLevel(JClass("java.util.logging.Level").ALL)
        root_logger.addHandler(dummy_handler)

    def set_file_handler(self, file_handler: Optional[Handler]) -> None:
        self._file_handler = file_handler

    @JOverride
    def isLoggable(self, record: JObject) -> bool:  # noqa: N802
        name = f"jpype.{record.getLoggerName()}"
        # Format message with potential parameters. (Don't know how to postpone this
        # until after we are sure the message will be displayed.)
        message = f"{record.getLevel()}: {(self._formatter.formatMessage(record))}"

        logger = self._loggers.get(name)
        if logger is None:
            logger = getLogger(name)
            self._loggers[name] = logger

        logger.debug(message)
        if self._file_handler:
            self._file_handler.handle(
                LogRecord(
                    name=name,
                    level=DEBUG,
                    pathname="Unknown.java",
                    lineno=-1,
                    msg=message,
                    args=(),
                    exc_info=None,
                    func=None,
                    sinfo=None,
                )
            )

        # Causes the Java-configured logging handler to discard all messages.
        return False
