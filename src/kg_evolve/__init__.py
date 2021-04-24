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

__version__ = "dev"
try:
    from kg_evolve._version import __version__
except ImportError:
    pass

__version_info__ = tuple(
    (int(part) if part.isdigit() else part)
    for part in __version__.split(".", maxsplit=4)
)


import logging

from kg_evolve.__main__ import main

__all__ = [
    "__version__",
    "main",
]

# Don't show log messages in applications that don't configure logging.
# See https://docs.python.org/3/howto/logging.html#configuring-logging-for-a-library
logging.getLogger(__name__).addHandler(logging.NullHandler())
