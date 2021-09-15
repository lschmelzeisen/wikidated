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

from __future__ import annotations

from contextlib import contextmanager
from hashlib import sha512
from logging import getLogger
from pathlib import Path
from platform import system
from subprocess import DEVNULL, PIPE
from tarfile import TarFile
from tempfile import NamedTemporaryFile, TemporaryFile
from typing import IO, Collection, Iterator, NamedTuple, Optional

from wikidated._utils import download_file_with_progressbar, external_process, hashcheck

_LOGGER = getLogger(__name__)

# From: https://maven.apache.org/download.cgi
_MAVEN_VERSION = "3.8.2"
_MAVEN_BIN_ARCHIVE_URL = (
    f"https://dlcdn.apache.org/maven/maven-3/{_MAVEN_VERSION}/binaries/"
    f"apache-maven-{_MAVEN_VERSION}-bin.tar.gz"
)
_MAVEN_BIN_ARCHIVE_SHA512 = (
    "b0bf39460348b2d8eae1c861ced6c3e8a077b6e761fb3d4669be5de09490521a"
    "74db294cf031b0775b2dfcd57bd82246e42ce10904063ef8e3806222e686f222"
)


class JavaArtifact(NamedTuple):
    group_id: str
    artifact_id: str
    version: str


class JavaDependencyDownloader:
    def __init__(self, *, jars_dir: Path, maven_dir: Path) -> None:
        self.jars_dir = jars_dir
        self.maven_dir = maven_dir

    def download_java_dependencies(self, artifacts: Collection[JavaArtifact]) -> None:
        if self.are_artifacts_present(artifacts):
            _LOGGER.debug("JARs are already present, skipping download...")
            return

        self._download_maven()

        _LOGGER.debug("Downloading JARs with Maven...")
        self._download_artifacts_with_maven(artifacts)

        _LOGGER.debug("  Done.")

    def are_artifacts_present(self, artifacts: Collection[JavaArtifact]) -> bool:
        # Check if JARs are present or need to be downloaded still. Since we do not know
        # the transitive dependencies yet we use the presence of the JARs we are given
        # as a heuristic for whether everything is there.
        for artifact in artifacts:
            if not (
                self.jars_dir / f"{artifact.artifact_id}-{artifact.version}.jar"
            ).exists():
                return False
        return True

    def _maven_bin_path(self) -> Path:
        if system() == "Windows":
            return self.maven_dir / f"apache-maven-{_MAVEN_VERSION}" / "bin" / "mvn.cmd"
        return self.maven_dir / f"apache-maven-{_MAVEN_VERSION}" / "bin" / "mvn"

    def _download_maven(self) -> None:
        if self._maven_bin_path().exists():
            _LOGGER.debug("Found Maven executable, skipping download...")
            return

        with TemporaryFile() as maven_bin_archive_fd:
            _LOGGER.debug("Downloading Maven distribution...")
            download_file_with_progressbar(_MAVEN_BIN_ARCHIVE_URL, maven_bin_archive_fd)
            maven_bin_archive_fd.seek(0)

            _LOGGER.debug("Verifying Maven distribution...")
            hashcheck(maven_bin_archive_fd, sha512(), _MAVEN_BIN_ARCHIVE_SHA512)
            maven_bin_archive_fd.seek(0)

            _LOGGER.debug("Extracting Maven distribution...")
            maven_bin_archive = TarFile.open(fileobj=maven_bin_archive_fd)
            maven_bin_archive.extractall(self.maven_dir)

    @staticmethod
    @contextmanager
    def _maven_temp_pom(artifacts: Collection[JavaArtifact]) -> Iterator[Path]:
        pom_fd: Optional[IO[str]] = None
        try:
            with NamedTemporaryFile("w", encoding="UTF-8", delete=False) as pom_fd:
                pom_fd.write(
                    '<?xml version="1.0" encoding="UTF-8"?>\n'
                    '<project xmlns="http://maven.apache.org/POM/4.0.0"\n'
                    '  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"\n'
                    '  xsi:schemaLocation="http://maven.apache.org/POM/4.0.0\n'
                    '    http://maven.apache.org/xsd/maven-4.0.0.xsd">\n'
                    "  <modelVersion>4.0.0</modelVersion>\n\n"
                    "  <groupId>dummy-pom</groupId>\n"
                    "  <artifactId>dummy-pom</artifactId>\n"
                    "  <version>1.0-SNAPSHOT</version>\n\n"
                    "  <dependencies>\n"
                )
                for artifact in artifacts:
                    pom_fd.write(
                        "    <dependency>\n"
                        f"      <groupId>{artifact.group_id}</groupId>\n"
                        f"      <artifactId>{artifact.artifact_id}</artifactId>\n"
                        f"      <version>{artifact.version}</version>\n"
                        "    </dependency>\n"
                    )
                pom_fd.write("""  </dependencies>\n</project>\n""")

            yield Path(pom_fd.name)

        finally:
            if pom_fd:
                Path(pom_fd.name).unlink()

    def _download_artifacts_with_maven(
        self, artifacts: Collection[JavaArtifact]
    ) -> None:
        with self._maven_temp_pom(artifacts) as pom_path, external_process(
            (
                str(self._maven_bin_path()),
                "dependency:copy-dependencies",
                f"-DoutputDirectory={self.jars_dir.absolute()}",
                f"-Dmaven.repo.local={self.maven_dir / 'repo'}",
                "--file",
                str(pom_path),
                "--quiet",
            ),
            stdin=DEVNULL,
            stdout=PIPE,
            stderr=PIPE,
            name="Maven",
            exhaust_stdout_to_log=True,
            exhaust_stderr_to_log=True,
            check_return_code_zero=True,
        ):
            pass
