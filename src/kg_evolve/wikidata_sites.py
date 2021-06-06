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

import gzip
import re
from ast import literal_eval
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Mapping, Optional, TypeVar
from urllib.parse import quote as urlquote

if TYPE_CHECKING:
    F = TypeVar("F", bound=Callable[..., Any])

    def JOverride(_f: F) -> F:  # noqa: N802
        ...


from jpype import JImplements, JOverride  # type: ignore # noqa: F811


@dataclass
class WikidataSite:
    site_id: int
    site_global_key: str
    site_type: str
    site_group: str
    site_source: str
    site_language: str
    site_protocol: str
    site_domain: str
    site_data: str
    site_forward: str
    site_config: str
    file_path: str = ""
    page_path: str = ""

    def __post_init__(self) -> None:
        match = re.match(
            r"""^a:1:{s:5:"paths";a:2:{"""
            r"""s:9:"file_path";s:\d+:"([^"]*)";"""
            r"""s:9:"page_path";s:\d+:"([^"]*)";"""
            r"""}}""",
            self.site_data,
        )
        assert match
        self.file_path = match[1]
        self.page_path = match[2]


@JImplements("org.wikidata.wdtk.datamodel.interfaces.Sites", deferred=True)
@dataclass
class WikidataSites:
    sites: Mapping[str, WikidataSite]

    @JOverride
    def setSiteInformation(  # noqa: N802
        self,
        siteKey: str,  # noqa: N803
        group: str,
        languageCode: str,  # noqa: N803
        siteType: str,  # noqa: N803
        filePath: str,  # noqa: N803
        pagePath: str,  # noqa: N803
    ) -> None:
        raise NotImplementedError("getSiteInformation")

    @JOverride
    def getLanguageCode(self, siteKey: str) -> Optional[str]:  # noqa: N802, N803
        return self.sites[siteKey].site_language if siteKey in self.sites else None

    @JOverride
    def getGroup(self, siteKey: str) -> Optional[str]:  # noqa: N802, N803
        return self.sites[siteKey].site_group if siteKey in self.sites else None

    @JOverride
    def getPageUrl(  # noqa: N802
        self, siteKey: str, pageTitle: str  # noqa: N803
    ) -> Optional[str]:
        site = self.sites.get(siteKey)
        if site is None:
            return None
        encoded_page_title = (
            (
                urlquote(pageTitle.replace(" ", "_"), encoding="UTF-8")
                .replace("%3A", ":")
                .replace("%2F", "/")
            )
            if site.site_type == "mediawiki"
            else urlquote(pageTitle, encoding="UTF-8")
        )
        return site.page_path.replace("$1", encoded_page_title)

    @JOverride
    def getSiteLinkUrl(self, siteLink: object) -> str:  # noqa: N802, N803
        raise NotImplementedError("getSiteLinkUrl")

    @JOverride
    def getFileUrl(  # noqa: N802
        self, siteKey: str, fileName: str  # noqa: N803
    ) -> Optional[str]:
        return (
            self.sites[siteKey].file_path.replace("$1", fileName)
            if siteKey in self.sites
            else None
        )

    @JOverride
    def getSiteType(self, siteKey: str) -> Optional[str]:  # noqa: N802, N803
        return self.sites[siteKey].site_type if siteKey in self.sites else None

    @classmethod
    def from_sql(cls, file: Path) -> WikidataSites:
        # Implementation of org.wikidata.wdtk.dumpfiles.MwSitesDumpFileProcessor.
        with gzip.open(file, "rt", encoding="UTF-8") as fin:
            for line in fin:
                if line.startswith("INSERT INTO `sites` VALUES"):
                    break
            else:
                raise Exception("Could not find INSERT-statement.")

        sites = {}
        for match in re.finditer(r"\([^)]*\)", line):
            site = WikidataSite(*literal_eval(match[0]))
            sites[site.site_global_key] = site
        return WikidataSites(sites)
