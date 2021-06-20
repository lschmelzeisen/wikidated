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

import csv
from typing import Iterator, Mapping

from nasty_utils import DecompressingTextIOWrapper

from wikidata_history_analyzer.datamodel.wikidata_page_meta import WikidataPageMeta
from wikidata_history_analyzer.dumpfiles.wikidata_dump import WikidataDump


class WikidataPageTable(WikidataDump):
    def iter_page_metas(
        self, namespace_titles: Mapping[int, str]
    ) -> Iterator[WikidataPageMeta]:
        assert self.path.exists()

        insert_line_start = "INSERT INTO `page` VALUES ("
        insert_line_end = ");\n"

        with DecompressingTextIOWrapper(
            self.path, encoding="UTF-8", progress_bar=True
        ) as fin:
            for line in fin:
                if not (
                    line.startswith(insert_line_start)
                    and line.endswith(insert_line_end)
                ):
                    continue

                # While the lines that we are parsing here are SQL and not CSV, we
                # basically want to split a string along delimiters, except when the
                # delimiters are quoted, which is exactly what CSV-parsing does.
                for match in csv.DictReader(
                    line[len(insert_line_start) : -len(insert_line_end)].split("),("),
                    fieldnames=[
                        "page_id",
                        "namespace",
                        "title",
                        "restrictions",
                        "is_redirect",
                        "is_new",
                        "random",
                        "touched",
                        "links_updated",
                        "latest_revision_id",
                        "len",
                        "content_model",
                        "lang",
                    ],
                    quotechar="'",
                    escapechar="\\",
                ):
                    namespace = int(match["namespace"])
                    namespace_title = namespace_titles[namespace]
                    yield WikidataPageMeta.construct(
                        namespace=namespace,
                        title=match["title"],
                        prefixed_title=(
                            namespace_title + ":" + match["title"]
                            if namespace_title
                            else match["title"]
                        ),
                        page_id=match["page_id"],
                        restrictions=match["restrictions"],
                        is_redirect=int(match["is_redirect"]),
                        is_new=int(match["is_new"]),
                        random=float(match["random"]),
                        touched=match["touched"],
                        links_updated=(
                            match["links_updated"]
                            if match["links_updated"] != "NULL"
                            else None
                        ),
                        latest_revision_id=match["latest_revision_id"],
                        len=int(match["len"]),
                        content_model=match["content_model"],
                        lang=match["lang"] if match["lang"] != "NULL" else None,
                    )
