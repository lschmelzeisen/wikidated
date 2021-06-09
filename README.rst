================================================================================
Wikidata History Analyzer
================================================================================

Wikidata History Analyzer is a tool/library for parsing and analyzing the
`Wikidata dumps <https://www.wikidata.org/wiki/Wikidata:Database_download>`_
(the ``pages-meta-history.xml`` format to be concrete).
It supports:

* Serializing the dumps into time streams of RDF triple operations (add/delete)
  both individually per Wikidata entity and globally across all entities.
* Calculating statistics over time (for example, update frequencies of entities,
  lifetimes of triples, etc.)

Currently, while the major functionality is implemented, the API is not yet
ready for public use and in particular is not documented yet.
A first release is planned for the second half of July.
But since you found this repository already yet, why no keep an eye on it, star
it on GitHub, and be automatically notified once it's ready? ;)

License
========================================================================================

Copyright 2021 Lukas Schmelzeisen.
Licensed under the
`Apache License, Version 2.0 <https://www.apache.org/licenses/LICENSE-2.0>`_.
