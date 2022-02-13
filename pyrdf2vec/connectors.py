import asyncio
import operator
from abc import ABC, abstractmethod
from functools import partial
from tracemalloc import stop
from typing import Dict, List, Optional, Tuple, Union
from urllib import parse

import aiohttp
import attr
import numpy as np
import requests
import sys
from cachetools import Cache, TTLCache, cachedmethod
from cachetools.keys import hashkey
from random import random

from pyrdf2vec.typings import Literal, Response


@attr.s
class Connector(ABC):
    """Base class of the connectors.

    Attributes:
        _asession: The aiohttp session to use for asynchrone requests.
            Defaults to None.
        _headers: The HTTP headers to use.
            Defaults to {"Accept": "application/sparql-results+json"}.
        cache: The policy and size cache to use.
            Defaults to TTLCache(maxsize=1024, ttl=1200).
        endpoint: The endpoint to execute the queries.

    """

    endpoint = attr.ib(
        type=str,
        validator=attr.validators.instance_of(str),
    )

    cache = attr.ib(
        kw_only=True,
        type=Cache,
        factory=lambda: TTLCache(maxsize=1024, ttl=1200),
        validator=attr.validators.optional(attr.validators.instance_of(Cache)),
    )

    randomness = attr.ib(
        kw_only=True,
        type=float,
        default=1.0,
        validator=attr.validators.instance_of(float),
    )

    query_string = attr.ib(
        kw_only=True,
        type=Optional[str],
        default='/query?query=',
        validator=attr.validators.optional(attr.validators.instance_of(str)),
    )

    _headers = attr.ib(
        init=False,
        type=Dict[str, str],
        repr=False,
        default={"Accept": "application/sparql-results+json"},
    )

    _asession = attr.ib(init=False, default=None)

    async def close(self) -> None:
        """Closes the aiohttp session."""
        await self._asession.close()

    @abstractmethod
    def fetch(self, query: str):
        """Fetchs the result of a query.

        Args:
            query: The query to fetch the result

        Returns:
            The JSON response.

        Raises:
            NotImplementedError: If this method is called, without having
                provided an implementation.

        """
        raise NotImplementedError("This must be implemented!")


@attr.s
class SPARQLConnector(Connector):
    """Represents a SPARQL connector.

    Attributes:
        _asession: The aiohttp session to use for asynchrone requests.
            Defaults to None.
        _headers: The HTTP headers to use.
            Defaults to {"Accept": "application/sparql-results+json"}.
        cache: The policy and size cache to use.
            Defaults to connectors.TTLCache(maxsize=1024, ttl=1200).
        endpoint: The endpoint to execute the queries.

    """

    async def afetch(self, queries: List[str]) -> List[List[Response]]:
        """Fetchs the result of SPARQL queries asynchronously.

        Args:
            queries: The queries.

        Returns:
            The response of the queries.

        """
        if self._asession is None:
            self._asession = aiohttp.ClientSession(raise_for_status=True)
        return await asyncio.gather(*(self._fetch(query) for query in queries))

    async def _fetch(self, query) -> Response:
        """Fetchs the result of a SPARQL query with the aiohttp session.

        This function is useful only to avoid unnecessarily filling the fetch
        function's cache with values that can never be retrieved because of a
        different session that uses a coroutine.

        Args:
            query: The query to fetch the result.

        Returns:
            The response of the query in a JSON format.

        """
        if self.endpoint == 'https://query.wikidata.org/sparql':
            url = f"{self.endpoint}?format=json&{self.query_string}={parse.quote(query)}"
        else:
            url = f"{self.endpoint}{self.query_string}{parse.quote(query)}"
        async with self._asession.get(url, headers=self._headers) as res:
            return await res.json()

    @cachedmethod(operator.attrgetter("cache"), key=partial(hashkey, "fetch"))
    def fetch(self, query: str) -> Response:
        """Fetchs the result of a SPARQL query.

        Args:
            query: The query to fetch the result.

        Returns:
            The response of the query in a JSON format.

        """
        
        if self.endpoint == 'https://query.wikidata.org/sparql':
            url = f"{self.endpoint}?format=json&{self.query_string}={parse.quote(query)}"
        else:
            url = f"{self.endpoint}{self.query_string}{parse.quote(query)}"
        
        if self.endpoint == 'https://query.wikidata.org/sparql':
            try:
                with requests.get(url, headers = {'User-agent': 'gerry/1.0 (a.gherardi9@campus.unimib.it) pyrdf2vec/1.0'}) as res:
                    return res.json()
            except:
                with requests.get(url, headers = {'User-agent': 'gerry/1.0 (a.gherardi9@campus.unimib.it) pyrdf2vec/1.0'}) as res:
                    print(res.status_code)
                    print(res.headers)
        else:
            with requests.get(url, headers = self._headers) as res:
                return res.json()

    def get_query(self, entity: str, preds: Optional[List[str]] = None, verbose: int = 0) -> str:
        """Gets the SPARQL query for an entity.

        Args:
            entity: The entity to get the SPARQL query.
            preds: The predicate chain to fetch a literal
                Defaults to None.

        Returns:
            The SPARQL query for the given entity.

        """
        query = f"SELECT ?p ?o WHERE {{ <{entity}> ?p "
        if preds:
            query = f"SELECT ?o WHERE {{ <{entity}> <{preds[0]}> "
            for i in range(1, len(preds)):
                query += f"?o{i} . ?o{i} <{preds[i]}> "
        query += "?o . "

        if 'www.wikidata.org' in entity:
            query += "FILTER(CONTAINS(STR(?o), \"wikidata.org/entity/Q\")) "    
        else:
            r = random()
            if self.randomness >= r:
                query += "FILTER EXISTS { ?o owl:sameAs ?WikidataEntity . FILTER(CONTAINS(STR(?WikidataEntity), \"wikidata.org/entity\")) } . "
            else:
                query += "FILTER NOT EXISTS { ?o owl:sameAs ?WikidataEntity . FILTER(CONTAINS(STR(?WikidataEntity), \"wikidata.org/entity\")) } . "
                query += "FILTER(CONTAINS(STR(?o), \"http://dbpedia.org/resource/\")) . "
                query += "FILTER(!CONTAINS(STR(?o), \"http://dbpedia.org/resource/File:\")) . "
                query += "FILTER(!CONTAINS(STR(?o), \"http://dbpedia.org/resource/Template:\")) . "

        query += "}"
        return query

    def res2literals(self, res) -> Union[Literal, Tuple[Literal, ...]]:
        """Converts a JSON response server to literal(s).

        Args:
            res: The JSON response.

        Returns:
            The literal(s).

        """
        if len(res) == 0:
            return np.NaN
        literals = []
        for literal in res:
            try:
                literals.append(float(literal["o"]["value"]))
            except Exception:
                literals.append(literal["o"]["value"])
        if len(literals) > 1:
            return tuple(literals)
        return literals[0]
