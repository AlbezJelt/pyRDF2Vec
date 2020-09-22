from typing import List, Sequence

import rdflib
from gensim.models.word2vec import Word2Vec
from sklearn.utils.validation import check_is_fitted

from pyrdf2vec.graphs import RDFLoader, Vertex
from pyrdf2vec.samplers import UniformSampler
from pyrdf2vec.walkers import RandomWalker, Walker


class RDF2VecTransformer:
    """Transforms nodes in a knowledge graph into an embedding.

    Attributes:
        vector_size: The dimension of the embeddings.
            Defaults to 500.
        walkers: The walking strategy.
            Defaults to pyrdf2vec.walkers.RandomWalker(2, None,
            UniformSampler(inverse=False)).
        n_jobs: The number of threads to train the model.
            Defaults to 1.
        sg: The training algorithm. 1 for skip-gram; otherwise CBOW.
            Defaults to 1.
        max_iter: The number of iterations (epochs) over the corpus.
            Defaults to 10.
        negative: The negative sampling.
            If > 0, the negative sampling will be used. Otherwise no negative
            sampling is used.
            Defaults to 25.
        min_count: The total frequency to ignores all words.
            Defaults to 1.

    """

    def __init__(
        self,
        vector_size: int = 500,
        walkers: Sequence[Walker] = [
            RandomWalker(2, None, UniformSampler(inverse=False))
        ],
        n_jobs: int = 1,
        window: int = 5,
        sg: int = 1,
        max_iter: int = 10,
        negative: int = 25,
        min_count: int = 1,
    ):
        self.max_iter = max_iter
        self.min_count = min_count
        self.n_jobs = n_jobs
        self.negative = negative
        self.sg = sg
        self.vector_size = vector_size
        self.walkers = walkers
        self.window = window

    def fit(
        self,
        kg: RDFLoader,
        instances: List[rdflib.URIRef],
        verbose: bool = False,
    ) -> "RDF2VecTransformer":
        """Fits the embedding network based on provided instances.

        Args:
            kg: The knowledge graph.
                The graph from which the neighborhoods are extracted for the
                provided instances.
            instances: The instances to create the embedding.
                The test instances should be passed to the fit method as well.

                Due to RDF2Vec being unsupervised, there is no label leakage.

        """
        if isinstance(kg, RDFLoader) and not all(
            [Vertex(str(instance)) in kg._vertices for instance in instances]
        ):
            raise ValueError("The provided instances must be in the graph")

        self.walks_ = []
        for walker in self.walkers:
            self.walks_ += list(walker.extract(kg, instances))
        sentences = [list(map(str, x)) for x in self.walks_]

        if verbose:
            print(
                f"Extracted {len(self.walks_)} walks"
                " for {len(instances)} instances!"
            )

        self.model_ = Word2Vec(
            sentences,
            size=self.vector_size,
            window=self.window,
            workers=self.n_jobs,
            sg=self.sg,
            iter=self.max_iter,
            negative=self.negative,
            min_count=self.min_count,
            seed=42,
        )
        return self

    def transform(self, instances: List[rdflib.URIRef]) -> List[str]:
        """Constructs a feature vector for the provided instances.

        Args:
            instances: The instances to create the embedding.
                The test instances should be passed to the fit method as well.

                Due to RDF2Vec being unsupervised, there is no label leakage.

        Returns:
            The embeddings of the provided instances.

        """
        check_is_fitted(self, ["model_"])
        if not all([str(inst) in self.model_.wv for inst in instances]):
            raise ValueError(
                "The instances must have been provided to fit() first "
                "before they can be transformed into a numerical vector."
            )
        feature_vectors = []
        for instance in instances:
            feature_vectors.append(self.model_.wv.get_vector(str(instance)))
        return feature_vectors

    def fit_transform(self, kg, instances: List[rdflib.URIRef]) -> List[str]:
        """Creates a Word2Vec model and generate embeddings for the provided
        instances.

        Args:
            kg: The knowledge graph.
                The graph from which we will extract neighborhoods for the
                provided instances.
            instances: The instances to create the embedding.
                The test instances should be passed to the fit method as well.

                Due to RDF2Vec being unsupervised, there is no label leakage.

        Returns:
            The embeddings of the provided instances.

        """
        self.fit(kg, instances)
        return self.transform(instances)
