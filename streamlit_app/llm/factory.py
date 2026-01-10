from __future__ import annotations

from dataclasses import dataclass
import re
import time
from typing import Optional

import pandas as pd

from langchain.agents import create_agent
from langchain.tools import tool
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnableLambda, RunnableParallel, RunnablePassthrough

try:
    from langchain_classic.embeddings import CacheBackedEmbeddings
    from langchain_classic.storage import LocalFileStore
except ImportError:  # pragma: no cover - handled in runtime fallbacks
    CacheBackedEmbeddings = None  # type: ignore[assignment]
    LocalFileStore = None  # type: ignore[assignment]

from ..config import AppConfig
from ..data.loader import DatasetManager


@dataclass(frozen=True)
class ModelConfig:
    provider: str = "auto"
    model_name: str = "gemini-1.5-flash"
    temperature: float = 0.1
    api_key_env: str = "GEMINI_API_KEY"
    api_base_env: Optional[str] = None
    cohere_model_name: str = "command-r-08-2024"
    cohere_api_key_env: str = "COHERE_API_KEY"


@dataclass(frozen=True)
class EmbeddingConfig:
    provider: str = "auto"
    model_name: str = "models/embedding-001"
    api_key_env: str = "GEMINI_API_KEY"
    api_base_env: Optional[str] = None
    cohere_model_name: str = "embed-multilingual-v3.0"
    cohere_api_key_env: str = "COHERE_API_KEY"


class LangChainFactory:
    """Centraliza criação de componentes LangChain usando configurações do app."""

    def __init__(
        self,
        config: AppConfig,
        dataset_manager: DatasetManager,
        model_config: Optional[ModelConfig] = None,
        embedding_config: Optional[EmbeddingConfig] = None,
    ) -> None:
        self._config = config
        self._dataset_manager = dataset_manager
        self._model_config = model_config or ModelConfig()
        self._embedding_config = embedding_config or EmbeddingConfig()
        self._last_chat_provider: Optional[str] = None
        self._last_embedding_provider: Optional[str] = None
        self._blocked_chat_providers: set[str] = set()
        self._blocked_embedding_providers: set[str] = set()
        self._embedding_cache_store: Optional[LocalFileStore] = None

    def build_chat_model(self):
        errors: list[tuple[str, str]] = []
        for candidate in self._chat_provider_candidates():
            if not self._chat_credentials_available(candidate):
                errors.append((candidate, "Credenciais ausentes"))
                continue
            try:
                model = self._build_chat_model_by_provider(candidate)
                self._last_chat_provider = candidate
                return model
            except Exception as exc:  # noqa: BLE001
                self._blocked_chat_providers.add(candidate.lower())
                errors.append((candidate, str(exc)))
        details = "; ".join(f"{name}: {err}" for name, err in errors) or "nenhum provider configurado"
        raise RuntimeError(
            "Não foi possível inicializar o modelo de chat. "
            f"Tentativas: {', '.join(self._chat_provider_candidates(raw=True))}. Detalhes: {details}."
        )

    def build_embeddings(self):
        errors: list[tuple[str, str]] = []
        for candidate in self._embedding_provider_candidates():
            if not self._embedding_credentials_available(candidate):
                errors.append((candidate, "Credenciais ausentes"))
                continue
            try:
                embeddings = self._build_embeddings_by_provider(candidate)
                self._last_embedding_provider = candidate
                return embeddings
            except Exception as exc:  # noqa: BLE001
                self._blocked_embedding_providers.add(candidate.lower())
                errors.append((candidate, str(exc)))
        details = "; ".join(f"{name}: {err}" for name, err in errors) or "nenhum provider configurado"
        raise RuntimeError(
            "Não foi possível inicializar as embeddings. "
            f"Tentativas: {', '.join(self._embedding_provider_candidates(raw=True))}. Detalhes: {details}."
        )

    def load_or_create_vectorstore(self):
        try:
            from langchain_community.vectorstores import FAISS
        except ImportError as exc:  # noqa: F401
            raise RuntimeError(
                "Instale langchain-community e faiss-cpu para usar a vectorstore (pip install langchain-community faiss-cpu)."
            ) from exc

        vector_dir = self._config.vectorstore_dir
        vector_dir.mkdir(parents=True, exist_ok=True)

        errors: list[tuple[str, str]] = []
        for candidate in self._embedding_provider_candidates():
            if not self._embedding_credentials_available(candidate):
                errors.append((candidate, "Credenciais ausentes"))
                continue
            try:
                embeddings = self._build_embeddings_by_provider(candidate)
                self._last_embedding_provider = candidate
                return self._load_vectorstore_for_provider(FAISS, vector_dir, embeddings, candidate)
            except Exception as exc:  # noqa: BLE001
                self._blocked_embedding_providers.add(candidate.lower())
                if candidate.lower() == "cohere":
                    self._blocked_chat_providers.add("cohere")
                errors.append((candidate, str(exc)))
                continue

        details = "; ".join(f"{name}: {err}" for name, err in errors) or "nenhum provider configurado"
        raise RuntimeError(
            "Não foi possível preparar a vectorstore. "
            f"Tentativas: {', '.join(self._embedding_provider_candidates(raw=True))}. Detalhes: {details}."
        )

    def build_sales_agent(self):
        vectorstore = self.load_or_create_vectorstore()
        retriever = vectorstore.as_retriever(search_kwargs={"k": 6})
        agent_model = self.build_chat_model()
        rag_model = agent_model
        rag_chain = self._build_rag_chain(retriever, rag_model)
        rag_tool = self._create_rag_tool(rag_chain)

        system_prompt = (
            "Você é um analista de dados especializado em estratégia comercial. "
            "Use apenas as informações obtidas pela ferramenta consultar_base_vendas "
            "para responder, destacando insights acionáveis e sinalizando limitações quando faltar contexto. "
            "Nunca utilize conhecimento externo ou invente valores."
        )

        return create_agent(
            model=agent_model,
            tools=[rag_tool],
            system_prompt=system_prompt,
        )

    def _chat_provider_candidates(self, *, raw: bool = False) -> list[str]:
        provider = (self._model_config.provider or "auto").lower()
        if provider == "auto":
            candidates = ["cohere", "gemini", "openai", "azure_openai"]
        else:
            candidates = [provider]
        if raw:
            return candidates
        return [name for name in candidates if name not in self._blocked_chat_providers]

    def _embedding_provider_candidates(self, *, raw: bool = False) -> list[str]:
        provider = (self._embedding_config.provider or "auto").lower()
        if provider == "auto":
            candidates = ["cohere", "gemini", "openai", "azure_openai"]
        else:
            candidates = [provider]
        if raw:
            return candidates
        return [name for name in candidates if name not in self._blocked_embedding_providers]

    def _build_chat_model_by_provider(self, provider: str):
        normalized = provider.lower()
        if normalized in {"gemini", "google", "google_genai"}:
            return self._build_gemini_chat_model()
        if normalized == "cohere":
            return self._build_cohere_chat_model()
        if normalized == "openai":
            return self._build_openai_chat_model()
        if normalized == "azure_openai":
            return self._build_azure_openai_chat_model()
        raise RuntimeError(f"Provider de chat não suportado: {provider}")

    def _build_embeddings_by_provider(self, provider: str):
        normalized = provider.lower()
        if normalized in {"gemini", "google", "google_genai"}:
            embeddings = self._build_gemini_embeddings()
            return self._maybe_cache_embeddings(embeddings, normalized)
        if normalized == "cohere":
            embeddings = self._build_cohere_embeddings()
            return self._maybe_cache_embeddings(embeddings, normalized)
        if normalized == "openai":
            embeddings = self._build_openai_embeddings()
            return self._maybe_cache_embeddings(embeddings, normalized)
        if normalized == "azure_openai":
            embeddings = self._build_azure_openai_embeddings()
            return self._maybe_cache_embeddings(embeddings, normalized)
        raise RuntimeError(f"Provider de embeddings não suportado: {provider}")

    def _chat_credentials_available(self, provider: str) -> bool:
        normalized = provider.lower()
        if normalized == "cohere":
            return bool(self._get_env(self._model_config.cohere_api_key_env))
        if normalized in {"gemini", "google", "google_genai"}:
            return bool(self._get_env(self._model_config.api_key_env))
        if normalized == "openai":
            return bool(self._get_env(self._model_config.api_key_env))
        if normalized == "azure_openai":
            return bool(self._get_env(self._model_config.api_key_env) and self._get_env(self._model_config.api_base_env))
        return False

    def _embedding_credentials_available(self, provider: str) -> bool:
        normalized = provider.lower()
        if normalized == "cohere":
            return bool(self._get_env(self._embedding_config.cohere_api_key_env))
        if normalized in {"gemini", "google", "google_genai"}:
            return bool(self._get_env(self._embedding_config.api_key_env))
        if normalized == "openai":
            return bool(self._get_env(self._embedding_config.api_key_env))
        if normalized == "azure_openai":
            return bool(self._get_env(self._embedding_config.api_key_env) and self._get_env(self._embedding_config.api_base_env))
        return False

    def _build_rag_chain(self, retriever, rag_model):
        prompt = ChatPromptTemplate.from_messages(
            [
                (
                    "system",
                    "Você gera resumos objetivos usando somente o contexto fornecido. "
                    "Responda em português, cite números exatamente como aparecerem e informe explicitamente "
                    "quando nenhum registro relevante estiver disponível.",
                ),
                (
                    "human",
                    "Pergunta do usuário:\n{question}\n\nContexto estruturado:\n{context}\n\n"
                    "Produza uma resposta concisa destacando métricas relevantes e recomendações comerciais.",
                ),
            ]
        )

        return (
            RunnableParallel(
                context=retriever | RunnableLambda(self._format_documents),
                question=RunnablePassthrough(),
            )
            | prompt
            | rag_model
            | StrOutputParser()
        )

    def _create_rag_tool(self, rag_chain):
        @tool("consultar_base_vendas")
        def consultar_base_vendas(question: str) -> str:
            """Recupera e resume dados da base de vendas para a pergunta informada."""

            try:
                return rag_chain.invoke(question)
            except Exception as exc:  # noqa: BLE001
                raise RuntimeError(f"Falha ao consultar a base de vendas: {exc}") from exc

        return consultar_base_vendas

    def _load_vectorstore_for_provider(self, FAISS, vector_dir, embeddings, provider: str):
        provider_tag = provider.lower()
        index_path = vector_dir / f"faiss_index_{provider_tag}"
        index_file = index_path / "index.faiss"
        meta_file = index_path / "index.pkl"
        if index_file.exists() and meta_file.exists():
            return FAISS.load_local(
                str(index_path),
                embeddings=embeddings,
                allow_dangerous_deserialization=True,
            )

        docs = self._build_documents(limit_documents=5_000)
        store = self._build_vectorstore_with_batches(
            FAISS,
            docs,
            embeddings,
            provider_tag,
        )
        index_path.mkdir(parents=True, exist_ok=True)
        store.save_local(str(index_path))
        return store

    def _build_vectorstore_with_batches(self, FAISS, docs, embeddings, provider_tag: str):
        """Create the FAISS index embedding small batches to respect provider limits."""
        if not docs:
            return FAISS.from_texts([], embeddings)

        batch_size, delay_seconds = self._embedding_batch_params(provider_tag)
        batch_size = max(1, batch_size)

        first_batch = docs[:batch_size]
        texts = [doc.page_content for doc in first_batch]
        metadatas = [doc.metadata for doc in first_batch]
        store = FAISS.from_texts(texts, embeddings, metadatas=metadatas)

        if delay_seconds and len(docs) > batch_size:
            time.sleep(delay_seconds)

        for start in range(batch_size, len(docs), batch_size):
            batch = docs[start : start + batch_size]
            if not batch:
                continue
            store.add_documents(batch)
            if delay_seconds and start + batch_size < len(docs):
                time.sleep(delay_seconds)

        return store

    def _embedding_batch_params(self, provider_tag: str) -> tuple[int, float]:
        if provider_tag.lower() == "cohere":
            return (80, 1.5)
        return (400, 0.0)

    def _maybe_cache_embeddings(self, embeddings, provider: str):
        if CacheBackedEmbeddings is None or LocalFileStore is None:
            return embeddings
        if isinstance(embeddings, CacheBackedEmbeddings):
            return embeddings

        try:
            store = self._get_embedding_cache_store()
        except Exception:
            return embeddings

        namespace = self._build_embedding_namespace(provider, embeddings)
        return CacheBackedEmbeddings.from_bytes_store(
            embeddings,
            store,
            namespace=namespace,
            query_embedding_cache=True,
        )

    def _get_embedding_cache_store(self):
        if LocalFileStore is None:
            raise RuntimeError(
                "Instale langchain-classic para habilitar o cache de embeddings (pip install langchain-classic)."
            )
        if self._embedding_cache_store is None:
            cache_dir = self._config.cache_dir / "embedding_cache"
            cache_dir.mkdir(parents=True, exist_ok=True)
            self._embedding_cache_store = LocalFileStore(str(cache_dir))
        return self._embedding_cache_store

    def _build_embedding_namespace(self, provider: str, embeddings) -> str:
        normalized = provider.lower()
        model_name: Optional[str]
        if normalized == "cohere":
            model_name = self._embedding_config.cohere_model_name
        else:
            model_name = self._embedding_config.model_name

        embedder_model = getattr(embeddings, "model", None)
        if isinstance(embedder_model, str) and embedder_model:
            model_name = embedder_model

        if model_name:
            raw = f"{normalized}__{model_name}"
        else:
            raw = normalized
        return self._sanitize_namespace(raw)

    @staticmethod
    def _sanitize_namespace(raw: str) -> str:
        sanitized = re.sub(r"[^a-zA-Z0-9._-]+", "_", raw)
        sanitized = sanitized.strip("._- ")
        return sanitized or "default"

    @staticmethod
    def _format_documents(docs) -> str:
        if not docs:
            return "Nenhum registro relevante encontrado para a consulta."

        formatted_blocks: list[str] = []
        for idx, doc in enumerate(docs, start=1):
            metadata = doc.metadata or {}
            header_parts = []
            anuncio = metadata.get("cd_anuncio")
            categoria = metadata.get("categoria")
            if anuncio:
                header_parts.append(f"Anúncio {anuncio}")
            if categoria:
                header_parts.append(f"Categoria: {categoria}")
            header = " | ".join(header_parts) if header_parts else f"Documento {idx}"

            metrics = []
            receita = metadata.get("receita_total")
            quantidade = metadata.get("quantidade_total")
            pedidos = metadata.get("pedidos_total")
            margem = metadata.get("margem_media")
            if receita is not None:
                metrics.append(f"receita_total={float(receita):.2f}")
            if quantidade is not None:
                metrics.append(f"volume={float(quantidade):.0f}")
            if pedidos is not None:
                metrics.append(f"pedidos={int(pedidos)}")
            if margem is not None:
                metrics.append(f"margem_media={float(margem):.4f}")
            metrics_line = ", ".join(metrics)

            block_parts = [f"[{idx}] {header}"]
            if metrics_line:
                block_parts.append(metrics_line)
            block_parts.append(doc.page_content)
            formatted_blocks.append("\n".join(block_parts))

        return "\n\n".join(formatted_blocks)

    def _build_cohere_chat_model(self):
        try:
            from langchain_cohere import ChatCohere
        except ImportError as exc:  # noqa: F401
            raise RuntimeError(
                "Instale langchain-cohere para usar o provedor Cohere (pip install langchain-cohere)."
            ) from exc

        api_key = self._get_env(self._model_config.cohere_api_key_env)
        if not api_key:
            raise RuntimeError(
                f"Defina a variável de ambiente {self._model_config.cohere_api_key_env} para utilizar o provedor Cohere."
            )

        return ChatCohere(
            model=self._model_config.cohere_model_name,
            temperature=self._model_config.temperature,
            cohere_api_key=api_key,
        )

    def _build_gemini_chat_model(self):
        try:
            from langchain_google_genai import ChatGoogleGenerativeAI
        except ImportError as exc:  # noqa: F401
            raise RuntimeError(
                "Instale langchain-google-genai para usar o provedor Gemini (pip install langchain-google-genai)."
            ) from exc

        api_key = self._get_env(self._model_config.api_key_env)
        if not api_key:
            raise RuntimeError(
                "Defina a variável de ambiente GEMINI_API_KEY para utilizar o provedor Gemini."
            )

        return ChatGoogleGenerativeAI(
            model=self._model_config.model_name,
            temperature=self._model_config.temperature,
            google_api_key=api_key,
        )

    def _build_openai_chat_model(self):
        try:
            from langchain_openai import ChatOpenAI
        except ImportError as exc:  # noqa: F401
            raise RuntimeError(
                "Instale langchain-openai para usar ChatOpenAI (pip install langchain-openai)."
            ) from exc

        return ChatOpenAI(
            model=self._model_config.model_name,
            temperature=self._model_config.temperature,
            openai_api_key=self._get_env(self._model_config.api_key_env),
            openai_api_base=self._get_env(self._model_config.api_base_env),
        )

    def _build_azure_openai_chat_model(self):
        try:
            from langchain_openai import AzureChatOpenAI
        except ImportError as exc:  # noqa: F401
            raise RuntimeError(
                "Instale langchain-openai para usar AzureChatOpenAI (pip install langchain-openai)."
            ) from exc

        return AzureChatOpenAI(
            deployment_name=self._model_config.model_name,
            temperature=self._model_config.temperature,
            openai_api_key=self._get_env(self._model_config.api_key_env),
            azure_endpoint=self._get_env(self._model_config.api_base_env),
        )

    def _build_openai_embeddings(self):
        try:
            from langchain_openai import OpenAIEmbeddings
        except ImportError as exc:  # noqa: F401
            raise RuntimeError(
                "Instale langchain-openai para usar embeddings da OpenAI (pip install langchain-openai)."
            ) from exc

        return OpenAIEmbeddings(
            model=self._embedding_config.model_name,
            api_key=self._get_env(self._embedding_config.api_key_env),
            base_url=self._get_env(self._embedding_config.api_base_env),
        )

    def _build_cohere_embeddings(self):
        try:
            from langchain_cohere import CohereEmbeddings
        except ImportError as exc:  # noqa: F401
            raise RuntimeError(
                "Instale langchain-cohere para usar embeddings da Cohere (pip install langchain-cohere)."
            ) from exc

        api_key = self._get_env(self._embedding_config.cohere_api_key_env)
        if not api_key:
            raise RuntimeError(
                f"Defina a variável de ambiente {self._embedding_config.cohere_api_key_env} para utilizar embeddings da Cohere."
            )

        return CohereEmbeddings(
            model=self._embedding_config.cohere_model_name,
            cohere_api_key=api_key,
        )

    def _build_gemini_embeddings(self):
        try:
            from langchain_google_genai import GoogleGenerativeAIEmbeddings
        except ImportError as exc:  # noqa: F401
            raise RuntimeError(
                "Instale langchain-google-genai para usar embeddings do Gemini (pip install langchain-google-genai)."
            ) from exc

        api_key = self._get_env(self._embedding_config.api_key_env)
        if not api_key:
            raise RuntimeError(
                "Defina a variável de ambiente GEMINI_API_KEY para utilizar embeddings do Gemini."
            )

        return GoogleGenerativeAIEmbeddings(
            model=self._embedding_config.model_name,
            google_api_key=api_key,
        )

    def _build_azure_openai_embeddings(self):
        try:
            from langchain_openai import AzureOpenAIEmbeddings
        except ImportError as exc:  # noqa: F401
            raise RuntimeError(
                "Instale langchain-openai para usar embeddings do Azure OpenAI (pip install langchain-openai)."
            ) from exc

        return AzureOpenAIEmbeddings(
            deployment=self._embedding_config.model_name,
            api_key=self._get_env(self._embedding_config.api_key_env),
            azure_endpoint=self._get_env(self._embedding_config.api_base_env),
        )

    def _build_documents(self, *, limit_documents: int) -> list:
        try:
            from langchain_core.documents import Document
        except ImportError as exc:  # noqa: F401
            raise RuntimeError(
                "Instale langchain para gerar documentos de contexto (pip install langchain)."
            ) from exc

        df = self._dataset_manager.load()
        if df.empty:
            return []
        aggregations = {
            "ds_anuncio": ("ds_anuncio", "first"),
            "categoria": ("categoria", "first"),
            "qtd_total": ("qtd_sku", "sum"),
            "receita_total": ("rbld", "sum"),
            "pedidos": ("nr_nota_fiscal", "nunique"),
            "margem_media": ("perc_margem_bruta", "mean"),
        }
        grouped = df.groupby("cd_anuncio", as_index=False).agg(**aggregations)
        grouped = grouped.sort_values("receita_total", ascending=False).head(limit_documents)
        docs: list[Document] = []
        for _, row in grouped.iterrows():
            anuncio = str(row.get("cd_anuncio", ""))
            descricao = str(row.get("ds_anuncio", ""))
            categoria = str(row.get("categoria", ""))
            receita = float(row.get("receita_total", 0.0))
            quantidade = float(row.get("qtd_total", 0.0))
            pedidos = int(row.get("pedidos", 0))
            margem = float(row.get("margem_media", 0.0))
            content = (
                f"Anúncio {anuncio} ({descricao}) na categoria {categoria}. "
                f"Receita total {receita:.2f}, quantidade vendida {quantidade:.0f}, pedidos {pedidos}, "
                f"margem média {margem:.4f}."
            )
            metadata = {
                "cd_anuncio": anuncio,
                "ds_anuncio": descricao,
                "categoria": categoria,
                "receita_total": receita,
                "quantidade_total": quantidade,
                "pedidos_total": pedidos,
                "margem_media": margem,
            }
            docs.append(Document(page_content=content, metadata=metadata))
        return docs

    @staticmethod
    def _get_env(key: Optional[str]) -> Optional[str]:
        if not key:
            return None
        import os

        return os.getenv(key)
