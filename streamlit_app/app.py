from __future__ import annotations

from pathlib import Path
import os
import sys
from typing import Any, Dict

import pandas as pd
import streamlit as st
from dotenv import load_dotenv

# Garante que o pacote streamlit_app possa ser importado quando o script é executado diretamente.
_CURRENT_DIR = Path(__file__).resolve().parent
_PROJECT_ROOT = _CURRENT_DIR.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

load_dotenv(_PROJECT_ROOT / ".env", override=False)

from streamlit_app.auth.manager import AuthManager  # noqa: E402
from streamlit_app.chat.commands import (  # noqa: E402
    CommandContext,
    dispatch_command,
    list_available_commands,
)
from streamlit_app.config import AppConfig, load_config  # noqa: E402
from streamlit_app.data.loader import DatasetManager  # noqa: E402
from streamlit_app.llm.factory import (  # noqa: E402
    EmbeddingConfig,
    LangChainFactory,
    ModelConfig,
)
from streamlit_app.services.conversation_store import (  # noqa: E402
    ConversationStore,
)
from streamlit_app.ui.components import (  # noqa: E402
    render_analysis_tables,
    render_chat_history,
    render_command_help,
)


@st.cache_resource(show_spinner=False)
def _get_auth_manager(db_path: Path) -> AuthManager:
    return AuthManager(db_path)


@st.cache_resource(show_spinner=False)
def _get_conversation_store(db_path: Path) -> ConversationStore:
    return ConversationStore(db_path)


@st.cache_resource(show_spinner=False)
def _get_dataset_manager(source_path: Path, cache_dir: Path) -> DatasetManager:
    return DatasetManager(source_path, cache_dir)


def _create_langchain_factory(config: AppConfig, dataset_manager: DatasetManager) -> LangChainFactory:
    if st.session_state.get("llm_force_gemini"):
        return LangChainFactory(
            config,
            dataset_manager,
            model_config=ModelConfig(provider="gemini"),
            embedding_config=EmbeddingConfig(provider="gemini"),
        )
    return LangChainFactory(config, dataset_manager)


def main() -> None:
    st.set_page_config(page_title="Sales Toolkit Chatbot", layout="wide")
    config = load_config()
    auth_manager = _get_auth_manager(config.database_path)
    _ensure_default_admin(auth_manager)
    conversation_store = _get_conversation_store(config.database_path)
    dataset_manager = _get_dataset_manager(config.base_dataset_path, config.cache_dir)

    if "user" not in st.session_state:
        _render_login(auth_manager)
        return

    user_info = st.session_state["user"]
    conversation_id = _ensure_conversation(conversation_store, user_info["id"])
    st.session_state.setdefault("latest_tables", {})

    _render_sidebar(
        config,
        dataset_manager,
        conversation_store,
        user_info,
        conversation_id,
    )

    st.title("Assistente Comercial - Sales Toolkit")
    st.caption("Converse com a IA sobre o desempenho de vendas ou solicite análises com comandos.")

    messages_payload = _load_messages(conversation_store, conversation_id)
    render_chat_history(messages_payload)

    if prompt := st.chat_input("Pergunte sobre os dados ou use um comando (/...)" ):
        conversation_store.append_message(conversation_id, "user", prompt)
        if prompt.startswith("/"):
            context = CommandContext(config=config, dataset_manager=dataset_manager)
            result = dispatch_command(prompt, context)
            if result.error:
                reply = f":warning: {result.error}"
                tables = {}
            else:
                reply = result.reply or "Comando processado."
                tables = result.tables
            conversation_store.append_message(conversation_id, "assistant", reply)
            st.session_state["latest_tables"] = tables
        else:
            reply = _run_langchain_query(
                config,
                dataset_manager,
                conversation_store,
                conversation_id,
                prompt,
            )
            conversation_store.append_message(conversation_id, "assistant", reply)
            st.session_state["latest_tables"] = {}
        st.rerun()

    latest_tables: Dict[str, pd.DataFrame] = st.session_state.get("latest_tables", {})
    if latest_tables:
        st.subheader("Resultados da última análise")
        render_analysis_tables(latest_tables)
        _render_download_button(dataset_manager, latest_tables)


def _render_login(auth_manager: AuthManager) -> None:
    st.title("Sales Toolkit Chatbot")
    st.write("Informe suas credenciais para acessar o assistente e gerar análises.")
    with st.form("login_form"):
        username = st.text_input("Usuário")
        password = st.text_input("Senha", type="password")
        submitted = st.form_submit_button("Entrar")
    if submitted:
        user = auth_manager.authenticate(username, password)
        if user is None:
            st.error("Credenciais inválidas.")
            return
        st.session_state["user"] = {"id": user.id, "username": user.username, "is_admin": user.is_admin}
        st.rerun()


def _ensure_default_admin(auth_manager: AuthManager) -> None:
    username = os.getenv("SALES_TOOLKIT_ADMIN_USER")
    password = os.getenv("SALES_TOOLKIT_ADMIN_PASSWORD")
    if username and password:
        auth_manager.ensure_default_admin(username, password)


def _ensure_conversation(conversation_store: ConversationStore, user_id: int) -> int:
    if "conversation_id" in st.session_state:
        return st.session_state["conversation_id"]
    conversation = conversation_store.get_or_create_default(user_id)
    st.session_state["conversation_id"] = conversation.id
    return conversation.id


def _convert_message_payload(records) -> list[Dict[str, str]]:
    payload = []
    for item in records:
        payload.append({"role": item.role, "content": item.content})
    return payload


def _load_messages(conversation_store: ConversationStore, conversation_id: int) -> list[Dict[str, str]]:
    records = conversation_store.load_messages(conversation_id)
    return _convert_message_payload(records)


def _handle_new_conversation(*, conversation_store: ConversationStore, user_id: int) -> None:
    title = st.session_state.get("new_conversation_title", "Nova sessão").strip() or "Nova sessão"
    conversation = conversation_store.create_conversation(user_id, title)
    st.session_state["conversation_id"] = conversation.id
    st.session_state["new_conversation_title"] = "Nova sessão"
    st.rerun()


def _render_sidebar(
    config: AppConfig,
    dataset_manager: DatasetManager,
    conversation_store: ConversationStore,
    user_info: Dict[str, Any],
    conversation_id: int,
) -> None:
    with st.sidebar:
        st.header("Conta")
        st.markdown(f"**Usuário:** {user_info['username']}")
        if st.button("Sair"):
            st.session_state.clear()
            st.rerun()

        st.divider()
        conversations = conversation_store.list_conversations(user_info["id"])
        if conversations:
            labels = [f"{c.title} (#{c.id})" for c in conversations]
            ids = [c.id for c in conversations]
            try:
                default_index = ids.index(conversation_id)
            except ValueError:
                default_index = 0
            selection = st.selectbox("Conversas", labels, index=default_index)
            selected_id = ids[labels.index(selection)]
            if selected_id != conversation_id:
                st.session_state["conversation_id"] = selected_id
                st.rerun()
        st.session_state.setdefault("new_conversation_title", "Nova sessão")
        st.text_input(
            "Título da nova conversa",
            key="new_conversation_title",
        )
        st.button(
            "Nova conversa",
            on_click=_handle_new_conversation,
            kwargs={
                "conversation_store": conversation_store,
                "user_id": user_info["id"],
            },
        )

        st.divider()
        st.subheader("Dados carregados")
        summary = _dataset_summary(dataset_manager)
        for label, value in summary.items():
            st.metric(label, value)
        if st.button("Atualizar cache de parquet"):
            optimized_path = config.optimized_dataset_path
            dataset_manager.to_parquet(optimized_path, force=True)
            st.success(f"Dataset otimizado salvo em {optimized_path}")

        st.divider()
        st.subheader("IA de Dados")
        if st.session_state.get("llm_force_gemini"):
            st.info("Fallback para Gemini ativo após limite do provedor Cohere.")
        if st.button("Recriar vetor de contexto"):
            try:
                with st.spinner("Gerando documentos para a IA..."):
                    factory = _create_langchain_factory(config, dataset_manager)
                    factory.load_or_create_vectorstore()
                    st.session_state.pop("rag_agent", None)
                    st.session_state.pop("rag_agent_error", None)
                st.success("Vector store pronta.")
            except Exception as exc:  # noqa: BLE001
                st.error(f"Falha ao preparar vector store: {exc}")
        render_command_help(list_available_commands())


def _dataset_summary(dataset_manager: DatasetManager) -> Dict[str, Any]:
    df = dataset_manager.load()
    if df.empty:
        return {"Linhas": 0}
    min_date = pd.to_datetime(df.get("data"), dayfirst=True, errors="coerce").min()
    max_date = pd.to_datetime(df.get("data"), dayfirst=True, errors="coerce").max()
    categorias = df.get("categoria").nunique(dropna=True) if "categoria" in df.columns else 0
    return {
        "Linhas": f"{len(df):,}".replace(",", "."),
        "Períodos": df.get("periodo").nunique(dropna=True) if "periodo" in df.columns else 0,
        "Categorias": categorias,
        "Intervalo": f"{min_date:%d/%m/%Y} — {max_date:%d/%m/%Y}" if pd.notna(min_date) and pd.notna(max_date) else "N/D",
    }


def _run_langchain_query(
    config: AppConfig,
    dataset_manager: DatasetManager,
    conversation_store: ConversationStore,
    conversation_id: int,
    prompt: str,
) -> str:
    agent = st.session_state.get("rag_agent")
    if agent is None:
        try:
            with st.spinner("Preparando agente de vendas..."):
                factory = _create_langchain_factory(config, dataset_manager)
                agent = factory.build_sales_agent()
                st.session_state["rag_agent"] = agent
                if "cohere" in getattr(factory, "_blocked_embedding_providers", set()) or "cohere" in getattr(
                    factory, "_blocked_chat_providers", set()
                ):
                    st.session_state["llm_force_gemini"] = True
                st.session_state.pop("rag_agent_error", None)
        except Exception as exc:  # noqa: BLE001
            st.session_state["rag_agent_error"] = str(exc)
            return f"Não foi possível preparar o agente de perguntas no momento: {exc}"

    history_messages = _load_messages(conversation_store, conversation_id)
    try:
        result = agent.invoke({"messages": history_messages})
    except Exception as exc:  # noqa: BLE001
        if _should_switch_to_gemini(exc) and not st.session_state.get("llm_force_gemini", False):
            st.session_state.pop("rag_agent", None)
            st.session_state.pop("rag_agent_error", None)
            st.session_state["llm_force_gemini"] = True
            return _run_langchain_query(
                config,
                dataset_manager,
                conversation_store,
                conversation_id,
                prompt,
            )
        st.session_state["rag_agent_error"] = str(exc)
        return f"Ocorreu um erro ao consultar a IA: {exc}"

    reply = _extract_agent_reply(result)
    if reply:
        return reply
    return (
        "Não consegui encontrar informações suficientes na base para responder a esta pergunta. "
        "Tente refinar o pedido com mais detalhes."
    )


def _should_switch_to_gemini(exc: Exception) -> bool:
    message = str(exc).lower()
    keywords = {
        "too many requests",
        "rate limit",
        "429",
        "trial token rate limit",
    }
    return any(keyword in message for keyword in keywords)


def _extract_agent_reply(agent_output: Any) -> str:
    if isinstance(agent_output, dict):
        messages = agent_output.get("messages")
    elif isinstance(agent_output, list):
        messages = agent_output
    else:
        return _normalize_agent_content(agent_output)

    if not messages:
        return ""

    for message in reversed(messages):
        role = None
        content = None
        if isinstance(message, dict):
            role = message.get("role")
            content = message.get("content")
        else:
            role = getattr(message, "type", None) or getattr(message, "role", None)
            content = getattr(message, "content", None)
            if role == "ai":
                role = "assistant"

        if role == "assistant":
            normalized = _normalize_agent_content(content)
            if normalized:
                return normalized
    return ""


def _normalize_agent_content(content: Any) -> str:
    if content is None:
        return ""
    if isinstance(content, str):
        return content.strip()
    if isinstance(content, list):
        segments: list[str] = []
        for block in content:
            if isinstance(block, str):
                segments.append(block)
                continue
            if isinstance(block, dict):
                text = block.get("text") or block.get("content") or block.get("data")
                if isinstance(text, str):
                    segments.append(text)
                elif text is not None:
                    segments.append(str(text))
                continue
            possible_text = getattr(block, "text", None)
            if isinstance(possible_text, str):
                segments.append(possible_text)
            elif possible_text is not None:
                segments.append(str(possible_text))
        combined = "\n".join(segment.strip() for segment in segments if segment)
        return combined.strip()
    return str(content).strip()


def _render_download_button(dataset_manager: DatasetManager, tables: Dict[str, pd.DataFrame]) -> None:
    try:
        excel_bytes = dataset_manager.export_analysis_tables(tables)
    except Exception as exc:  # noqa: BLE001
        st.warning(f"Não foi possível preparar o download em Excel: {exc}")
        return
    st.download_button(
        "Baixar resultado em Excel",
        data=excel_bytes,
        file_name="analise_sales_toolkit.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


if __name__ == "__main__":
    main()
