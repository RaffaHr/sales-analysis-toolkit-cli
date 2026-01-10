from __future__ import annotations

from typing import Dict

import pandas as pd
import streamlit as st


def render_chat_history(messages: list[dict[str, str]]) -> None:
    for message in messages:
        role = message.get("role", "assistant")
        content = message.get("content", "")
        with st.chat_message(role):
            st.markdown(content)


def render_analysis_tables(tables: Dict[str, pd.DataFrame]) -> None:
    if not tables:
        return
    tabs = st.tabs(list(tables.keys()))
    for tab, (sheet_name, df) in zip(tabs, tables.items()):
        with tab:
            st.dataframe(df, use_container_width=True)


def render_command_help(specs) -> None:
    with st.expander("Comandos disponíveis"):
        for spec in specs:
            st.markdown(f"**/{spec.name}** — {spec.description}")
            st.code(spec.usage, language="text")
