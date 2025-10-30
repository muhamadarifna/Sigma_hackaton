# app.py
# ============================================
# RAG on Snowflake Cortex — Streamlit App
# Layout: kolom kiri (Q/A), kolom kanan (Riwayat & Context)
# ============================================

from typing import List
import streamlit as st

# Snowflake SDKs
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.session import Session
from snowflake.core import Root
from snowflake.cortex import complete

# TruLens (opsional, sesuai skrip kamu)
from trulens.core.otel.instrument import instrument
from trulens.otel.semconv.trace import SpanAttributes


# =========================
# Konfigurasi default (ubah bila perlu)
# =========================
DEFAULT_DB = "TELCO"
DEFAULT_SCHEMA = "DATAMART"
DEFAULT_SERVICE = "FOMC_SEARCH_SERVICE"
DEFAULT_COLUMNS = ["chunk"]     # contoh kolom hasil index (sesuaikan dengan punyamu)
DEFAULT_K = 4
DEFAULT_MODEL = "mistral-large2"

AVAILABLE_MODELS = [
    "mistral-large2"
]


# =========================
# Backend: Retriever & RAG
# =========================
class CortexSearchRetriever:
    def __init__(
        self,
        snowpark_session: Session,
        db: str,
        schema: str,
        service: str,
        columns: List[str],
        limit_to_retrieve: int = 4,
    ):
        self._session = snowpark_session
        self._db = db
        self._schema = schema
        self._service = service
        self._columns = columns
        self._limit = limit_to_retrieve

    def retrieve(self, query: str) -> List[str]:
        root = Root(self._session)
        search_service = (
            root
            .databases[self._db]
            .schemas[self._schema]
            .cortex_search_services[self._service]
        )
        resp = search_service.search(
            query=query,
            columns=self._columns,
            limit=self._limit
        )

        if not getattr(resp, "results", None):
            return []

        # Jika hanya satu kolom (mis. "chunk"), balikan list[str]
        if len(self._columns) == 1:
            col = self._columns[0]
            return [row.get(col, "") for row in resp.results]
        # Jika banyak kolom, gabungkan jadi satu string per hasil
        merged = []
        for row in resp.results:
            merged.append(" | ".join(str(row.get(c, "")) for c in self._columns))
        return merged


class RAG:
    def __init__(self, retriever: CortexSearchRetriever, model_name: str):
        self.retriever = retriever
        self.model_name = model_name

    @instrument(
        span_type=SpanAttributes.SpanType.RETRIEVAL,
        attributes={
            SpanAttributes.RETRIEVAL.QUERY_TEXT: "query",
            SpanAttributes.RETRIEVAL.RETRIEVED_CONTEXTS: "return",
        }
    )
    def retrieve_context(self, query: str) -> List[str]:
        return self.retriever.retrieve(query)

    def _build_prompt(self, query: str, context_chunks: List[str]) -> str:
        context_str = "\n\n".join(context_chunks) if context_chunks else ""
        prompt = f"""
You are an expert assistant extracting information strictly from the given context.

Instructions:
- Answer fully and precisely based on the context.
- Do NOT hallucinate.
- If the answer is not in the context, say you don't have enough information.

Context:
{context_str}

Question:
{query}

Answer:
""".strip()
        return prompt

    @instrument(span_type=SpanAttributes.SpanType.GENERATION)
    def generate_completion_stream(self, query: str, context_chunks: List[str]):
        prompt = self._build_prompt(query, context_chunks)
        # Stream token-by-token. Bisa string atau dict per token tergantung environment.
        for upd in complete(self.model_name, prompt, stream=True):
            if isinstance(upd, dict):
                token = upd.get("response", "")
            else:
                token = str(upd)
            if token:
                yield token

    @instrument(
        span_type=SpanAttributes.SpanType.RECORD_ROOT,
        attributes={
            SpanAttributes.RECORD_ROOT.INPUT: "query",
            SpanAttributes.RECORD_ROOT.OUTPUT: "return",
        }
    )
    def query_stream(self, query: str):
        ctx = self.retrieve_context(query)
        return ctx, self.generate_completion_stream(query, ctx)


def build_rag(
    db: str,
    schema: str,
    service: str,
    columns: List[str],
    k: int,
    model: str
) -> RAG:
    # get_active_session hanya tersedia di Streamlit in Snowflake
    session = get_active_session()
    retriever = CortexSearchRetriever(
        snowpark_session=session,
        db=db,
        schema=schema,
        service=service,
        columns=columns,
        limit_to_retrieve=k
    )
    return RAG(retriever=retriever, model_name=model)


# =========================
# Streamlit UI (dua kolom)
# =========================
st.set_page_config(page_title="RAG on Snowflake Cortex", page_icon="❄️", layout="wide")

st.title("❄️ SIGMA")
st.caption("Strategic Insights for Growth & Marketing Automation")
st.caption("Masukkan pertanyaanmu. Aplikasi ini mengambil konteks dari Cortex Search dan menjawab dengan model Cortex.")

# Sidebar: konfigurasi
with st.sidebar:
    st.header("⚙️ Pengaturan")
    db = st.text_input("Database", value=DEFAULT_DB)
    schema = st.text_input("Schema", value=DEFAULT_SCHEMA)
    service = st.text_input("Cortex Search Service", value=DEFAULT_SERVICE)
    model_name = st.selectbox("Model", options=AVAILABLE_MODELS, index=AVAILABLE_MODELS.index(DEFAULT_MODEL))
    top_k = st.slider("Jumlah konteks (Top-K)", min_value=1, max_value=10, value=DEFAULT_K, step=1)
    show_context = st.checkbox("Tampilkan konteks yang diambil", value=False)
    st.markdown("---")
    if "history" not in st.session_state:
        st.session_state["history"] = []
    if st.button("🧹 Clear history"):
        st.session_state["history"] = []

# Layout dua kolom
left_col, right_col = st.columns([2, 1], gap="large")

with left_col:
    query = st.text_input(
        "Tulis pertanyaanmu di sini:",
        value="",
        placeholder="Apa yang kamu mau tanyakan?"
    )
    ask = st.button("Tanya", type="primary", use_container_width=True)

    answer_container = st.empty()  # tempat stream jawaban

    if ask and query.strip():
        try:
            rag = build_rag(db, schema, service, DEFAULT_COLUMNS, top_k, model_name)
            with st.spinner("Mengambil konteks dan menyusun jawaban..."):
                contexts, stream = rag.query_stream(query.strip())

            acc = ""
            placeholder = answer_container.markdown("")
            for token in stream:
                acc += token
                placeholder.markdown(acc)

            # simpan ke history
            st.session_state["history"].append({"q": query.strip(), "a": acc, "ctx": contexts})

        except Exception as e:
            st.error(f"Terjadi error: {e}")

with right_col:
    st.subheader("🕘 Riwayat")
    hist = st.session_state.get("history", [])
    if not hist:
        st.info("Belum ada riwayat.")
    else:
        # tampilkan 5 terakhir (terbaru di atas)
        for item in reversed(hist[-5:]):
            with st.expander(f"Q: {item['q'][:80] + ('…' if len(item['q'])>80 else '')}", expanded=False):
                st.markdown(f"**Q:** {item['q']}")
                st.markdown(f"**A:** {item['a']}")
                if show_context and item.get("ctx"):
                    st.markdown("**Context (Top-K):**")
                    for i, c in enumerate(item["ctx"], start=1):
                        st.markdown(f"- {i}. {c}")

    # Panel konteks khusus pertanyaan terakhir
    if show_context and hist:
        st.markdown("---")
        st.subheader("📎 Konteks Terakhir")
        last = hist[-1]
        if last.get("ctx"):
            for i, c in enumerate(last["ctx"], start=1):
                st.markdown(f"**{i}.** {c}")
        else:
            st.caption("Tidak ada konteks yang ditemukan.")
