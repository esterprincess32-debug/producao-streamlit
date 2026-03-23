import os
import json
import hashlib
from datetime import datetime, timedelta

import pandas as pd
import streamlit as st
import firebase_store

st.set_page_config(layout="wide", page_title="Controle de Producao")

ARQUIVO_DADOS = "producao.csv"
ARQUIVO_EVENTOS = "producao_eventos.csv"
ARQUIVO_USUARIOS = "usuarios.csv"
ARQUIVO_CONFIG = "config_sistema.json"
COLECAO_DADOS = "producao_dados"
COLECAO_EVENTOS = "producao_eventos"
COLECAO_USUARIOS = "producao_usuarios"
COLECAO_CONFIG = "producao_config"
DEFAULT_PECAS_POR_GRADE = 6
DEFAULT_CORES = ["Preto", "Cinza", "Azul", "Azul Marinho", "Azul BB", "Azul Royal", "Vermelho", "Branco"]
DEFAULT_MODELOS = ["NIKE CLUB", "LACOSTE", "BROOKS", "MERCEDES", "BMW"]
STATUS_FLUXO = ["1. Corte", "2. Costura", "3. Acabamento", "4. Finalizado"]
PECAS_POR_GRADE = DEFAULT_PECAS_POR_GRADE
CORES = list(DEFAULT_CORES)
MODELOS_DISPONIVEIS = list(DEFAULT_MODELOS)
COLUNAS_CORES = [f"Cor_{c.replace(' ', '_')}" for c in CORES]
COLUNAS_BASE = [
    "ID",
    "Pedido",
    "Cliente",
    "NumeroCliente",
    "Observacao",
    "Modelo",
    "Qtd",
    "Status",
    "Entrada",
    "PrazoFinalizacao",
    "ResponsavelLancamento",
]
COLUNAS_EVENTOS = [
    "DataHora",
    "Data",
    "Hora",
    "Pedido",
    "ModeloID",
    "Cliente",
    "Modelo",
    "Acao",
    "StatusAntes",
    "StatusDepois",
    "Grades",
    "Qtd",
    "Detalhes",
]
COLUNAS_USUARIOS = ["Usuario", "SenhaHash", "Perfil", "Ativo"]
LIMITE_EVENTOS_FIREBASE = 3000

def normalizar_nome_cor(nome):
    txt = str(nome).strip()
    if not txt:
        return ""
    return " ".join(txt.split()).title()


def normalizar_numero_cliente(numero):
    txt = str(numero).strip()
    if not txt:
        return "-"
    return " ".join(txt.split())


def nome_coluna_cor(nome_cor):
    return f"Cor_{normalizar_nome_cor(nome_cor).replace(' ', '_')}"


def aplicar_config_em_memoria(cfg):
    global PECAS_POR_GRADE, CORES, COLUNAS_CORES, MODELOS_DISPONIVEIS
    pecas = int(cfg.get("pecas_por_grade", DEFAULT_PECAS_POR_GRADE))
    pecas = max(1, pecas)

    cores_cfg = cfg.get("cores", DEFAULT_CORES)
    if not isinstance(cores_cfg, list):
        cores_cfg = DEFAULT_CORES

    cores_norm = []
    seen = set()
    for c in cores_cfg:
        cn = normalizar_nome_cor(c)
        if not cn:
            continue
        key = cn.lower()
        if key in seen:
            continue
        seen.add(key)
        cores_norm.append(cn)
    if not cores_norm:
        cores_norm = list(DEFAULT_CORES)

    modelos_cfg = cfg.get("modelos", DEFAULT_MODELOS)
    if not isinstance(modelos_cfg, list):
        modelos_cfg = DEFAULT_MODELOS
    modelos_norm = []
    seen_modelos = set()
    for m in modelos_cfg:
        mn = texto_maiusculo(m)
        if not mn:
            continue
        if mn in seen_modelos:
            continue
        seen_modelos.add(mn)
        modelos_norm.append(mn)
    if not modelos_norm:
        modelos_norm = list(DEFAULT_MODELOS)

    PECAS_POR_GRADE = pecas
    CORES = cores_norm
    MODELOS_DISPONIVEIS = modelos_norm
    COLUNAS_CORES = [nome_coluna_cor(c) for c in CORES]


def carregar_config_sistema():
    cfg = {
        "pecas_por_grade": DEFAULT_PECAS_POR_GRADE,
        "cores": list(DEFAULT_CORES),
        "modelos": list(DEFAULT_MODELOS),
        "usuarios_somente_visualizacao": [],
    }
    if firebase_store.is_enabled():
        df = firebase_store.load_collection_df(COLECAO_CONFIG, ["Chave", "Valor"])
        if not df.empty:
            mapa = {str(r["Chave"]): r["Valor"] for _, r in df.iterrows()}
            try:
                cfg["pecas_por_grade"] = int(mapa.get("pecas_por_grade", DEFAULT_PECAS_POR_GRADE))
            except Exception:
                cfg["pecas_por_grade"] = DEFAULT_PECAS_POR_GRADE
            try:
                cores_val = mapa.get("cores_json", json.dumps(DEFAULT_CORES, ensure_ascii=False))
                cfg["cores"] = json.loads(cores_val) if isinstance(cores_val, str) else list(DEFAULT_CORES)
            except Exception:
                cfg["cores"] = list(DEFAULT_CORES)
            try:
                modelos_val = mapa.get("modelos_json", json.dumps(DEFAULT_MODELOS, ensure_ascii=False))
                cfg["modelos"] = json.loads(modelos_val) if isinstance(modelos_val, str) else list(DEFAULT_MODELOS)
            except Exception:
                cfg["modelos"] = list(DEFAULT_MODELOS)
            try:
                vis_val = mapa.get("usuarios_somente_visualizacao_json", "[]")
                vis = json.loads(vis_val) if isinstance(vis_val, str) else []
                cfg["usuarios_somente_visualizacao"] = [str(x).strip().lower() for x in vis if str(x).strip()]
            except Exception:
                cfg["usuarios_somente_visualizacao"] = []
    else:
        if os.path.exists(ARQUIVO_CONFIG):
            try:
                with open(ARQUIVO_CONFIG, "r", encoding="utf-8") as f:
                    raw = json.load(f)
                cfg["pecas_por_grade"] = int(raw.get("pecas_por_grade", DEFAULT_PECAS_POR_GRADE))
                cfg["cores"] = raw.get("cores", list(DEFAULT_CORES))
                cfg["modelos"] = raw.get("modelos", list(DEFAULT_MODELOS))
                vis = raw.get("usuarios_somente_visualizacao", [])
                cfg["usuarios_somente_visualizacao"] = [str(x).strip().lower() for x in vis if str(x).strip()]
            except Exception:
                pass
    aplicar_config_em_memoria(cfg)
    return cfg


def salvar_config_sistema(pecas_por_grade, cores_lista, modelos_lista=None, usuarios_somente_visualizacao=None):
    if modelos_lista is None:
        modelos_lista = CONFIG_SISTEMA.get("modelos", DEFAULT_MODELOS)
    if usuarios_somente_visualizacao is None:
        usuarios_somente_visualizacao = CONFIG_SISTEMA.get("usuarios_somente_visualizacao", [])
    usuarios_norm = [str(x).strip().lower() for x in usuarios_somente_visualizacao if str(x).strip()]
    modelos_norm = [texto_maiusculo(x) for x in modelos_lista if str(x).strip()]
    cfg = {
        "pecas_por_grade": int(pecas_por_grade),
        "cores": list(cores_lista),
        "modelos": list(modelos_norm),
        "usuarios_somente_visualizacao": usuarios_norm,
    }
    aplicar_config_em_memoria(cfg)

    if firebase_store.is_enabled():
        df_cfg = pd.DataFrame(
            [
                {"Chave": "pecas_por_grade", "Valor": str(int(pecas_por_grade))},
                {"Chave": "cores_json", "Valor": json.dumps(list(cores_lista), ensure_ascii=False)},
                {"Chave": "modelos_json", "Valor": json.dumps(list(modelos_norm), ensure_ascii=False)},
                {
                    "Chave": "usuarios_somente_visualizacao_json",
                    "Valor": json.dumps(usuarios_norm, ensure_ascii=False),
                },
            ]
        )
        firebase_store.save_collection_df(COLECAO_CONFIG, df_cfg, key_field="Chave")
    else:
        with open(ARQUIVO_CONFIG, "w", encoding="utf-8") as f:
            json.dump(cfg, f, ensure_ascii=False, indent=2)


def aplicar_estilo():
    st.markdown(
        """
        <style>
          @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@500;700;800&display=swap');

          :root {
            --bg-main: #f3f6fb;
            --bg-soft: #ffffff;
            --bg-hero: linear-gradient(120deg, #071a33 0%, #0f2e57 52%, #1d4f88 100%);
            --line: #d8e3f2;
            --txt-main: #0d2340;
            --txt-soft: #5d6f89;
            --primary: #0f5dcf;
            --primary-hover: #0a4eb0;
            --success: #0d9b63;
          }

          .stApp {
            font-family: "Plus Jakarta Sans", sans-serif;
            background:
              radial-gradient(1200px 420px at 100% -10%, #dce8fb 0%, transparent 56%),
              radial-gradient(900px 360px at -10% 20%, #edf3ff 0%, transparent 50%),
              var(--bg-main);
            color: var(--txt-main);
          }

          [data-testid="stAppViewContainer"] { height: 100vh; }
          [data-testid="stSidebarContent"] { height: 100vh; overflow-y: auto; }
          .block-container {
            max-width: 100%;
            padding: 0.45rem 0.8rem 0.35rem 0.8rem;
          }

          [data-testid="stSidebar"] {
            background: linear-gradient(180deg, #08172d 0%, #102746 62%, #15345f 100%);
            border-right: 1px solid rgba(255,255,255,0.12);
            min-width: clamp(220px, 18vw, 280px) !important;
            max-width: clamp(220px, 18vw, 280px) !important;
          }
          [data-testid="stSidebar"] * { color: #eff4ff !important; }
          [data-testid="stSidebar"] label,
          [data-testid="stSidebar"] p,
          [data-testid="stSidebar"] span,
          [data-testid="stSidebar"] div {
            color: #eff4ff !important;
          }
          [data-testid="stSidebar"] [data-baseweb="input"] {
            background: #ecf2fc !important;
            border: 1px solid #bdd0ea !important;
            border-radius: 10px !important;
          }
          [data-testid="stSidebar"] [data-baseweb="input"] input {
            color: #10233f !important;
            -webkit-text-fill-color: #10233f !important;
            font-weight: 600 !important;
          }
          [data-testid="stSidebar"] [data-baseweb="input"] input::placeholder {
            color: #5f7396 !important;
            opacity: 1 !important;
          }
          [data-testid="stSidebar"] [data-testid="stForm"] label,
          [data-testid="stSidebar"] [data-testid="stForm"] p,
          [data-testid="stSidebar"] [data-testid="stForm"] span {
            color: #10233f !important;
            font-weight: 700 !important;
          }
          [data-testid="stSidebar"] [data-testid="stForm"] [data-baseweb="input"] {
            background: #f5f8fe !important;
            border: 1px solid #b7cceb !important;
          }
          [data-testid="stSidebar"] [data-baseweb="textarea"] {
            background: #f5f8fe !important;
            border: 1px solid #b7cceb !important;
            border-radius: 10px !important;
          }
          [data-testid="stSidebar"] [data-baseweb="textarea"] textarea {
            color: #10233f !important;
            -webkit-text-fill-color: #10233f !important;
            font-weight: 600 !important;
          }
          [data-testid="stSidebar"] [data-testid="stForm"] [data-baseweb="button"] {
            background: #dbe6f8 !important;
            border-left: 1px solid #bfd0ea !important;
            color: #17488d !important;
          }
          [data-testid="stSidebar"] [data-testid="stForm"] [data-baseweb="button"] svg {
            fill: #17488d !important;
          }
          [data-testid="stSidebar"] .stExpander {
            border: 1px solid rgba(255,255,255,0.18) !important;
            background: rgba(255,255,255,0.07) !important;
          }
          [data-testid="stSidebar"] .stExpander details summary {
            background: rgba(140, 173, 220, 0.24) !important;
          }
          [data-testid="stSidebar"] .stExpander label,
          [data-testid="stSidebar"] .stExpander p,
          [data-testid="stSidebar"] .stExpander span {
            color: #eef4ff !important;
          }
          [data-testid="stSidebar"] [data-testid="stNumberInput"] input {
            color: #10233f !important;
            -webkit-text-fill-color: #10233f !important;
            font-weight: 700 !important;
            background: #edf3fd !important;
          }
          [data-testid="stSidebar"] [data-testid="stNumberInput"] button,
          [data-testid="stSidebar"] [data-testid="stFormSubmitButton"] button {
            background: var(--primary) !important;
            border: 1px solid #0a4ca7 !important;
            color: #fff !important;
            font-weight: 700 !important;
          }
          [data-testid="stSidebar"] [data-testid="stNumberInput"] button:hover,
          [data-testid="stSidebar"] [data-testid="stFormSubmitButton"] button:hover {
            background: var(--primary-hover) !important;
          }

          .hero-box {
            border-radius: 16px;
            background: var(--bg-hero);
            border: 1px solid rgba(255,255,255,0.12);
            padding: 12px 14px;
            box-shadow: 0 12px 28px rgba(6, 24, 54, 0.22);
            margin-bottom: 8px;
          }
          .hero-title {
            margin: 0;
            color: #f4f8ff;
            font-weight: 800;
            letter-spacing: 0.2px;
            font-size: 1.08rem;
          }
          .hero-sub {
            margin: 3px 0 0 0;
            color: #d7e6ff;
            font-size: 0.82rem;
            font-weight: 600;
          }

          .access-pill {
            border: 1px solid #cfe0f6;
            background: #f6faff;
            border-radius: 12px;
            padding: 8px 10px;
            margin: 6px 0 10px 0;
            color: #0f396a;
            font-size: 0.82rem;
            font-weight: 700;
          }

          .kpi-card {
            border: 1px solid var(--line);
            border-radius: 14px;
            padding: 10px 12px;
            background: var(--bg-soft);
            box-shadow: 0 6px 16px rgba(15, 39, 74, 0.06);
          }

          .stage-shell {
            border: 1px solid var(--line);
            background: #ffffff;
            border-radius: 14px;
            padding: 8px;
            box-shadow: 0 8px 20px rgba(16, 42, 78, 0.07);
          }
          .stage-head {
            display: flex;
            align-items: center;
            justify-content: space-between;
            border-radius: 10px;
            background: linear-gradient(180deg, #f6f9ff 0%, #edf4ff 100%);
            border: 1px solid #d7e4f7;
            padding: 7px 9px;
            margin-bottom: 8px;
          }
          .stage-title {
            font-size: 0.88rem;
            font-weight: 800;
            color: #113157;
          }
          .stage-count {
            min-width: 24px;
            text-align: center;
            font-size: 0.74rem;
            font-weight: 800;
            color: #ffffff;
            background: #0f5dcf;
            border-radius: 999px;
            padding: 2px 7px;
          }
          .kpi-label {
            font-size: 0.75rem;
            color: var(--txt-soft);
            font-weight: 700;
            margin: 0;
          }
          .kpi-value {
            font-size: 1.22rem;
            color: var(--txt-main);
            font-weight: 800;
            margin: 1px 0 0 0;
          }

          [data-testid="stVerticalBlock"] [data-testid="stMarkdownContainer"] h3 {
            color: #0e2b52;
            font-weight: 800;
            font-size: 1.05rem;
            margin: 0.2rem 0;
          }

          [data-testid="stExpander"] {
            border: 1px solid #d7e3f3 !important;
            border-radius: 11px !important;
            background: #ffffff !important;
            margin-bottom: 6px;
          }
          [data-testid="stExpander"] details summary {
            background: #f8fbff;
            border-radius: 10px;
          }

          [data-testid="stButton"] button {
            border-radius: 10px;
            border: 1px solid #0a4ca7;
            background: var(--primary);
            color: #fff;
            font-weight: 700;
          }
          [data-testid="stButton"] button:hover {
            background: var(--primary-hover);
            border-color: #083f8d;
          }
          [data-testid="stButton"] button:disabled {
            background: #a3b4cd;
            border-color: #94a7c1;
            color: #eef3fb;
          }

          [data-testid="stForm"] {
            border: 1px solid var(--line);
            border-radius: 12px;
            padding: 8px;
            background: #fbfdff;
          }

          [data-testid="stCaptionContainer"] {
            color: #61748f;
            font-size: 0.78rem;
          }
          [data-testid="stMarkdownContainer"] p { margin-bottom: 0.18rem; }

          section.main > div { overflow: hidden; }
          section.main div[data-testid="column"] > div[data-testid="stVerticalBlock"] {
            max-height: 78vh;
            overflow-y: auto;
            padding-right: 2px;
          }

          /* Ultra-wide */
          @media (min-width: 1700px) {
            .block-container { padding-left: 1rem; padding-right: 1rem; }
            section.main div[data-testid="column"] > div[data-testid="stVerticalBlock"] {
              max-height: 82vh;
            }
          }

          /* Notebook / desktop medio: 2 colunas por linha no painel */
          @media (max-width: 1400px) {
            section.main div[data-testid="column"] {
              min-width: 49% !important;
              flex: 1 1 49% !important;
            }
          }

          /* Tablet e mobile: 1 coluna por linha */
          @media (max-width: 900px) {
            .block-container { padding: 0.35rem 0.45rem; }
            [data-testid="stSidebar"] {
              min-width: 100% !important;
              max-width: 100% !important;
            }
            section.main > div { overflow: auto; }
            section.main div[data-testid="column"] { min-width: 100% !important; flex: 1 1 100% !important; }
            section.main div[data-testid="column"] > div[data-testid="stVerticalBlock"] {
              max-height: none;
              overflow-y: visible;
              padding-right: 0;
            }
          }
        </style>
        """,
        unsafe_allow_html=True,
    )


def aplicar_estilo_dashboard():
    st.markdown(
        """
        <style>
          .stApp {
            background:
              radial-gradient(900px 380px at 110% -10%, rgba(72, 142, 255, 0.20) 0%, transparent 58%),
              radial-gradient(900px 360px at -10% 20%, rgba(23, 93, 201, 0.12) 0%, transparent 52%),
              linear-gradient(180deg, #eef3fb 0%, #e8eef8 100%);
          }
          .dash2-hero {
            border: 1px solid rgba(187, 210, 243, 0.95);
            border-radius: 16px;
            padding: 14px 16px;
            background:
              radial-gradient(120% 120% at 100% 0%, rgba(74, 140, 240, 0.30) 0%, rgba(74,140,240,0) 55%),
              linear-gradient(120deg, rgba(255,255,255,0.94) 0%, rgba(246,250,255,0.92) 100%);
            box-shadow: 0 18px 34px rgba(13, 40, 79, 0.14);
            backdrop-filter: blur(6px);
            margin-bottom: 10px;
            display: flex;
            justify-content: space-between;
            align-items: center;
            gap: 8px;
          }
          .dash2-title {
            margin: 0;
            color: #0b2a4d;
            font-size: 1.1rem;
            font-weight: 800;
            letter-spacing: 0.2px;
          }
          .dash2-sub {
            margin: 2px 0 0 0;
            color: #506a8f;
            font-size: 0.82rem;
            font-weight: 600;
          }
          .dash2-chip {
            border: 1px solid #bdd6fb;
            background: linear-gradient(180deg, #f7faff 0%, #e8f2ff 100%);
            color: #0d3f84;
            border-radius: 999px;
            padding: 5px 10px;
            font-size: 0.74rem;
            font-weight: 800;
            white-space: nowrap;
          }

          .dash2-kpi {
            border: 1px solid rgba(193, 213, 241, 0.96);
            border-radius: 12px;
            padding: 10px 12px;
            background: linear-gradient(180deg, rgba(255,255,255,0.92) 0%, rgba(248,251,255,0.90) 100%);
            box-shadow: 0 12px 22px rgba(14, 43, 82, 0.12);
            backdrop-filter: blur(5px);
            position: relative;
            overflow: hidden;
          }
          .dash2-kpi::before {
            content: "";
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            height: 4px;
            background: linear-gradient(90deg, #2f7de3 0%, #0f5dcf 100%);
          }
          .dash2-kpi-label {
            margin: 2px 0 0 0;
            color: #5a7193;
            font-size: 0.75rem;
            font-weight: 700;
          }
          .dash2-kpi-value {
            margin: 2px 0 0 0;
            color: #0a2a4d;
            font-size: 1.6rem;
            font-weight: 800;
          }

          .dash2-stage {
            border: 1px solid rgba(194, 214, 242, 0.96);
            border-radius: 12px;
            background: linear-gradient(180deg, rgba(255,255,255,0.92) 0%, rgba(248,251,255,0.90) 100%);
            box-shadow: 0 14px 24px rgba(14, 43, 82, 0.10);
            backdrop-filter: blur(5px);
            overflow: hidden;
          }
          .dash2-stage-head {
            display: flex;
            align-items: center;
            justify-content: space-between;
            padding: 8px 10px;
            border-bottom: 1px solid #e5eefc;
            background: #f7faff;
          }
          .dash2-stage-title { color: #12345c; font-size: 0.86rem; font-weight: 800; }
          .dash2-stage-count {
            min-width: 22px;
            text-align: center;
            color: #fff;
            background: #1c67d1;
            border-radius: 999px;
            font-size: 0.72rem;
            font-weight: 800;
            padding: 1px 6px;
          }
          .dash2-stage-body { padding: 8px; }

          .dash2-item {
            border: 1px solid #dee9fb;
            border-radius: 10px;
            background: linear-gradient(180deg, #ffffff 0%, #f8fbff 100%);
            padding: 7px 8px;
            margin-bottom: 6px;
            box-shadow: 0 6px 12px rgba(14, 43, 82, 0.06);
          }
          .dash2-item-top {
            display: flex;
            justify-content: space-between;
            align-items: center;
            gap: 8px;
          }
          .dash2-item-name {
            color: #0d2e55;
            font-size: 0.82rem;
            font-weight: 800;
            margin: 0;
          }
          .dash2-item-meta {
            color: #5d7497;
            font-size: 0.72rem;
            font-weight: 600;
            margin: 2px 0 0 0;
          }
          .dash2-item-line {
            color: #385a86;
            font-size: 0.74rem;
            font-weight: 600;
            margin: 2px 0 0 0;
          }
          .dash2-item-line b {
            color: #143963;
            font-weight: 800;
          }
          .dash2-badge {
            font-size: 0.68rem;
            font-weight: 800;
            border-radius: 999px;
            padding: 2px 7px;
            border: 1px solid;
          }
          .dash2-badge.ok {
            color: #0c7d4f;
            background: #eaf8f2;
            border-color: #b9ebd4;
          }
          .dash2-badge.late {
            color: #a43b2b;
            background: #fff0ed;
            border-color: #f4c3bb;
          }
        </style>
        """,
        unsafe_allow_html=True,
    )


def hash_senha(senha):
    return hashlib.sha256(str(senha).encode("utf-8")).hexdigest()


def texto_maiusculo(valor):
    return str(valor).strip().upper()


def carregar_usuarios():
    usuarios_padrao = [
        {"Usuario": "estoque", "SenhaHash": hash_senha("123"), "Perfil": "visualizador", "Ativo": 1},
        {"Usuario": "vitor", "SenhaHash": hash_senha("1408"), "Perfil": "editor", "Ativo": 1},
        {"Usuario": "lucas_ti", "SenhaHash": hash_senha("Luzineide12."), "Perfil": "editor", "Ativo": 1},
        {"Usuario": "dashboard", "SenhaHash": hash_senha("123"), "Perfil": "dashboard", "Ativo": 1},
    ]
    force_sync = os.getenv("FORCE_SYNC_USERS", "").strip().lower() in ("1", "true", "yes", "on")
    if firebase_store.is_enabled():
        df = firebase_store.load_collection_df(COLECAO_USUARIOS, COLUNAS_USUARIOS)
    else:
        if not os.path.exists(ARQUIVO_USUARIOS):
            base = pd.DataFrame(
                usuarios_padrao,
                columns=COLUNAS_USUARIOS,
            )
            base.to_csv(ARQUIVO_USUARIOS, index=False)
            return base
        df = pd.read_csv(ARQUIVO_USUARIOS)

    if df.empty or force_sync:
        df = pd.DataFrame(usuarios_padrao, columns=COLUNAS_USUARIOS)
        if firebase_store.is_enabled():
            firebase_store.save_collection_df(COLECAO_USUARIOS, df, key_field="Usuario")
        else:
            df.to_csv(ARQUIVO_USUARIOS, index=False)

    for c in COLUNAS_USUARIOS:
        if c not in df.columns:
            df[c] = "" if c != "Ativo" else 1
    df["Usuario"] = df["Usuario"].astype(str).str.strip().str.lower()
    df["Perfil"] = df["Perfil"].astype(str).str.strip().str.lower()
    df["Ativo"] = pd.to_numeric(df["Ativo"], errors="coerce").fillna(1).astype(int)
    return df


def salvar_usuarios(df):
    df = df.copy()
    for c in COLUNAS_USUARIOS:
        if c not in df.columns:
            df[c] = "" if c != "Ativo" else 1
    df["Usuario"] = df["Usuario"].astype(str).str.strip().str.lower()
    df["Perfil"] = df["Perfil"].astype(str).str.strip().str.lower()
    df["Ativo"] = pd.to_numeric(df["Ativo"], errors="coerce").fillna(1).astype(int)
    df = df[COLUNAS_USUARIOS]

    if firebase_store.is_enabled():
        firebase_store.save_collection_df(COLECAO_USUARIOS, df, key_field="Usuario")
    else:
        df.to_csv(ARQUIVO_USUARIOS, index=False)


def usuario_master():
    return str(st.session_state.get("usuario", "")).strip().lower() == "lucas_ti"


def usuario_somente_visualizacao():
    if perfil_atual() == "dashboard":
        return True
    u = str(st.session_state.get("usuario", "")).strip().lower()
    lista = CONFIG_SISTEMA.get("usuarios_somente_visualizacao", [])
    return u in set([str(x).strip().lower() for x in lista])


def autenticar(usuario, senha):
    df = carregar_usuarios()
    u = str(usuario).strip().lower()
    s_hash = hash_senha(senha)
    hit = df[(df["Usuario"] == u) & (df["SenhaHash"] == s_hash) & (df["Ativo"] == 1)]
    if hit.empty:
        return None
    perfil = str(hit.iloc[0]["Perfil"]).lower()
    if perfil not in ["editor", "visualizador", "dashboard"]:
        perfil = "visualizador"
    return {"usuario": u, "perfil": perfil}


def inicializar_sessao_acesso():
    if "logado" not in st.session_state:
        st.session_state["logado"] = False
    if "usuario" not in st.session_state:
        st.session_state["usuario"] = ""
    if "perfil" not in st.session_state:
        st.session_state["perfil"] = "visualizador"


def render_login_sidebar():
    with st.sidebar:
        st.markdown("---")
        st.subheader("Acesso")
        if not st.session_state["logado"]:
            with st.form("form_login"):
                usuario = st.text_input("Usuario")
                senha = st.text_input("Senha", type="password")
                entrar = st.form_submit_button("Entrar")
            st.caption("Usuarios ativos: estoque, vitor, lucas_ti, dashboard")
            if entrar:
                auth = autenticar(usuario, senha)
                if auth is None:
                    st.error("Usuario ou senha invalidos.")
                else:
                    st.session_state["logado"] = True
                    st.session_state["usuario"] = auth["usuario"]
                    st.session_state["perfil"] = auth["perfil"]
                    st.rerun()
            st.stop()

        perfil = str(st.session_state["perfil"]).lower()
        if perfil == "dashboard":
            resumo = "Acesso dashboard: somente painel de indicadores."
        elif usuario_somente_visualizacao():
            resumo = "Acesso somente visualizacao: dashboard e relatorios."
        elif perfil == "editor":
            resumo = "Acesso total: lancar, separar, editar, mover, arquivar."
        else:
            resumo = "Acesso consulta: lancar pedido e separar (Pronto em Separar pedido)."
        if perfil == "dashboard":
            st.markdown(
                f"""
                <div style="border:1px solid rgba(184,214,255,0.45); border-radius:12px; padding:10px; background:linear-gradient(180deg, rgba(255,255,255,0.12), rgba(255,255,255,0.05));">
                  <div style="font-weight:800; font-size:15px;">Painel Dashboard</div>
                  <div style="font-size:12px; margin-top:6px;"><b>Usuario:</b> {st.session_state['usuario']}</div>
                  <div style="font-size:12px;"><b>Perfil:</b> {perfil}</div>
                  <div style="font-size:12px; margin-top:6px;">{resumo}</div>
                  <div style="font-size:11px; margin-top:6px; opacity:0.95;">Somente visualizacao: sem edicao e sem movimentacao de pedidos.</div>
                </div>
                """,
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                f"""
                <div style="border:1px solid rgba(255,255,255,0.25); border-radius:10px; padding:10px; background:rgba(255,255,255,0.06);">
                  <div style="font-weight:800; font-size:15px;">Usuario: {st.session_state['usuario']}</div>
                  <div style="font-weight:700; font-size:13px;">Perfil: {perfil}</div>
                  <div style="font-size:12px; margin-top:6px;">{resumo}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )
        if st.button("Sair", use_container_width=True):
            st.session_state["logado"] = False
            st.session_state["usuario"] = ""
            st.session_state["perfil"] = "visualizador"
            st.rerun()


def perfil_atual():
    return str(st.session_state.get("perfil", "visualizador")).lower()


def pode_lancar_pedido():
    if usuario_somente_visualizacao():
        return False
    return perfil_atual() in ["editor", "visualizador"]


def pode_editar_completo():
    if usuario_somente_visualizacao():
        return False
    return perfil_atual() == "editor"


def pode_mover_pronto(status_atual):
    if usuario_somente_visualizacao():
        return False
    if perfil_atual() == "editor":
        return True
    return perfil_atual() == "visualizador" and str(status_atual) == STATUS_FLUXO[0]


def assinatura_arquivos():
    itens = []
    for caminho in [ARQUIVO_DADOS, ARQUIVO_EVENTOS]:
        if os.path.exists(caminho):
            stat = os.stat(caminho)
            itens.append((caminho, int(stat.st_mtime_ns), int(stat.st_size)))
        else:
            itens.append((caminho, 0, 0))
    return tuple(itens)


@st.fragment(run_every="4s")
def monitorar_alteracoes():
    # Em Firebase/Render, monitora mudanca real no ultimo evento.
    # So atualiza quando houver nova manipulacao.
    if firebase_store.is_enabled():
        token_atual = firebase_store.get_latest_field_value(COLECAO_EVENTOS, "DataHora")
        if "firebase_evento_token" not in st.session_state:
            st.session_state["firebase_evento_token"] = token_atual
            return
        if token_atual != st.session_state["firebase_evento_token"]:
            st.session_state["firebase_evento_token"] = token_atual
            st.rerun()
        st.empty()
        return

    assinatura_atual = assinatura_arquivos()
    if "assinatura_arquivos" not in st.session_state:
        st.session_state["assinatura_arquivos"] = assinatura_atual
        return

    if assinatura_atual != st.session_state["assinatura_arquivos"]:
        st.session_state["assinatura_arquivos"] = assinatura_atual
        st.rerun()

    # Mantem o fragmento ativo sem poluir interface.
    st.empty()


@st.fragment(run_every="5s")
def dashboard_live():
    if firebase_store.is_enabled():
        try:
            firebase_store._invalidate_collection_cache(COLECAO_DADOS)
            firebase_store._invalidate_collection_cache(COLECAO_EVENTOS)
        except Exception:
            pass
    df_live = carregar_dados()
    ev_live = carregar_eventos()
    render_dashboard_visual(df_live, ev_live)


def salvar_dados(df):
    if firebase_store.is_enabled():
        firebase_store.save_collection_df(COLECAO_DADOS, df, key_field="ID")
    else:
        df.to_csv(ARQUIVO_DADOS, index=False)


def carregar_dados():
    if firebase_store.is_enabled():
        df = firebase_store.load_collection_df(COLECAO_DADOS, COLUNAS_BASE + COLUNAS_CORES)
        if df.empty:
            df = pd.DataFrame(columns=COLUNAS_BASE + COLUNAS_CORES)
            salvar_dados(df)
            return df
    else:
        if not os.path.exists(ARQUIVO_DADOS):
            df = pd.DataFrame(columns=COLUNAS_BASE + COLUNAS_CORES)
            salvar_dados(df)
            return df
        df = pd.read_csv(ARQUIVO_DADOS)
    for coluna in COLUNAS_BASE + COLUNAS_CORES:
        if coluna not in df.columns:
            df[coluna] = 0 if coluna.startswith("Cor_") else ""

    df["ID"] = pd.to_numeric(df["ID"], errors="coerce").fillna(0).astype(int)
    df["Pedido"] = pd.to_numeric(df["Pedido"], errors="coerce").fillna(0).astype(int)
    df["Qtd"] = pd.to_numeric(df["Qtd"], errors="coerce").fillna(0).astype(int)
    for coluna in COLUNAS_CORES:
        df[coluna] = pd.to_numeric(df[coluna], errors="coerce").fillna(0).astype(int)

    # Migracao de base antiga sem campo Pedido.
    sem_pedido = df["Pedido"] <= 0
    if sem_pedido.any():
        df.loc[sem_pedido, "Pedido"] = df.loc[sem_pedido, "ID"]
        df["Pedido"] = pd.to_numeric(df["Pedido"], errors="coerce").fillna(0).astype(int)
    # Garante prazo de 3 dias para qualquer registro sem prazo.
    sem_prazo = df["PrazoFinalizacao"].astype(str).str.strip() == ""
    if sem_prazo.any():
        df.loc[sem_prazo, "PrazoFinalizacao"] = gerar_prazo_padrao()

    if sem_pedido.any() or sem_prazo.any():
        salvar_dados(df)

    return df


def carregar_eventos():
    if firebase_store.is_enabled():
        df = firebase_store.load_collection_df(
            COLECAO_EVENTOS,
            COLUNAS_EVENTOS,
            order_by="DataHora",
            descending=True,
            limit=LIMITE_EVENTOS_FIREBASE,
        )
        if df.empty:
            df = pd.DataFrame(columns=COLUNAS_EVENTOS)
            return df
    else:
        if not os.path.exists(ARQUIVO_EVENTOS):
            df = pd.DataFrame(columns=COLUNAS_EVENTOS)
            df.to_csv(ARQUIVO_EVENTOS, index=False)
            return df
        df = pd.read_csv(ARQUIVO_EVENTOS)
    for coluna in COLUNAS_EVENTOS:
        if coluna not in df.columns:
            df[coluna] = ""
    # Corrige bases antigas/incompletas: se Data estiver vazia, deriva de DataHora.
    dh = pd.to_datetime(df.get("DataHora", ""), errors="coerce")
    data_atual = df.get("Data", "").astype(str).str.strip()
    data_derivada = dh.dt.strftime("%Y-%m-%d").fillna("")
    df["Data"] = data_atual.where(data_atual != "", data_derivada)

    df["Pedido"] = pd.to_numeric(df["Pedido"], errors="coerce").fillna(0).astype(int)
    df["ModeloID"] = pd.to_numeric(df["ModeloID"], errors="coerce").fillna(0).astype(int)
    df["Grades"] = pd.to_numeric(df["Grades"], errors="coerce").fillna(0).astype(int)
    df["Qtd"] = pd.to_numeric(df["Qtd"], errors="coerce").fillna(0).astype(int)
    if "DataHora" in df.columns:
        df = df.sort_values(by="DataHora", ascending=False, kind="stable").reset_index(drop=True)
    return df


def registrar_evento(
    acao,
    pedido,
    modeloid=0,
    cliente="",
    modelo="",
    status_antes="",
    status_depois="",
    grades=0,
    qtd=0,
    detalhes="",
):
    agora = datetime.now()
    novo = {
        "DataHora": agora.strftime("%Y-%m-%d %H:%M:%S.%f"),
        "Data": agora.strftime("%Y-%m-%d"),
        "Hora": agora.strftime("%H:%M:%S"),
        "Pedido": int(pedido) if pedido else 0,
        "ModeloID": int(modeloid) if modeloid else 0,
        "Cliente": str(cliente),
        "Modelo": str(modelo),
        "Acao": str(acao),
        "StatusAntes": str(status_antes),
        "StatusDepois": str(status_depois),
        "Grades": int(grades),
        "Qtd": int(qtd),
        "Detalhes": str(detalhes),
    }
    if firebase_store.is_enabled():
        firebase_store.append_document(COLECAO_EVENTOS, novo)
    else:
        eventos = carregar_eventos()
        eventos = pd.concat([eventos, pd.DataFrame([novo])], ignore_index=True)
        eventos.to_csv(ARQUIVO_EVENTOS, index=False)


def proximo_status(status):
    if status not in STATUS_FLUXO:
        return None
    idx = STATUS_FLUXO.index(status)
    return STATUS_FLUXO[idx + 1] if idx < len(STATUS_FLUXO) - 1 else None


def status_anterior(status):
    if status not in STATUS_FLUXO:
        return None
    idx = STATUS_FLUXO.index(status)
    return STATUS_FLUXO[idx - 1] if idx > 0 else None


def progresso_status(status):
    if status not in STATUS_FLUXO:
        return 0.0
    return (STATUS_FLUXO.index(status) + 1) / len(STATUS_FLUXO)


def total_grades_row(row):
    return int(sum(int(row[col]) for col in COLUNAS_CORES))


def gerar_prazo_padrao():
    return (datetime.now() + timedelta(days=3)).strftime("%Y-%m-%d")


def prazo_legivel(valor):
    try:
        return pd.to_datetime(valor).strftime("%d/%m/%Y")
    except Exception:
        return "-"


def dias_para_prazo(valor):
    try:
        dt = pd.to_datetime(str(valor), errors="coerce")
        if pd.isna(dt):
            return None
        hoje = datetime.now().date()
        return (dt.date() - hoje).days
    except Exception:
        return None


def badge_prazo(valor):
    dias = dias_para_prazo(valor)
    if dias is None:
        return "<span style='color:#5d6f89'>Prazo: -</span>"
    if dias >= 3:
        cor = "#1f9d55"
    elif dias >= 1:
        cor = "#d97706"
    else:
        cor = "#dc2626"
    return f"<span style='color:{cor}; font-weight:800'>Prazo: {dias} dia(s)</span>"


def observacao_legivel(valor):
    txt = str(valor).strip()
    if not txt or txt.lower() == "nan":
        return ""
    return txt


def pedido_vencido(grupo):
    if grupo.empty:
        return False
    status = str(grupo.iloc[0]["Status"])
    if status == STATUS_FLUXO[-1]:
        return False
    prazo_raw = str(grupo.iloc[0].get("PrazoFinalizacao", "")).strip()
    if not prazo_raw:
        return False
    prazo_dt = pd.to_datetime(prazo_raw, errors="coerce")
    if pd.isna(prazo_dt):
        return False
    return prazo_dt.date() < datetime.now().date()


def linhas_cores(row):
    linhas = []
    for nome_cor, coluna in zip(CORES, COLUNAS_CORES):
        qtd = int(row[coluna])
        if qtd > 0:
            grade_txt = "grade" if qtd == 1 else "grades"
            linhas.append(f"- {qtd} {grade_txt} {nome_cor.lower()}")
    return linhas


def mover_pedido(pedido_id, novo_status):
    df = carregar_dados()
    grupo = df[df["Pedido"] == pedido_id]
    if grupo.empty:
        return
    status_antes = str(grupo.iloc[0]["Status"])
    qtd_total = int(grupo["Qtd"].sum())
    grades_total = int(sum(total_grades_row(r) for _, r in grupo.iterrows()))
    cliente = str(grupo.iloc[0]["Cliente"])

    df.loc[df["Pedido"] == pedido_id, "Status"] = novo_status
    salvar_dados(df)

    registrar_evento(
        acao="MOVER_STATUS",
        pedido=pedido_id,
        cliente=cliente,
        status_antes=status_antes,
        status_depois=novo_status,
        grades=grades_total,
        qtd=qtd_total,
        detalhes=f"Pedido movido de '{status_antes}' para '{novo_status}'.",
    )
    if novo_status == STATUS_FLUXO[-1]:
        registrar_evento(
            acao="FINALIZAR_PEDIDO",
            pedido=pedido_id,
            cliente=cliente,
            status_antes=status_antes,
            status_depois=novo_status,
            grades=grades_total,
            qtd=qtd_total,
            detalhes="Pedido finalizado.",
        )
    st.rerun()


def deletar_pedido(pedido_id):
    df = carregar_dados()
    grupo = df[df["Pedido"] == pedido_id]
    if grupo.empty:
        return
    cliente = str(grupo.iloc[0]["Cliente"])
    qtd_total = int(grupo["Qtd"].sum())
    grades_total = int(sum(total_grades_row(r) for _, r in grupo.iterrows()))
    status_atual = str(grupo.iloc[0]["Status"])
    total_modelos = len(grupo)

    registrar_evento(
        acao="ARQUIVAR_PEDIDO",
        pedido=pedido_id,
        cliente=cliente,
        status_antes=status_atual,
        status_depois="Arquivado",
        grades=grades_total,
        qtd=qtd_total,
        detalhes=f"Pedido arquivado com {total_modelos} modelo(s).",
    )

    df = df[df["Pedido"] != pedido_id]
    salvar_dados(df)
    st.rerun()


def deletar_modelo(id_modelo):
    df = carregar_dados()
    linha = df[df["ID"] == id_modelo]
    if linha.empty:
        return

    row = linha.iloc[0]
    pedido_id = int(row["Pedido"])
    grades = total_grades_row(row)
    qtd = int(row["Qtd"])

    registrar_evento(
        acao="EXCLUIR_MODELO",
        pedido=pedido_id,
        modeloid=id_modelo,
        cliente=str(row["Cliente"]),
        modelo=str(row["Modelo"]),
        status_antes=str(row["Status"]),
        grades=grades,
        qtd=qtd,
        detalhes="Modelo removido do pedido.",
    )

    df = df[df["ID"] != id_modelo]
    salvar_dados(df)
    st.rerun()


def atualizar_modelo(id_modelo, cliente, numero_cliente, observacao, modelo, cores):
    df = carregar_dados()
    linha = df[df["ID"] == id_modelo]
    if linha.empty:
        return
    antigo = linha.iloc[0]
    pedido_id = int(antigo["Pedido"])
    total_grades = int(sum(int(v) for v in cores.values()))
    qtd = total_grades * PECAS_POR_GRADE

    cliente_fmt = texto_maiusculo(cliente)
    numero_cliente_fmt = normalizar_numero_cliente(numero_cliente)
    modelo_fmt = texto_maiusculo(modelo)
    df.loc[df["ID"] == id_modelo, "Cliente"] = cliente_fmt
    df.loc[df["ID"] == id_modelo, "NumeroCliente"] = numero_cliente_fmt
    df.loc[df["ID"] == id_modelo, "Observacao"] = str(observacao).strip()
    df.loc[df["ID"] == id_modelo, "Modelo"] = modelo_fmt
    df.loc[df["ID"] == id_modelo, "Qtd"] = qtd
    for coluna, valor in cores.items():
        df.loc[df["ID"] == id_modelo, coluna] = int(valor)
    salvar_dados(df)

    registrar_evento(
        acao="EDITAR_MODELO",
        pedido=pedido_id,
        modeloid=id_modelo,
        cliente=cliente_fmt,
        modelo=modelo_fmt,
        status_antes=str(antigo["Status"]),
        status_depois=str(antigo["Status"]),
        grades=total_grades,
        qtd=qtd,
        detalhes=f"Modelo editado. Antes: '{antigo['Modelo']}'.",
    )
    st.rerun()


def adicionar_modelo_ao_pedido(
    pedido_id,
    cliente,
    numero_cliente,
    observacao,
    status,
    entrada,
    prazo_finalizacao,
    responsavel_lancamento,
    modelo,
    cores,
):
    df = carregar_dados()
    novo_id = int(df["ID"].max() + 1) if not df.empty else 1
    total_grades = int(sum(int(v) for v in cores.values()))
    qtd = total_grades * PECAS_POR_GRADE
    cliente_fmt = texto_maiusculo(cliente)
    numero_cliente_fmt = normalizar_numero_cliente(numero_cliente)
    observacao_fmt = str(observacao).strip()
    modelo_fmt = texto_maiusculo(modelo)
    novo = {
        "ID": novo_id,
        "Pedido": pedido_id,
        "Cliente": cliente_fmt,
        "NumeroCliente": numero_cliente_fmt,
        "Observacao": observacao_fmt,
        "Modelo": modelo_fmt,
        "Qtd": qtd,
        "Status": status,
        "Entrada": entrada,
        "PrazoFinalizacao": prazo_finalizacao,
        "ResponsavelLancamento": responsavel_lancamento,
        **{k: int(v) for k, v in cores.items()},
    }
    df = pd.concat([df, pd.DataFrame([novo])], ignore_index=True)
    salvar_dados(df)

    registrar_evento(
        acao="ADICIONAR_MODELO",
        pedido=pedido_id,
        modeloid=novo_id,
        cliente=cliente_fmt,
        modelo=modelo_fmt,
        status_depois=status,
        grades=total_grades,
        qtd=qtd,
        detalhes="Novo modelo adicionado ao pedido.",
    )
    st.rerun()


def contador_producao_dia(eventos):
    hoje = datetime.now().strftime("%Y-%m-%d")
    df = eventos[(eventos["Data"] == hoje) & (eventos["Acao"] == "MOVER_STATUS") & (eventos["StatusDepois"] == "4. Finalizado")]
    return int(df["Qtd"].sum()) if not df.empty else 0


def eventos_de_finalizacao(eventos_df):
    if eventos_df.empty:
        return eventos_df.copy()
    return eventos_df[
        ((eventos_df["Acao"] == "FINALIZAR_PEDIDO"))
        | ((eventos_df["Acao"] == "MOVER_STATUS") & (eventos_df["StatusDepois"] == "4. Finalizado"))
    ].copy()


def resumo_relatorio_producao(df_atual, eventos_filtrados):
    status_idx = {s: i for i, s in enumerate(STATUS_FLUXO)}
    movimentos = eventos_filtrados[eventos_filtrados["Acao"] == "MOVER_STATUS"].copy()
    retrabalho = 0
    if not movimentos.empty:
        for _, r in movimentos.iterrows():
            a = status_idx.get(str(r["StatusAntes"]), -1)
            d = status_idx.get(str(r["StatusDepois"]), -1)
            if d < a:
                retrabalho += 1

    # Historico no periodo selecionado (sobe mesmo apos arquivar/excluir)
    lancados_periodo = int((eventos_filtrados["Acao"] == "LANCAR_PEDIDO").sum())
    finalizados_periodo = int(eventos_de_finalizacao(eventos_filtrados)["Pedido"].nunique())
    arquivados = int((eventos_filtrados["Acao"] == "ARQUIVAR_PEDIDO").sum())
    pendentes_periodo = max(lancados_periodo - finalizados_periodo - arquivados, 0)

    texto = []
    texto.append(f"- Pedidos pendentes no periodo selecionado: {int(pendentes_periodo)}")
    texto.append(f"- Pedidos finalizados no periodo selecionado: {int(finalizados_periodo)}")
    texto.append(f"- Pedidos arquivados no periodo selecionado: {int(arquivados)}")
    texto.append(f"- Movimentos de retrabalho (volta de etapa): {int(retrabalho)}")
    return "\n".join(texto)


def tabela_pedidos_finalizados(eventos_filtrados):
    colunas_saida = ["Pedido", "Cliente", "Lancado em", "Finalizado em", "Tempo ate finalizar", "Confirmacao"]
    if eventos_filtrados.empty:
        return pd.DataFrame(columns=colunas_saida)

    ev = eventos_filtrados.copy()
    ev["DataHora_dt"] = pd.to_datetime(ev["DataHora"], errors="coerce")
    ev = ev[~ev["DataHora_dt"].isna()].copy()
    if ev.empty:
        return pd.DataFrame(columns=colunas_saida)

    lanc = ev[ev["Acao"] == "LANCAR_PEDIDO"][["Pedido", "Cliente", "DataHora_dt"]].copy()
    fin = eventos_de_finalizacao(ev)[["Pedido", "Cliente", "Acao", "DataHora_dt"]].copy()
    if fin.empty:
        return pd.DataFrame(columns=colunas_saida)

    # Evita duplicidade da mesma finalizacao (MOVER_STATUS + FINALIZAR_PEDIDO no mesmo segundo).
    fin["prioridade"] = fin["Acao"].apply(lambda a: 0 if str(a) == "FINALIZAR_PEDIDO" else 1)
    fin["chave_final"] = fin["Pedido"].astype(str) + "|" + fin["DataHora_dt"].dt.floor("s").astype(str)
    fin = fin.sort_values(["chave_final", "prioridade", "DataHora_dt"]).drop_duplicates("chave_final", keep="first")
    fin = fin.sort_values(["Pedido", "DataHora_dt"]).rename(columns={"DataHora_dt": "Finalizado_dt", "Cliente": "Cliente_fin"})

    if lanc.empty:
        out = fin.copy()
        out["Lancado_dt"] = pd.NaT
        out["Cliente_lanc"] = ""
    else:
        lanc = lanc.sort_values(["Pedido", "DataHora_dt"]).rename(columns={"DataHora_dt": "Lancado_dt", "Cliente": "Cliente_lanc"})
        rows = []
        for _, f in fin.iterrows():
            pedido = int(f["Pedido"])
            final_dt = f["Finalizado_dt"]
            cand = lanc[(lanc["Pedido"] == pedido) & (lanc["Lancado_dt"] <= final_dt)]
            if cand.empty:
                lancado_dt = pd.NaT
                cliente_lanc = ""
            else:
                last = cand.iloc[-1]
                lancado_dt = last["Lancado_dt"]
                cliente_lanc = last["Cliente_lanc"]
            row = f.to_dict()
            row["Lancado_dt"] = lancado_dt
            row["Cliente_lanc"] = cliente_lanc
            rows.append(row)
        out = pd.DataFrame(rows)

    out["Cliente"] = out["Cliente_fin"].fillna(out["Cliente_lanc"]).fillna("-")
    out["Lancado em"] = out["Lancado_dt"].dt.strftime("%d/%m/%Y %H:%M").fillna("-")
    out["Finalizado em"] = out["Finalizado_dt"].dt.strftime("%d/%m/%Y %H:%M").fillna("-")

    delta = out["Finalizado_dt"] - out["Lancado_dt"]
    horas = (delta.dt.total_seconds() / 3600).round(1)
    out["Tempo ate finalizar"] = horas.apply(lambda h: f"{h}h" if pd.notna(h) and h >= 0 else "-")
    out["Confirmacao"] = "Finalizado"

    out = out[["Pedido", "Cliente", "Lancado em", "Finalizado em", "Tempo ate finalizar", "Confirmacao"]]
    out = out.sort_values(by="Finalizado em", ascending=False, kind="stable").reset_index(drop=True)
    return out


def render_dashboard_detalhado(df_atual, eventos, agora):
    st.markdown(
        f"""
        <div class="dash2-hero">
          <div>
            <p class="dash2-title">Painel de Acompanhamento</p>
            <p class="dash2-sub">Visualizacao rapida do fluxo, com foco em leitura.</p>
          </div>
          <span class="dash2-chip">Atualizado: {agora}</span>
        </div>
        """,
        unsafe_allow_html=True,
    )
    prod_hoje = contador_producao_dia(eventos)
    pedidos_abertos = int(df_atual[df_atual["Status"] != "4. Finalizado"]["Pedido"].nunique())
    pedidos_finalizados = int(df_atual[df_atual["Status"] == "4. Finalizado"]["Pedido"].nunique())
    pecas_em_aberto = int(df_atual[df_atual["Status"] != "4. Finalizado"]["Qtd"].sum()) if not df_atual.empty else 0

    c1, c2, c3, c4 = st.columns(4)
    c1.markdown(
        f"<div class='dash2-kpi'><p class='dash2-kpi-label'>Pecas finalizadas hoje</p><p class='dash2-kpi-value'>{prod_hoje}</p></div>",
        unsafe_allow_html=True,
    )
    c2.markdown(
        f"<div class='dash2-kpi'><p class='dash2-kpi-label'>Pedidos em aberto</p><p class='dash2-kpi-value'>{pedidos_abertos}</p></div>",
        unsafe_allow_html=True,
    )
    c3.markdown(
        f"<div class='dash2-kpi'><p class='dash2-kpi-label'>Pedidos finalizados</p><p class='dash2-kpi-value'>{pedidos_finalizados}</p></div>",
        unsafe_allow_html=True,
    )
    c4.markdown(
        f"<div class='dash2-kpi'><p class='dash2-kpi-label'>Pecas em aberto</p><p class='dash2-kpi-value'>{pecas_em_aberto}</p></div>",
        unsafe_allow_html=True,
    )
    st.markdown("**Etapas em andamento**")

    col1, col2, col3, col4 = st.columns(4)
    fases = {
        STATUS_FLUXO[0]: (col1, "Separar pedido"),
        STATUS_FLUXO[1]: (col2, "Em costura"),
        STATUS_FLUXO[2]: (col3, "Acabamento"),
        STATUS_FLUXO[3]: (col4, "Prontos"),
    }

    for status_chave, (coluna_st, titulo_visual) in fases.items():
        with coluna_st:
            pedidos_fase = df_atual[df_atual["Status"].astype(str).str.strip() == str(status_chave).strip()]
            total_pedidos_etapa = int(pedidos_fase["Pedido"].nunique()) if not pedidos_fase.empty else 0
            st.markdown(
                f"""
                <div class="dash2-stage">
                  <div class="dash2-stage-head">
                    <span class="dash2-stage-title">{titulo_visual}</span>
                    <span class="dash2-stage-count">{total_pedidos_etapa}</span>
                  </div>
                  <div class="dash2-stage-body"></div>
                </div>
                """,
                unsafe_allow_html=True,
            )
            if total_pedidos_etapa == 0:
                st.caption("Sem pedidos nesta etapa.")
                continue

            pecas_etapa = int(pedidos_fase["Qtd"].sum()) if not pedidos_fase.empty else 0
            st.caption(f"{pecas_etapa} peca(s) nesta etapa")
            for pedido_id in sorted(pedidos_fase["Pedido"].unique())[:10]:
                grupo = pedidos_fase[pedidos_fase["Pedido"] == pedido_id]
                principal = grupo.iloc[0]
                total_grades = int(sum(total_grades_row(r) for _, r in grupo.iterrows()))
                total_pecas = int(grupo["Qtd"].sum())
                titulo_cliente = str(principal["Cliente"]).strip() or f"Pedido #{pedido_id}"
                prazo_txt = prazo_legivel(principal.get("PrazoFinalizacao", ""))
                responsavel_lancamento = str(principal.get("ResponsavelLancamento", "")).strip() or "-"
                cliente_info = str(principal.get("Cliente", "-")).strip() or "-"
                num_info = str(principal.get("NumeroCliente", "-")).strip() or "-"
                obs_info = observacao_legivel(principal.get("Observacao", ""))

                prazo_badge = badge_prazo(principal.get("PrazoFinalizacao", ""))
                st.markdown(
                    f"**Cliente:** {cliente_info}  |  {prazo_badge}  |  "
                    f"**Lancado por:** {responsavel_lancamento}  |  **Num:** {num_info}",
                    unsafe_allow_html=True,
                )
                if obs_info:
                    st.markdown(f"**Observacao:** {obs_info}")

                titulo_dash = f"{titulo_cliente} | Num {num_info}"
                with st.expander(titulo_dash, expanded=True):
                    card = st.container(border=True)
                    card.markdown(f"**Pedido #{pedido_id}**")
                    card.caption(f"Lancado por: {responsavel_lancamento}")
                    card.caption(f"Cliente: {principal['Cliente']}")
                    card.caption(f"Numero: {num_info}")
                    if obs_info:
                        card.caption(f"Observacao: {obs_info}")
                    card.caption(f"{len(grupo)} modelo(s) | {total_grades} grade(s) | {total_pecas} peca(s)")
                    card.progress(progresso_status(status_chave))
                    card.caption(f"Entrada: {principal['Entrada']}")
                    card.markdown(f"{prazo_badge}", unsafe_allow_html=True)
                    if pedido_vencido(grupo):
                        card.error("Prazo vencido para finalizar este pedido.")

                    card.markdown("**Modelos:**")
                    for _, item in grupo.iterrows():
                        model_cols = card.columns([5, 1])
                        model_cols[0].markdown(f"**{item['Modelo']}**")
                        for linha_cor in linhas_cores(item):
                            card.caption(linha_cor)
                        card.caption(f"Total: {total_grades_row(item)} grade(s) | {int(item['Qtd'])} peca(s)")


def render_dashboard_visual(df_atual, eventos):
    agora = datetime.now().strftime("%d/%m/%Y %H:%M")
    try:
        versao_path = os.path.abspath(__file__)
        versao_data = datetime.fromtimestamp(os.path.getmtime(__file__)).strftime("%d/%m/%Y %H:%M:%S")
        st.caption(f"Versao dashboard: {versao_data} | {versao_path}")
    except Exception:
        pass
    if perfil_atual() == "dashboard":
        render_dashboard_detalhado(df_atual, eventos, agora)
        return
    st.markdown(
        f"""
        <div class="dash2-hero">
          <div>
            <p class="dash2-title">Painel de Acompanhamento</p>
            <p class="dash2-sub">Visualizacao rapida do fluxo, com foco em leitura.</p>
          </div>
          <span class="dash2-chip">Atualizado: {agora}</span>
        </div>
        """,
        unsafe_allow_html=True,
    )
    prod_hoje = contador_producao_dia(eventos)
    pedidos_abertos = int(df_atual[df_atual["Status"] != "4. Finalizado"]["Pedido"].nunique())
    pedidos_finalizados = int(df_atual[df_atual["Status"] == "4. Finalizado"]["Pedido"].nunique())
    pecas_em_aberto = int(df_atual[df_atual["Status"] != "4. Finalizado"]["Qtd"].sum()) if not df_atual.empty else 0

    c1, c2, c3, c4 = st.columns(4)
    c1.markdown(
        f"<div class='dash2-kpi'><p class='dash2-kpi-label'>Pecas finalizadas hoje</p><p class='dash2-kpi-value'>{prod_hoje}</p></div>",
        unsafe_allow_html=True,
    )
    c2.markdown(
        f"<div class='dash2-kpi'><p class='dash2-kpi-label'>Pedidos em aberto</p><p class='dash2-kpi-value'>{pedidos_abertos}</p></div>",
        unsafe_allow_html=True,
    )
    c3.markdown(
        f"<div class='dash2-kpi'><p class='dash2-kpi-label'>Pedidos finalizados</p><p class='dash2-kpi-value'>{pedidos_finalizados}</p></div>",
        unsafe_allow_html=True,
    )
    c4.markdown(
        f"<div class='dash2-kpi'><p class='dash2-kpi-label'>Pecas em aberto</p><p class='dash2-kpi-value'>{pecas_em_aberto}</p></div>",
        unsafe_allow_html=True,
    )
    st.markdown("**Etapas em andamento**")

    col1, col2, col3, col4 = st.columns(4)
    fases = {
        STATUS_FLUXO[0]: (col1, "Separar pedido"),
        STATUS_FLUXO[1]: (col2, "Em costura"),
        STATUS_FLUXO[2]: (col3, "Acabamento"),
        STATUS_FLUXO[3]: (col4, "Prontos"),
    }

    for status_chave, (coluna_st, titulo_visual) in fases.items():
        with coluna_st:
            pedidos_fase = df_atual[df_atual["Status"].astype(str).str.strip() == str(status_chave).strip()]
            total_pedidos_etapa = int(pedidos_fase["Pedido"].nunique()) if not pedidos_fase.empty else 0
            st.markdown(
                f"""
                <div class="dash2-stage">
                  <div class="dash2-stage-head">
                    <span class="dash2-stage-title">{titulo_visual}</span>
                    <span class="dash2-stage-count">{total_pedidos_etapa}</span>
                  </div>
                  <div class="dash2-stage-body"></div>
                </div>
                """,
                unsafe_allow_html=True,
            )

            if total_pedidos_etapa == 0:
                st.caption("Sem pedidos nesta etapa.")
                continue

            pecas_etapa = int(pedidos_fase["Qtd"].sum()) if not pedidos_fase.empty else 0
            st.caption(f"{pecas_etapa} peca(s) nesta etapa")
            for pedido_id in sorted(pedidos_fase["Pedido"].unique())[:10]:
                grupo = pedidos_fase[pedidos_fase["Pedido"] == pedido_id]
                principal = grupo.iloc[0]
                total_grades = int(sum(total_grades_row(r) for _, r in grupo.iterrows()))
                total_pecas = int(grupo["Qtd"].sum())
                titulo_cliente = str(principal["Cliente"]).strip() or f"Pedido #{pedido_id}"
                resumo = f"{total_pecas} peca(s)"
                anterior = status_anterior(status_chave)
                proximo = proximo_status(status_chave)
                pode_pronto_este_pedido = pode_mover_pronto(status_chave)
                prazo_badge = badge_prazo(principal.get("PrazoFinalizacao", ""))

                if perfil_atual() == "dashboard":
                    prazo_txt = prazo_legivel(principal.get("PrazoFinalizacao", ""))
                    responsavel_lancamento = str(principal.get("ResponsavelLancamento", "")).strip() or "-"
                    cliente_info = str(principal.get("Cliente", "-")).strip() or "-"
                    num_info = str(principal.get("NumeroCliente", "-")).strip() or "-"
                    obs_info = str(principal.get("Observacao", "")).strip()

                    st.markdown(
                        f"**Cliente:** {cliente_info}  |  **Prazo:** {prazo_txt}  |  "
                        f"**Lancado por:** {responsavel_lancamento}  |  **Num:** {num_info}"
                    )
                    if obs_info:
                        st.markdown(f"**Observacao:** {obs_info}")

                    titulo_dash = f"{titulo_cliente} | Num {num_info} | Prazo {prazo_txt}"
                    with st.expander(titulo_dash, expanded=True):
                        card = st.container(border=True)
                        card.markdown(f"**Pedido #{pedido_id}**")
                        card.caption(f"Lancado por: {responsavel_lancamento}")
                        card.caption(f"Cliente: {principal['Cliente']}")
                        card.caption(f"Numero: {num_info}")
                    if obs_info:
                        card.caption(f"Observacao: {obs_info}")
                        card.caption(f"{len(grupo)} modelo(s) | {total_grades} grade(s) | {total_pecas} peca(s)")
                        card.progress(progresso_status(status_chave))
                        card.caption(f"Entrada: {principal['Entrada']}")
                        card.caption(f"Prazo para finalizar: {prazo_txt}")
                        if pedido_vencido(grupo):
                            card.error("Prazo vencido para finalizar este pedido.")

                        card.markdown("**Modelos:**")
                        for _, item in grupo.iterrows():
                            model_cols = card.columns([5, 1])
                            model_cols[0].markdown(f"**{item['Modelo']}**")
                            for linha_cor in linhas_cores(item):
                                card.caption(linha_cor)
                            card.caption(f"Total: {total_grades_row(item)} grade(s) | {int(item['Qtd'])} peca(s)")
                else:
                    linha_cols = st.columns([4, 1])
                    with linha_cols[0]:
                        with st.expander(f"{titulo_cliente}  |  {resumo}", expanded=False):
                            card = st.container(border=True)
                            card.markdown(f"**Pedido #{pedido_id}**")
                            responsavel_lancamento = str(principal.get("ResponsavelLancamento", "")).strip() or "-"
                            card.caption(f"Lancado por: {responsavel_lancamento}")
                            card.caption(f"Cliente: {principal['Cliente']}")
                            card.caption(f"Numero: {str(principal.get('NumeroCliente', '-')).strip() or '-'}")
                            observacao_card = str(principal.get("Observacao", "")).strip()
                            if observacao_card:
                                card.caption(f"Observacao: {observacao_card}")
                            card.caption(f"{len(grupo)} modelo(s) | {total_grades} grade(s) | {total_pecas} peca(s)")
                            card.progress(progresso_status(status_chave))
                            card.caption(f"Entrada: {principal['Entrada']}")
                            card.markdown(prazo_badge, unsafe_allow_html=True)
                            if pedido_vencido(grupo):
                                card.error("Prazo vencido para finalizar este pedido.")

                            card.markdown("**Modelos:**")
                            for _, item in grupo.iterrows():
                                item_id = int(item["ID"])
                                model_cols = card.columns([5, 1])
                                model_cols[0].markdown(f"**{item['Modelo']}**")
                                if model_cols[1].button(
                                    "X",
                                    key=f"dash_del_model_{item_id}",
                                    help="Excluir modelo",
                                    disabled=not pode_editar,
                                ):
                                    deletar_modelo(item_id)
                                for linha_cor in linhas_cores(item):
                                    card.caption(linha_cor)
                                card.caption(f"Total: {total_grades_row(item)} grade(s) | {int(item['Qtd'])} peca(s)")

                    with linha_cols[1]:
                        if proximo:
                            texto_botao = "Finalizar" if proximo == STATUS_FLUXO[-1] else "Pronto"
                            if st.button(
                                texto_botao,
                                key=f"dash_quick_mv_{pedido_id}",
                                use_container_width=True,
                                disabled=not pode_pronto_este_pedido,
                            ):
                                mover_pedido(int(pedido_id), proximo)
                        else:
                            if st.button(
                                "Arquivar",
                                key=f"dash_quick_del_{pedido_id}",
                                use_container_width=True,
                                disabled=not pode_editar,
                            ):
                                deletar_pedido(int(pedido_id))

                        edit_open_key = f"dash_edit_open_{pedido_id}"
                        add_open_key = f"dash_add_open_{pedido_id}"
                        if edit_open_key not in st.session_state:
                            st.session_state[edit_open_key] = False
                        if add_open_key not in st.session_state:
                            st.session_state[add_open_key] = False

                        btn_top_l, btn_top_r = card.columns(2)
                        btn_bot_l, btn_bot_r = card.columns(2)

                        if anterior:
                            if btn_top_l.button(
                                "Voltar",
                                key=f"dash_back_{pedido_id}",
                                use_container_width=True,
                                disabled=not pode_editar,
                            ):
                                mover_pedido(int(pedido_id), anterior)
                        else:
                            btn_top_l.write("")

                        if proximo:
                            texto_botao = "Finalizar" if proximo == STATUS_FLUXO[-1] else "Pronto"
                            if btn_top_r.button(
                                texto_botao,
                                key=f"dash_mv_{pedido_id}",
                                use_container_width=True,
                                disabled=not pode_pronto_este_pedido,
                            ):
                                mover_pedido(int(pedido_id), proximo)
                        else:
                            if btn_top_r.button(
                                "Arquivar",
                                key=f"dash_del_{pedido_id}",
                                use_container_width=True,
                                disabled=not pode_editar,
                            ):
                                deletar_pedido(int(pedido_id))

                        if btn_bot_l.button(
                            "Editar",
                            key=f"dash_toggle_edit_{pedido_id}",
                            use_container_width=True,
                            disabled=not pode_editar,
                        ):
                            st.session_state[edit_open_key] = not st.session_state[edit_open_key]
                            st.session_state[add_open_key] = False

                        if btn_bot_r.button(
                            "+ Modelo",
                            key=f"dash_toggle_add_{pedido_id}",
                            use_container_width=True,
                            disabled=not pode_editar,
                        ):
                            st.session_state[add_open_key] = not st.session_state[add_open_key]
                            st.session_state[edit_open_key] = False

                        if st.session_state[edit_open_key] and pode_editar:
                            card.markdown("---")
                            card.markdown("**Editar modelo do pedido**")
                            opcoes_itens = [int(x) for x in grupo["ID"].tolist()]
                            mapa_nome = {int(x["ID"]): str(x["Modelo"]) for _, x in grupo.iterrows()}
                            item_id_edit = card.selectbox(
                                "Escolha o modelo",
                                options=opcoes_itens,
                                format_func=lambda x: f"#{x} - {mapa_nome.get(x, '')}",
                                key=f"dash_edit_select_{pedido_id}",
                                disabled=not pode_editar,
                            )
                            item_edit = grupo[grupo["ID"] == item_id_edit].iloc[0]

                            with card.form(key=f"dash_form_edit_{pedido_id}"):
                                edit_cliente = st.text_input(
                                    "Cliente",
                                    value=str(item_edit["Cliente"]),
                                    key=f"dash_edit_cliente_{pedido_id}",
                                    disabled=not pode_editar,
                                )
                                edit_numero_cliente = st.text_input(
                                    "Numero do cliente",
                                    value=str(item_edit.get("NumeroCliente", "")),
                                    key=f"dash_edit_numero_cliente_{pedido_id}",
                                    disabled=not pode_editar,
                                )
                                edit_observacao = st.text_input(
                                    "Observacao",
                                    value=str(item_edit.get("Observacao", "")),
                                    key=f"dash_edit_observacao_{pedido_id}",
                                    disabled=not pode_editar,
                                )
                                opcoes_modelo_edit = list(MODELOS_DISPONIVEIS)
                                modelo_atual_edit = texto_maiusculo(str(item_edit["Modelo"]))
                                if modelo_atual_edit and modelo_atual_edit not in opcoes_modelo_edit:
                                    opcoes_modelo_edit = [modelo_atual_edit] + opcoes_modelo_edit
                                edit_modelo = st.selectbox(
                                    "Modelo/Grade",
                                    options=opcoes_modelo_edit,
                                    index=opcoes_modelo_edit.index(modelo_atual_edit)
                                    if modelo_atual_edit in opcoes_modelo_edit
                                    else 0,
                                    key=f"dash_edit_modelo_{pedido_id}",
                                    disabled=not pode_editar,
                                )
                                edit_cores = {}
                                cols_edit = st.columns(2)
                                for i, (nome_cor, coluna_cor) in enumerate(zip(CORES, COLUNAS_CORES)):
                                    edit_cores[coluna_cor] = cols_edit[i % 2].number_input(
                                        nome_cor,
                                        min_value=0,
                                        value=int(item_edit[coluna_cor]),
                                        step=1,
                                        key=f"dash_edit_{coluna_cor}_{pedido_id}_{item_id_edit}",
                                        disabled=not pode_editar,
                                    )

                                total_edit_grades = int(sum(edit_cores.values()))
                                st.caption(
                                    f"Novo total: {total_edit_grades} grade(s) = {total_edit_grades * PECAS_POR_GRADE} peca(s)"
                                )
                                salvar_edicao = st.form_submit_button("Salvar alteracoes", disabled=not pode_editar)
                                if salvar_edicao:
                                    if not edit_cliente.strip() or not edit_modelo.strip():
                                        st.warning("Cliente e modelo sao obrigatorios.")
                                    elif total_edit_grades <= 0:
                                        st.warning("Informe ao menos 1 grade para salvar.")
                                    else:
                                        atualizar_modelo(
                                            item_id_edit,
                                            edit_cliente,
                                            edit_numero_cliente,
                                            edit_observacao,
                                            edit_modelo,
                                            edit_cores,
                                        )

                        if st.session_state[add_open_key] and pode_editar:
                            card.markdown("---")
                            card.markdown("**Acrescentar modelo neste pedido**")
                            with card.form(key=f"dash_form_add_model_{pedido_id}"):
                                add_modelo = st.selectbox(
                                    "Novo modelo/grade",
                                    options=MODELOS_DISPONIVEIS,
                                    key=f"dash_add_modelo_{pedido_id}",
                                    disabled=not pode_editar,
                                )
                                add_cores = {}
                                cols_add = st.columns(2)
                                for i, (nome_cor, coluna_cor) in enumerate(zip(CORES, COLUNAS_CORES)):
                                    add_cores[coluna_cor] = cols_add[i % 2].number_input(
                                        nome_cor,
                                        min_value=0,
                                        value=0,
                                        step=1,
                                        key=f"dash_add_{coluna_cor}_{pedido_id}",
                                        disabled=not pode_editar,
                                    )

                                total_add_grades = int(sum(add_cores.values()))
                                st.caption(
                                    f"Novo modelo: {total_add_grades} grade(s) = {total_add_grades * PECAS_POR_GRADE} peca(s)"
                                )
                                add_submit = st.form_submit_button("Adicionar modelo", disabled=not pode_editar)
                                if add_submit:
                                    if not add_modelo.strip():
                                        st.warning("Informe o nome do novo modelo.")
                                    elif total_add_grades <= 0:
                                        st.warning("Informe ao menos 1 grade para o novo modelo.")
                                    else:
                                        adicionar_modelo_ao_pedido(
                                            pedido_id=int(pedido_id),
                                            cliente=str(principal["Cliente"]),
                                            numero_cliente=str(principal.get("NumeroCliente", "-")),
                                            observacao=str(principal.get("Observacao", "")),
                                            status=str(principal["Status"]),
                                            entrada=str(principal["Entrada"]),
                                            prazo_finalizacao=str(principal.get("PrazoFinalizacao", gerar_prazo_padrao())),
                                            responsavel_lancamento=str(principal.get("ResponsavelLancamento", "")).strip()
                                            or str(st.session_state.get("usuario", "")),
                                            modelo=add_modelo,
                                            cores=add_cores,
                                        )


def render_operacao(df_atual, eventos):
    prod_hoje = contador_producao_dia(eventos)
    pedidos_abertos = int(df_atual[df_atual["Status"] != "4. Finalizado"]["Pedido"].nunique())
    pedidos_finalizados = int(df_atual[df_atual["Status"] == "4. Finalizado"]["Pedido"].nunique())

    k1, k2, k3 = st.columns(3)
    k1.markdown(
        f"<div class='kpi-card'><p class='kpi-label'>Producao do dia (pecas finalizadas)</p><p class='kpi-value'>{prod_hoje}</p></div>",
        unsafe_allow_html=True,
    )
    k2.markdown(
        f"<div class='kpi-card'><p class='kpi-label'>Pedidos em aberto</p><p class='kpi-value'>{pedidos_abertos}</p></div>",
        unsafe_allow_html=True,
    )
    k3.markdown(
        f"<div class='kpi-card'><p class='kpi-label'>Pedidos finalizados</p><p class='kpi-value'>{pedidos_finalizados}</p></div>",
        unsafe_allow_html=True,
    )

    cols = st.columns(4)
    fases = {
        STATUS_FLUXO[0]: (cols[0], "Separar pedido"),
        STATUS_FLUXO[1]: (cols[1], "Em costura"),
        STATUS_FLUXO[2]: (cols[2], "Acabamento"),
        STATUS_FLUXO[3]: (cols[3], "Prontos"),
    }

    for status_chave, (coluna_st, titulo_visual) in fases.items():
        with coluna_st:
            pedidos_fase = df_atual[df_atual["Status"].astype(str).str.strip() == str(status_chave).strip()]
            total_pedidos_etapa = int(pedidos_fase["Pedido"].nunique()) if not pedidos_fase.empty else 0
            st.markdown(
                f"""
                <div class="stage-shell">
                  <div class="stage-head">
                    <span class="stage-title">{titulo_visual}</span>
                    <span class="stage-count">{total_pedidos_etapa}</span>
                  </div>
                </div>
                """,
                unsafe_allow_html=True,
            )
            if total_pedidos_etapa == 0:
                st.caption("Sem pedidos nesta etapa.")
                continue

            for pedido_id in sorted(pedidos_fase["Pedido"].unique()):
                grupo = pedidos_fase[pedidos_fase["Pedido"] == pedido_id]
                principal = grupo.iloc[0]
                total_grades = int(sum(total_grades_row(r) for _, r in grupo.iterrows()))
                total_pecas = int(grupo["Qtd"].sum())
                titulo_cliente = str(principal["Cliente"]).strip() or f"Pedido #{pedido_id}"
                resumo = f"{total_pecas} peca(s)"
                anterior = status_anterior(status_chave)
                proximo = proximo_status(status_chave)
                pode_pronto_este_pedido = pode_mover_pronto(status_chave)

                linha_cols = st.columns([4, 1])
                with linha_cols[0]:
                    prazo_txt = prazo_legivel(principal.get("PrazoFinalizacao", ""))
                    responsavel_lancamento = str(principal.get("ResponsavelLancamento", "")).strip() or "-"
                    cliente_info = str(principal.get("Cliente", "-")).strip() or "-"
                    num_info = str(principal.get("NumeroCliente", "-")).strip() or "-"
                    if perfil_atual() == "dashboard":
                        st.markdown(
                            f"**Cliente:** {cliente_info}  |  **Prazo:** {prazo_txt}  |  "
                            f"**Lancado por:** {responsavel_lancamento}  |  **Num:** {num_info}"
                        )
                        obs_info = str(principal.get("Observacao", "")).strip()
                        if obs_info:
                            st.markdown(f"**Observacao:** {obs_info}")
                    else:
                        st.caption(
                            f"Cliente: {cliente_info} | Prazo: {prazo_txt} | "
                            f"Lancado por: {responsavel_lancamento} | Num: {num_info}"
                        )
                    num_cliente = str(principal.get("NumeroCliente", "-")).strip() or "-"
                    titulo_base = f"{titulo_cliente}  |  {resumo}"
                    if perfil_atual() == "dashboard":
                        titulo_base = f"{titulo_cliente} | Num {num_cliente} | Prazo {prazo_txt}"
                    with st.expander(
                        titulo_base,
                        expanded=(perfil_atual() == "dashboard"),
                    ):
                        card = st.container(border=True)
                        card.markdown(f"**Pedido #{pedido_id}**")
                        card.caption(f"Lancado por: {responsavel_lancamento}")
                        card.caption(f"Cliente: {principal['Cliente']}")
                        card.caption(f"Numero: {str(principal.get('NumeroCliente', '-')).strip() or '-'}")
                        observacao_card = str(principal.get("Observacao", "")).strip()
                        if observacao_card:
                            card.caption(f"Observacao: {observacao_card}")
                        card.caption(f"{len(grupo)} modelo(s) | {total_grades} grade(s) | {total_pecas} peca(s)")
                        card.progress(progresso_status(status_chave))
                        card.caption(f"Entrada: {principal['Entrada']}")
                        card.caption(f"Prazo para finalizar: {prazo_legivel(principal.get('PrazoFinalizacao', ''))}")
                        if pedido_vencido(grupo):
                            card.error("Prazo vencido para finalizar este pedido.")

                        card.markdown("**Modelos:**")
                        for _, item in grupo.iterrows():
                            item_id = int(item["ID"])
                            model_cols = card.columns([5, 1])
                            model_cols[0].markdown(f"**{item['Modelo']}**")
                            if model_cols[1].button(
                                "X",
                                key=f"op_del_model_{item_id}",
                                help="Excluir modelo",
                                disabled=not pode_editar,
                            ):
                                deletar_modelo(item_id)
                            for linha_cor in linhas_cores(item):
                                card.caption(linha_cor)
                            card.caption(f"Total: {total_grades_row(item)} grade(s) | {int(item['Qtd'])} peca(s)")

                with linha_cols[1]:
                    if proximo:
                        texto_botao = "Finalizar" if proximo == STATUS_FLUXO[-1] else "Pronto"
                        if st.button(
                            texto_botao,
                            key=f"op_quick_mv_{pedido_id}",
                            use_container_width=True,
                            disabled=not pode_pronto_este_pedido,
                        ):
                            mover_pedido(int(pedido_id), proximo)
                    else:
                        if st.button(
                            "Arquivar",
                            key=f"op_quick_del_{pedido_id}",
                            use_container_width=True,
                            disabled=not pode_editar,
                        ):
                            deletar_pedido(int(pedido_id))

                    edit_open_key = f"op_edit_open_{pedido_id}"
                    add_open_key = f"op_add_open_{pedido_id}"
                    if edit_open_key not in st.session_state:
                        st.session_state[edit_open_key] = False
                    if add_open_key not in st.session_state:
                        st.session_state[add_open_key] = False

                    btn_top_l, btn_top_r = card.columns(2)
                    btn_bot_l, btn_bot_r = card.columns(2)

                    if anterior:
                        if btn_top_l.button(
                            "Voltar",
                            key=f"op_back_{pedido_id}",
                            use_container_width=True,
                            disabled=not pode_editar,
                        ):
                            mover_pedido(int(pedido_id), anterior)
                    else:
                        btn_top_l.write("")

                    if proximo:
                        texto_botao = "Finalizar" if proximo == STATUS_FLUXO[-1] else "Pronto"
                        if btn_top_r.button(
                            texto_botao,
                            key=f"op_mv_{pedido_id}",
                            use_container_width=True,
                            disabled=not pode_pronto_este_pedido,
                        ):
                            mover_pedido(int(pedido_id), proximo)
                    else:
                        if btn_top_r.button(
                            "Arquivar",
                            key=f"op_del_{pedido_id}",
                            use_container_width=True,
                            disabled=not pode_editar,
                        ):
                            deletar_pedido(int(pedido_id))

                    if btn_bot_l.button(
                        "Editar",
                        key=f"op_toggle_edit_{pedido_id}",
                        use_container_width=True,
                        disabled=not pode_editar,
                    ):
                        st.session_state[edit_open_key] = not st.session_state[edit_open_key]
                        st.session_state[add_open_key] = False

                    if btn_bot_r.button(
                        "+ Modelo",
                        key=f"op_toggle_add_{pedido_id}",
                        use_container_width=True,
                        disabled=not pode_editar,
                    ):
                        st.session_state[add_open_key] = not st.session_state[add_open_key]
                        st.session_state[edit_open_key] = False

                    if st.session_state[edit_open_key] and pode_editar:
                        card.markdown("---")
                        card.markdown("**Editar modelo do pedido**")
                        opcoes_itens = [int(x) for x in grupo["ID"].tolist()]
                        mapa_nome = {int(x["ID"]): str(x["Modelo"]) for _, x in grupo.iterrows()}
                        item_id_edit = card.selectbox(
                            "Escolha o modelo",
                            options=opcoes_itens,
                            format_func=lambda x: f"#{x} - {mapa_nome.get(x, '')}",
                            key=f"op_edit_select_{pedido_id}",
                            disabled=not pode_editar,
                        )
                        item_edit = grupo[grupo["ID"] == item_id_edit].iloc[0]

                        with card.form(key=f"op_form_edit_{pedido_id}"):
                            edit_cliente = st.text_input(
                                "Cliente",
                                value=str(item_edit["Cliente"]),
                                key=f"op_edit_cliente_{pedido_id}",
                                disabled=not pode_editar,
                            )
                            edit_numero_cliente = st.text_input(
                                "Numero do cliente",
                                value=str(item_edit.get("NumeroCliente", "")),
                                key=f"op_edit_numero_cliente_{pedido_id}",
                                disabled=not pode_editar,
                            )
                            edit_observacao = st.text_input(
                                "Observacao",
                                value=str(item_edit.get("Observacao", "")),
                                key=f"op_edit_observacao_{pedido_id}",
                                disabled=not pode_editar,
                            )
                            opcoes_modelo_edit = list(MODELOS_DISPONIVEIS)
                            modelo_atual_edit = texto_maiusculo(str(item_edit["Modelo"]))
                            if modelo_atual_edit and modelo_atual_edit not in opcoes_modelo_edit:
                                opcoes_modelo_edit = [modelo_atual_edit] + opcoes_modelo_edit
                            edit_modelo = st.selectbox(
                                "Modelo/Grade",
                                options=opcoes_modelo_edit,
                                index=opcoes_modelo_edit.index(modelo_atual_edit) if modelo_atual_edit in opcoes_modelo_edit else 0,
                                key=f"op_edit_modelo_{pedido_id}",
                                disabled=not pode_editar,
                            )
                            edit_cores = {}
                            cols_edit = st.columns(2)
                            for i, (nome_cor, coluna_cor) in enumerate(zip(CORES, COLUNAS_CORES)):
                                edit_cores[coluna_cor] = cols_edit[i % 2].number_input(
                                    nome_cor,
                                    min_value=0,
                                    value=int(item_edit[coluna_cor]),
                                    step=1,
                                    key=f"op_edit_{coluna_cor}_{pedido_id}_{item_id_edit}",
                                    disabled=not pode_editar,
                                )

                            total_edit_grades = int(sum(edit_cores.values()))
                            st.caption(f"Novo total: {total_edit_grades} grade(s) = {total_edit_grades * PECAS_POR_GRADE} peca(s)")
                            salvar_edicao = st.form_submit_button("Salvar alteracoes", disabled=not pode_editar)
                            if salvar_edicao:
                                if not edit_cliente.strip() or not edit_modelo.strip():
                                    st.warning("Cliente e modelo sao obrigatorios.")
                                elif total_edit_grades <= 0:
                                    st.warning("Informe ao menos 1 grade para salvar.")
                                else:
                                    atualizar_modelo(
                                        item_id_edit,
                                        edit_cliente,
                                        edit_numero_cliente,
                                        edit_observacao,
                                        edit_modelo,
                                        edit_cores,
                                    )

                    if st.session_state[add_open_key] and pode_editar:
                        card.markdown("---")
                        card.markdown("**Acrescentar modelo neste pedido**")
                        with card.form(key=f"op_form_add_model_{pedido_id}"):
                            add_modelo = st.selectbox(
                                "Novo modelo/grade",
                                options=MODELOS_DISPONIVEIS,
                                key=f"op_add_modelo_{pedido_id}",
                                disabled=not pode_editar,
                            )
                            add_cores = {}
                            cols_add = st.columns(2)
                            for i, (nome_cor, coluna_cor) in enumerate(zip(CORES, COLUNAS_CORES)):
                                add_cores[coluna_cor] = cols_add[i % 2].number_input(
                                    nome_cor,
                                    min_value=0,
                                    value=0,
                                    step=1,
                                    key=f"op_add_{coluna_cor}_{pedido_id}",
                                    disabled=not pode_editar,
                                )

                            total_add_grades = int(sum(add_cores.values()))
                            st.caption(f"Novo modelo: {total_add_grades} grade(s) = {total_add_grades * PECAS_POR_GRADE} peca(s)")
                            add_submit = st.form_submit_button("Adicionar modelo", disabled=not pode_editar)
                            if add_submit:
                                if not add_modelo.strip():
                                    st.warning("Informe o nome do novo modelo.")
                                elif total_add_grades <= 0:
                                    st.warning("Informe ao menos 1 grade para o novo modelo.")
                                else:
                                    adicionar_modelo_ao_pedido(
                                        pedido_id=int(pedido_id),
                                        cliente=str(principal["Cliente"]),
                                        numero_cliente=str(principal.get("NumeroCliente", "-")),
                                        observacao=str(principal.get("Observacao", "")),
                                        status=str(principal["Status"]),
                                        entrada=str(principal["Entrada"]),
                                        prazo_finalizacao=str(principal.get("PrazoFinalizacao", gerar_prazo_padrao())),
                                        responsavel_lancamento=str(principal.get("ResponsavelLancamento", "")).strip()
                                        or str(st.session_state.get("usuario", "")),
                                        modelo=add_modelo,
                                        cores=add_cores,
                                    )

CONFIG_SISTEMA = carregar_config_sistema()

aplicar_estilo()
inicializar_sessao_acesso()
st.markdown(
    """
    <div class="hero-box">
      <p class="hero-title">Centro de Operacoes da Producao</p>
      <p class="hero-sub">Fluxo por etapa, rastreabilidade de pedidos e relatorios de desempenho.</p>
    </div>
    """,
    unsafe_allow_html=True,
)
monitorar_alteracoes()
render_login_sidebar()
pode_lancar = pode_lancar_pedido()
pode_editar = pode_editar_completo()
somente_visual = usuario_somente_visualizacao()

if usuario_master():
    try:
        caminho_app = os.path.abspath(__file__)
        atualizado_em = datetime.fromtimestamp(os.path.getmtime(__file__)).strftime("%d/%m/%Y %H:%M:%S")
        st.caption(f"Arquivo em execucao: {caminho_app}")
        st.caption(f"Atualizado em: {atualizado_em}")
    except Exception:
        pass

st.markdown(
    "<div class='access-pill'>Acesso atual: "
    + (
        "PERFIL DASHBOARD (somente painel)."
        if perfil_atual() == "dashboard"
        else (
            "SOMENTE VISUALIZACAO (dashboard e relatorios)."
            if somente_visual
            else ("EDITOR (controle total)." if pode_editar else "CONSULTA (pode lancar e separar; sem edicao apos Pronto).")
        )
    )
    + "</div>",
    unsafe_allow_html=True,
)

if perfil_atual() == "dashboard":
    aplicar_estilo_dashboard()

with st.sidebar:
    if somente_visual:
        st.header("Visualizacao")
        if perfil_atual() == "dashboard":
            st.caption("Este usuario possui acesso somente ao Dashboard.")
        else:
            st.caption("Este usuario possui acesso somente para visualizar pedidos e relatorios.")
        cliente = ""
        numero_cliente = ""
        observacao_pedido = ""
        qtd_modelos = 1
    else:
        st.header("Novo pedido")
        if not pode_lancar:
            st.info("Sem permissao para lancar pedidos.")
        cliente = st.text_input("Nome do cliente", disabled=not pode_lancar)
        numero_cliente = st.text_input("Numero do cliente", disabled=not pode_lancar)
        with st.expander("Observacao (opcional)", expanded=False):
            observacao_pedido = st.text_area(
                "Observacao",
                height=70,
                key="novo_pedido_observacao",
                disabled=not pode_lancar,
            )
        qtd_modelos = st.number_input(
            "Quantidade de modelos no lancamento",
            min_value=1,
            max_value=8,
            value=1,
            step=1,
            disabled=not pode_lancar,
        )

    modelos_validos = []
    for idx in range(int(qtd_modelos)):
        if somente_visual:
            break
        st.markdown(f"**Modelo {idx + 1}**")
        modelo = st.selectbox(
            "Nome do modelo/grade",
            options=MODELOS_DISPONIVEIS,
            key=f"modelo_{idx}",
            disabled=not pode_lancar,
        )
        with st.expander(f"Cores do modelo {idx + 1}", expanded=(idx == 0)):
            entradas_cores = {}
            cols = st.columns(2)
            for i, (nome_cor, coluna_cor) in enumerate(zip(CORES, COLUNAS_CORES)):
                entradas_cores[coluna_cor] = cols[i % 2].number_input(
                    nome_cor,
                    min_value=0,
                    value=0,
                    step=1,
                    key=f"{coluna_cor}_{idx}",
                    disabled=not pode_lancar,
                )

        grades = int(sum(entradas_cores.values()))
        pecas = grades * PECAS_POR_GRADE
        st.caption(f"Modelo {idx + 1}: {grades} grade(s) = {pecas} peca(s)")

        if str(modelo).strip() and grades > 0:
            modelos_validos.append(
                {
                    "Modelo": str(modelo).strip(),
                    "Grades": {k: int(v) for k, v in entradas_cores.items()},
                    "Qtd": pecas,
                    "GradesTotal": grades,
                }
            )

    if not somente_visual and st.button("Lancar pedido", disabled=not pode_lancar):
        if not str(cliente).strip():
            st.warning("Preencha o nome do cliente.")
        elif not modelos_validos:
            st.warning("Informe ao menos 1 modelo com grades > 0.")
        else:
            cliente_fmt = texto_maiusculo(cliente)
            numero_cliente_fmt = normalizar_numero_cliente(numero_cliente)
            observacao_fmt = str(observacao_pedido).strip()
            df = carregar_dados()
            novo_id = int(df["ID"].max() + 1) if not df.empty else 1
            novo_pedido = int(df["Pedido"].max() + 1) if not df.empty else 1
            entrada = datetime.now().strftime("%d/%m %H:%M")
            prazo_finalizacao = gerar_prazo_padrao()

            novos = []
            qtd_total = 0
            grades_total = 0
            for item in modelos_validos:
                modelo_fmt = texto_maiusculo(item["Modelo"])
                novos.append(
                    {
                        "ID": novo_id,
                        "Pedido": novo_pedido,
                        "Cliente": cliente_fmt,
                        "NumeroCliente": numero_cliente_fmt,
                        "Observacao": observacao_fmt,
                        "Modelo": modelo_fmt,
                        "Qtd": item["Qtd"],
                        "Status": STATUS_FLUXO[0],
                        "Entrada": entrada,
                        "PrazoFinalizacao": prazo_finalizacao,
                        "ResponsavelLancamento": str(st.session_state.get("usuario", "")),
                        **item["Grades"],
                    }
                )
                registrar_evento(
                    acao="LANCAR_MODELO",
                    pedido=novo_pedido,
                    modeloid=novo_id,
                    cliente=cliente_fmt,
                    modelo=modelo_fmt,
                    status_depois=STATUS_FLUXO[0],
                    grades=item["GradesTotal"],
                    qtd=item["Qtd"],
                    detalhes="Modelo lancado no pedido.",
                )
                qtd_total += int(item["Qtd"])
                grades_total += int(item["GradesTotal"])
                novo_id += 1

            registrar_evento(
                acao="LANCAR_PEDIDO",
                pedido=novo_pedido,
                cliente=cliente_fmt,
                status_depois=STATUS_FLUXO[0],
                grades=grades_total,
                qtd=qtd_total,
                detalhes=f"Pedido lancado com {len(novos)} modelo(s).",
            )

            df = pd.concat([df, pd.DataFrame(novos)], ignore_index=True)
            salvar_dados(df)
            st.success(f"Pedido #{novo_pedido} salvo com {len(novos)} modelo(s).")
            st.rerun()

df_atual = carregar_dados()
eventos = carregar_eventos()

if perfil_atual() == "dashboard":
    abas_nomes = ["Dashboard"]
elif perfil_atual() == "estoque":
    abas_nomes = ["Operacao"]
elif somente_visual:
    abas_nomes = ["Dashboard", "Relatorios"]
else:
    abas_nomes = ["Operacao", "Dashboard", "Relatorios"]
if usuario_master():
    abas_nomes.append("Administracao")
abas = st.tabs(abas_nomes)
mapa_abas = {nome: idx for idx, nome in enumerate(abas_nomes)}

if "Dashboard" in mapa_abas:
    with abas[mapa_abas["Dashboard"]]:
        if perfil_atual() == "dashboard":
            dashboard_live()
        else:
            render_dashboard_visual(df_atual, eventos)

if "Operacao" in mapa_abas:
    with abas[mapa_abas["Operacao"]]:
        render_operacao(df_atual, eventos)

if "Relatorios" in mapa_abas:
    with abas[mapa_abas["Relatorios"]]:
        st.subheader("Janela de relatorios")
        abas_rel = st.tabs(["Producao", "Futuro 1", "Futuro 2"])

        with abas_rel[0]:
            if eventos.empty:
                st.info("Ainda nao existem eventos para relatorio.")
            else:
                if firebase_store.is_enabled():
                    st.caption(f"Modo performance ativo: exibindo ate {LIMITE_EVENTOS_FIREBASE} eventos mais recentes.")
                d1 = pd.to_datetime(eventos["Data"], errors="coerce")
                d1_fallback = pd.to_datetime(eventos["DataHora"], errors="coerce")
                d_ref = d1.where(~d1.isna(), d1_fallback)
                min_data = d_ref.min().date() if not d_ref.isna().all() else datetime.now().date()
                max_data = d_ref.max().date() if not d_ref.isna().all() else datetime.now().date()
                colf1, colf2 = st.columns(2)
                data_ini = colf1.date_input("Data inicial", value=min_data, min_value=min_data, max_value=max_data)
                data_fim = colf2.date_input("Data final", value=max_data, min_value=min_data, max_value=max_data)
                if data_fim < data_ini:
                    st.warning("Data final menor que data inicial. Ajuste os filtros.")
                else:
                    data_mask_ref = d_ref.dt.date
                    mask = (data_mask_ref >= data_ini) & (data_mask_ref <= data_fim)
                    ev = eventos[mask].copy()

                c1, c2, c3, c4 = st.columns(4)
                c1.metric("Pedidos lancados", int((ev["Acao"] == "LANCAR_PEDIDO").sum()))
                c2.metric("Modelos lancados", int((ev["Acao"] == "LANCAR_MODELO").sum()))
                c3.metric("Pecas finalizadas", int(ev[(ev["Acao"] == "MOVER_STATUS") & (ev["StatusDepois"] == "4. Finalizado")]["Qtd"].sum()))
                c4.metric("Exclusoes", int(((ev["Acao"] == "EXCLUIR_MODELO") | (ev["Acao"] == "ARQUIVAR_PEDIDO")).sum()))
                st.caption(f"Pedidos finalizados no periodo: {int(eventos_de_finalizacao(ev)['Pedido'].nunique())}")

                st.markdown("**Resumo inteligente da producao**")
                st.markdown(resumo_relatorio_producao(df_atual, ev))

                st.markdown("**Pedidos finalizados (com data de lancamento e finalizacao)**")
                tabela_finalizados = tabela_pedidos_finalizados(ev)
                if tabela_finalizados.empty:
                    st.caption("Nenhum pedido finalizado no periodo selecionado.")
                else:
                    st.dataframe(tabela_finalizados, use_container_width=True, hide_index=True)

                st.markdown("**Pedidos arquivados (clique no cliente para ver detalhes)**")
                ev_arquivados = ev[ev["Acao"] == "ARQUIVAR_PEDIDO"].copy()
                if ev_arquivados.empty:
                    st.caption("Nenhum pedido arquivado no periodo selecionado.")
                else:
                    ev_arquivados = ev_arquivados.sort_values(by="DataHora", ascending=False, kind="stable")
                    for _, arq in ev_arquivados.iterrows():
                        pedido_arq = int(arq["Pedido"])
                        cliente_arq = str(arq["Cliente"]).strip() or "-"
                        data_arq = str(arq["DataHora"])[:16].replace("-", "/")
                        qtd_arq = int(arq["Qtd"]) if str(arq["Qtd"]).strip() else 0
                        grades_arq = int(arq["Grades"]) if str(arq["Grades"]).strip() else 0
                        titulo = f"{cliente_arq} | Pedido #{pedido_arq} | {qtd_arq} peca(s)"
                        with st.expander(titulo, expanded=False):
                            st.caption(f"Arquivado em: {data_arq}")
                            st.caption(f"Quantidade total: {qtd_arq} peca(s) | {grades_arq} grade(s)")
                            st.caption(f"Status anterior: {str(arq.get('StatusAntes', '-'))}")
                            detalhes = str(arq.get("Detalhes", "")).strip()
                            if detalhes:
                                st.caption(f"Observacao: {detalhes}")

                            hist_pedido = ev[
                                (ev["Pedido"] == pedido_arq)
                                & (ev["Acao"].isin(["LANCAR_MODELO", "ADICIONAR_MODELO", "EDITAR_MODELO", "EXCLUIR_MODELO"]))
                            ][["DataHora", "Modelo", "Acao", "Grades", "Qtd", "Detalhes"]].copy()
                            if hist_pedido.empty:
                                st.caption("Sem historico de modelos disponivel para este pedido no periodo filtrado.")
                            else:
                                st.dataframe(
                                    hist_pedido.sort_values(by="DataHora", ascending=False),
                                    use_container_width=True,
                                    hide_index=True,
                                )


        with abas_rel[1]:
            st.info("Espaco reservado para futuros relatorios.")

        with abas_rel[2]:
            st.info("Espaco reservado para futuros relatorios.")

if usuario_master() and "Administracao" in mapa_abas:
    with abas[mapa_abas["Administracao"]]:
        st.subheader("Administracao do sistema (somente lucas_ti)")
        bloco1, bloco2, bloco3 = st.tabs(["Usuarios", "Base de dados", "Configuracoes"])

        with bloco1:
            users_df = carregar_usuarios()
            st.markdown("**Usuarios atuais**")
            vis = users_df.copy()
            vis["SenhaHash"] = "********"
            st.dataframe(vis[["Usuario", "Perfil", "Ativo", "SenhaHash"]], use_container_width=True, hide_index=True)

            st.markdown("**Editar usuario existente**")
            if users_df.empty:
                st.caption("Sem usuarios cadastrados.")
            else:
                usuario_sel = st.selectbox("Usuario", users_df["Usuario"].tolist(), key="adm_usuario_sel")
                user_row = users_df[users_df["Usuario"] == usuario_sel].iloc[0]
                novo_nome_usuario = st.text_input(
                    "Nome do usuario",
                    value=str(usuario_sel),
                    disabled=(usuario_sel == "lucas_ti"),
                    key="adm_usuario_novo_nome",
                )
                perfil_sel = st.selectbox(
                    "Perfil",
                    options=["editor", "visualizador", "dashboard"],
                    index=["editor", "visualizador", "dashboard"].index(str(user_row["Perfil"]))
                    if str(user_row["Perfil"]) in ["editor", "visualizador", "dashboard"]
                    else 1,
                    key="adm_perfil_sel",
                )
                ativo_sel = st.checkbox("Ativo", value=bool(int(user_row["Ativo"])), key="adm_ativo_sel")
                st.caption("Senha atual nao pode ser exibida (armazenada com hash).")
                ver_nova_senha = st.checkbox("Visualizar nova senha digitada", key="adm_ver_nova_senha")
                nova_senha = st.text_input(
                    "Nova senha (opcional)",
                    type="default" if ver_nova_senha else "password",
                    key="adm_nova_senha",
                )

                c_upd, c_del = st.columns(2)
                if c_upd.button("Salvar usuario", use_container_width=True):
                    usuario_novo = str(novo_nome_usuario).strip().lower()
                    if not usuario_novo:
                        st.warning("Informe um nome de usuario valido.")
                    elif usuario_novo != usuario_sel and (users_df["Usuario"] == usuario_novo).any():
                        st.warning("Esse nome de usuario ja existe.")
                    else:
                        idx = users_df.index[users_df["Usuario"] == usuario_sel][0]
                        users_df.loc[idx, "Usuario"] = usuario_novo
                        users_df.loc[idx, "Perfil"] = perfil_sel
                        users_df.loc[idx, "Ativo"] = 1 if ativo_sel else 0
                        if nova_senha.strip():
                            users_df.loc[idx, "SenhaHash"] = hash_senha(nova_senha.strip())
                        salvar_usuarios(users_df)
                        st.success(f"Usuario {usuario_sel} atualizado para {usuario_novo}.")
                        st.rerun()

                if c_del.button("Excluir usuario", use_container_width=True, disabled=(usuario_sel == "lucas_ti")):
                    users_df = users_df[users_df["Usuario"] != usuario_sel]
                    salvar_usuarios(users_df)
                    st.success(f"Usuario {usuario_sel} excluido.")
                    st.rerun()

            st.markdown("**Adicionar novo usuario**")
            with st.form("adm_add_user"):
                novo_usuario = st.text_input("Usuario novo")
                ver_senha_add = st.checkbox("Visualizar senha digitada", key="adm_ver_senha_add")
                nova_senha_add = st.text_input("Senha", type="default" if ver_senha_add else "password")
                novo_perfil = st.selectbox("Perfil do novo usuario", ["visualizador", "dashboard", "editor"])
                novo_ativo = st.checkbox("Ativo", value=True)
                add_user_submit = st.form_submit_button("Adicionar usuario")
                if add_user_submit:
                    u = str(novo_usuario).strip().lower()
                    if not u or not nova_senha_add.strip():
                        st.warning("Informe usuario e senha.")
                    else:
                        users_df = carregar_usuarios()
                        if (users_df["Usuario"] == u).any():
                            st.warning("Usuario ja existe.")
                        else:
                            novo = pd.DataFrame(
                                [{
                                    "Usuario": u,
                                    "SenhaHash": hash_senha(nova_senha_add.strip()),
                                    "Perfil": novo_perfil,
                                    "Ativo": 1 if novo_ativo else 0,
                                }]
                            )
                            users_df = pd.concat([users_df, novo], ignore_index=True)
                            salvar_usuarios(users_df)
                            st.success(f"Usuario {u} criado.")
                            st.rerun()

        with bloco2:
            st.markdown("**Editar pedidos (base principal)**")
            st.caption("Voce pode editar as colunas de cores (Cor_*). Ao salvar, a regra da grade sera aplicada automaticamente: Qtd = soma das cores x 6.")
            base_edit = st.data_editor(
                df_atual,
                use_container_width=True,
                hide_index=True,
                num_rows="dynamic",
                key="adm_editor_pedidos",
            )
            if st.button("Salvar alteracoes dos pedidos", use_container_width=True):
                df_save = base_edit.copy()
                for col in COLUNAS_BASE + COLUNAS_CORES:
                    if col not in df_save.columns:
                        df_save[col] = 0 if col.startswith("Cor_") else ""
                df_save["ID"] = pd.to_numeric(df_save["ID"], errors="coerce").fillna(0).astype(int)
                df_save["Pedido"] = pd.to_numeric(df_save["Pedido"], errors="coerce").fillna(0).astype(int)
                for col in COLUNAS_CORES:
                    df_save[col] = pd.to_numeric(df_save[col], errors="coerce").fillna(0).astype(int).clip(lower=0)
                # Regra de grade aplicada no painel administrativo (lucas_ti):
                # 1 grade = 6 pecas. Qtd e recalculada a partir das cores.
                df_save["Qtd"] = df_save[COLUNAS_CORES].sum(axis=1).astype(int) * PECAS_POR_GRADE
                salvar_dados(df_save[COLUNAS_BASE + COLUNAS_CORES])
                st.success("Pedidos atualizados com sucesso.")
                st.rerun()

            st.markdown("**Excluir pedido selecionado**")
            pedidos_disponiveis = sorted([int(p) for p in df_atual["Pedido"].dropna().unique().tolist()]) if not df_atual.empty else []
            if not pedidos_disponiveis:
                st.caption("Nenhum pedido disponivel para excluir.")
            else:
                csel, cdel = st.columns([3, 1])
                pedido_excluir = csel.selectbox("Pedido", pedidos_disponiveis, key="adm_pedido_excluir")
                if cdel.button("Excluir pedido", use_container_width=True):
                    df_novo = df_atual[df_atual["Pedido"] != int(pedido_excluir)].copy()
                    salvar_dados(df_novo[COLUNAS_BASE + COLUNAS_CORES] if not df_novo.empty else pd.DataFrame(columns=COLUNAS_BASE + COLUNAS_CORES))
                    st.success(f"Pedido #{int(pedido_excluir)} excluido da base.")
                    st.rerun()

            st.markdown("**Editar eventos**")
            eventos_edit = st.data_editor(
                eventos,
                use_container_width=True,
                hide_index=True,
                num_rows="dynamic",
                key="adm_editor_eventos",
            )
            if st.button("Salvar alteracoes dos eventos", use_container_width=True):
                ev_save = eventos_edit.copy()
                for col in COLUNAS_EVENTOS:
                    if col not in ev_save.columns:
                        ev_save[col] = ""
                ev_save["Pedido"] = pd.to_numeric(ev_save["Pedido"], errors="coerce").fillna(0).astype(int)
                ev_save["ModeloID"] = pd.to_numeric(ev_save["ModeloID"], errors="coerce").fillna(0).astype(int)
                ev_save["Grades"] = pd.to_numeric(ev_save["Grades"], errors="coerce").fillna(0).astype(int)
                ev_save["Qtd"] = pd.to_numeric(ev_save["Qtd"], errors="coerce").fillna(0).astype(int)
                if firebase_store.is_enabled():
                    firebase_store.save_collection_df(COLECAO_EVENTOS, ev_save[COLUNAS_EVENTOS])
                else:
                    ev_save[COLUNAS_EVENTOS].to_csv(ARQUIVO_EVENTOS, index=False)
                st.success("Eventos atualizados com sucesso.")
                st.rerun()

            st.markdown("**Acoes rapidas**")
            b1, b2 = st.columns(2)
            if b1.button("Limpar todos os pedidos", use_container_width=True):
                vazio = pd.DataFrame(columns=COLUNAS_BASE + COLUNAS_CORES)
                salvar_dados(vazio)
                st.success("Todos os pedidos foram removidos.")
                st.rerun()
            if b2.button("Limpar todos os eventos", use_container_width=True):
                vazio_ev = pd.DataFrame(columns=COLUNAS_EVENTOS)
                if firebase_store.is_enabled():
                    firebase_store.save_collection_df(COLECAO_EVENTOS, vazio_ev)
                else:
                    vazio_ev.to_csv(ARQUIVO_EVENTOS, index=False)
                st.success("Todos os eventos foram removidos.")
                st.rerun()

        with bloco3:
            st.markdown("**Regra da grade, cores e modelos do sistema**")
            st.caption("Essas configuracoes afetam o lancamento/edicao de modelos no sistema inteiro.")

            users_cfg = carregar_usuarios()
            usuarios_disponiveis = sorted(users_cfg["Usuario"].astype(str).str.strip().str.lower().unique().tolist())
            vis_atuais = [u for u in CONFIG_SISTEMA.get("usuarios_somente_visualizacao", []) if u in usuarios_disponiveis]
            usuarios_somente_visual = st.multiselect(
                "Usuarios com acesso somente visualizacao",
                options=usuarios_disponiveis,
                default=vis_atuais,
                help="Esses usuarios poderao apenas visualizar dashboard e relatorios.",
                key="adm_usuarios_somente_visual",
            )
            pecas_admin = st.number_input(
                "Pecas por grade",
                min_value=1,
                max_value=200,
                value=int(PECAS_POR_GRADE),
                step=1,
                key="adm_pecas_por_grade",
            )
            cores_texto_padrao = "\n".join(CORES)
            cores_admin_txt = st.text_area(
                "Cores (uma por linha)",
                value=cores_texto_padrao,
                height=220,
                key="adm_cores_texto",
            )
            st.caption("Exemplo: Preto, Cinza, Azul Marinho. Evite nomes repetidos.")

            modelos_texto_padrao = "\n".join(MODELOS_DISPONIVEIS)
            modelos_admin_txt = st.text_area(
                "Modelos (um por linha)",
                value=modelos_texto_padrao,
                height=180,
                key="adm_modelos_texto",
            )
            st.caption("Exemplo: NIKE CLUB, LACOSTE, BROOKS, MERCEDES, BMW.")

            if st.button("Salvar configuracoes de grade, cores e modelos", use_container_width=True):
                linhas = [normalizar_nome_cor(x) for x in str(cores_admin_txt).splitlines()]
                novas_cores = []
                seen = set()
                for c in linhas:
                    if not c:
                        continue
                    k = c.lower()
                    if k in seen:
                        continue
                    seen.add(k)
                    novas_cores.append(c)
                modelos_linhas = [texto_maiusculo(x) for x in str(modelos_admin_txt).splitlines() if str(x).strip()]
                novos_modelos = []
                seen_modelos = set()
                for m in modelos_linhas:
                    if m in seen_modelos:
                        continue
                    seen_modelos.add(m)
                    novos_modelos.append(m)

                if not novas_cores:
                    st.warning("Informe pelo menos uma cor valida.")
                elif not novos_modelos:
                    st.warning("Informe pelo menos um modelo valido.")
                else:
                    vis_final = [u for u in usuarios_somente_visual if str(u).strip().lower() != "lucas_ti"]
                    salvar_config_sistema(
                        int(pecas_admin),
                        novas_cores,
                        modelos_lista=novos_modelos,
                        usuarios_somente_visualizacao=vis_final,
                    )
                    st.success("Configuracoes salvas. A tela sera atualizada.")
                    st.rerun()
