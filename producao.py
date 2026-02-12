import os
import hashlib
from datetime import datetime, timedelta

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components
import firebase_store

st.set_page_config(layout="wide", page_title="Controle de Producao")

ARQUIVO_DADOS = "producao.csv"
ARQUIVO_EVENTOS = "producao_eventos.csv"
ARQUIVO_USUARIOS = "usuarios.csv"
COLECAO_DADOS = "producao_dados"
COLECAO_EVENTOS = "producao_eventos"
COLECAO_USUARIOS = "producao_usuarios"
PECAS_POR_GRADE = 6
STATUS_FLUXO = ["1. Corte", "2. Costura", "3. Acabamento", "4. Finalizado"]
CORES = ["Preto", "Cinza", "Azul", "Azul Marinho", "Azul BB", "Azul Royal", "Vermelho", "Branco"]
COLUNAS_CORES = [f"Cor_{c.replace(' ', '_')}" for c in CORES]
COLUNAS_BASE = ["ID", "Pedido", "Cliente", "Modelo", "Qtd", "Status", "Entrada", "PrazoFinalizacao", "ResponsavelLancamento"]
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


def aplicar_estilo():
    st.markdown(
        """
        <style>
          @import url('https://fonts.googleapis.com/css2?family=Manrope:wght@500;700;800&display=swap');

          .stApp {
            font-family: "Manrope", sans-serif;
            background: radial-gradient(circle at 0% 0%, #edf3ff 0%, #f7f9fc 42%, #f2f5fb 100%);
            color: #172133;
          }

          [data-testid="stSidebar"] {
            background: linear-gradient(180deg, #0f1c2f 0%, #15253d 100%);
            border-right: 1px solid rgba(255,255,255,0.08);
          }

          [data-testid="stSidebar"] * {
            color: #f2f6ff !important;
          }

          [data-testid="stSidebar"] [data-testid="stForm"] {
            background: rgba(255, 255, 255, 0.10) !important;
            border: 1px solid rgba(255, 255, 255, 0.28) !important;
            border-radius: 12px !important;
            padding: 12px !important;
          }

          [data-testid="stSidebar"] [data-testid="stForm"] label,
          [data-testid="stSidebar"] [data-testid="stForm"] p,
          [data-testid="stSidebar"] [data-testid="stForm"] span {
            color: #eef4ff !important;
            font-weight: 700 !important;
          }

          /* Botao Entrar com contraste alto na sidebar */
          [data-testid="stSidebar"] [data-testid="stForm"] [data-testid="stFormSubmitButton"] button {
            background: #1d56a8 !important;
            color: #ffffff !important;
            border: 1px solid #154584 !important;
            font-weight: 700 !important;
          }

          [data-testid="stSidebar"] [data-testid="stForm"] [data-testid="stFormSubmitButton"] button:hover {
            background: #17488d !important;
            color: #ffffff !important;
            border-color: #10386f !important;
          }

          [data-testid="stSidebar"] [data-testid="stForm"] [data-testid="stFormSubmitButton"] button:disabled {
            background: #5e6f8a !important;
            color: #eaf1ff !important;
            border-color: #4f607a !important;
            opacity: 1 !important;
          }

          [data-testid="stSidebar"] .stExpander {
            border: 1px solid rgba(255, 255, 255, 0.12);
            border-radius: 10px;
            background: rgba(255, 255, 255, 0.04);
          }

          [data-testid="stSidebar"] .stExpander details summary {
            background: rgba(148, 173, 214, 0.24);
            border-radius: 9px 9px 0 0;
          }

          [data-testid="stSidebar"] [data-baseweb="input"] {
            background: #edf2fb !important;
            border: 1px solid #bfd0ea !important;
            border-radius: 10px !important;
          }

          [data-testid="stSidebar"] [data-baseweb="input"] input {
            color: #10233f !important;
            caret-color: #10233f !important;
            font-weight: 600 !important;
            -webkit-text-fill-color: #10233f !important;
          }

          [data-testid="stSidebar"] [data-baseweb="input"] input::placeholder {
            color: #5f7396 !important;
            opacity: 1 !important;
          }

          /* Icone de mostrar senha (olho) mais visivel */
          [data-testid="stSidebar"] [data-baseweb="input"] [data-baseweb="button"] {
            background: #dbe6f8 !important;
            border-left: 1px solid #bfd0ea !important;
            color: #17488d !important;
          }

          [data-testid="stSidebar"] [data-baseweb="input"] [data-baseweb="button"] svg {
            fill: #17488d !important;
          }

          [data-testid="stSidebar"] [data-testid="stNumberInput"] button {
            color: #ffffff !important;
            background: #1d56a8 !important;
            border: 1px solid #154584 !important;
          }

          [data-testid="stSidebar"] [data-testid="stNumberInput"] button:hover {
            background: #17488d !important;
          }

          [data-testid="stSidebar"] [data-testid="stCaptionContainer"] {
            color: #dfe8f8 !important;
          }

          .hero-box {
            border: 1px solid #dbe4f3;
            background: linear-gradient(135deg, #ffffff 0%, #eef4ff 100%);
            border-radius: 16px;
            padding: 14px 16px;
            margin-bottom: 8px;
          }

          .hero-title {
            margin: 0;
            font-size: 1.25rem;
            font-weight: 800;
            color: #12213a;
          }

          .hero-sub {
            margin: 4px 0 0 0;
            color: #4d5f7d;
            font-size: 0.92rem;
            font-weight: 500;
          }

          .kpi-card {
            border: 1px solid #d8e0ef;
            border-radius: 14px;
            padding: 10px 12px;
            background: #ffffff;
          }

          .kpi-label {
            font-size: 12px;
            color: #5d6f8c;
            font-weight: 700;
            margin: 0;
          }

          .kpi-value {
            font-size: 22px;
            color: #13223b;
            font-weight: 800;
            margin: 0;
          }

          [data-testid="stVerticalBlock"] [data-testid="stMarkdownContainer"] h3 {
            color: #172a45;
            font-weight: 800;
          }

          [data-testid="stButton"] button {
            border-radius: 10px;
            border: 1px solid #143f80;
            font-weight: 700;
            background: #184e9e;
            color: #ffffff;
          }

          [data-testid="stButton"] button:hover {
            background: #123f7f;
            color: #ffffff;
            border-color: #0e3367;
          }

          [data-testid="stButton"] button:active {
            background: #0e3367;
            color: #ffffff;
            border-color: #0b2a55;
          }

          [data-testid="stButton"] button:disabled {
            background: #9aa9c0;
            color: #f5f7fb;
            border-color: #8d9ab0;
          }

          [data-testid="stForm"] {
            border: 1px solid #e2e8f5;
            border-radius: 12px;
            padding: 10px;
            background: #f9fbff;
          }

          [data-testid="stMetricValue"] {
            color: #14284a;
            font-weight: 800;
          }
        </style>
        """,
        unsafe_allow_html=True,
    )


def hash_senha(senha):
    return hashlib.sha256(str(senha).encode("utf-8")).hexdigest()


def carregar_usuarios():
    if firebase_store.is_enabled():
        df = firebase_store.load_collection_df(COLECAO_USUARIOS, COLUNAS_USUARIOS)
    else:
        if not os.path.exists(ARQUIVO_USUARIOS):
            base = pd.DataFrame(
                [
                    {"Usuario": "admin", "SenhaHash": hash_senha("admin123"), "Perfil": "editor", "Ativo": 1},
                    {"Usuario": "consulta", "SenhaHash": hash_senha("consulta123"), "Perfil": "visualizador", "Ativo": 1},
                ],
                columns=COLUNAS_USUARIOS,
            )
            base.to_csv(ARQUIVO_USUARIOS, index=False)
            return base
        df = pd.read_csv(ARQUIVO_USUARIOS)

    if df.empty:
        df = pd.DataFrame(
            [
                {"Usuario": "admin", "SenhaHash": hash_senha("admin123"), "Perfil": "editor", "Ativo": 1},
                {"Usuario": "consulta", "SenhaHash": hash_senha("consulta123"), "Perfil": "visualizador", "Ativo": 1},
            ],
            columns=COLUNAS_USUARIOS,
        )
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


def autenticar(usuario, senha):
    df = carregar_usuarios()
    u = str(usuario).strip().lower()
    s_hash = hash_senha(senha)
    hit = df[(df["Usuario"] == u) & (df["SenhaHash"] == s_hash) & (df["Ativo"] == 1)]
    if hit.empty:
        return None
    perfil = str(hit.iloc[0]["Perfil"]).lower()
    if perfil not in ["editor", "visualizador"]:
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
            st.caption("Usuarios ativos: lucas, marcos, vitor, lucas_ti")
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
        if perfil == "editor":
            resumo = "Acesso total: lancar, separar, editar, mover, arquivar."
        else:
            resumo = "Acesso consulta: lancar pedido e separar (Pronto em Separar pedido)."
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
    return perfil_atual() in ["editor", "visualizador"]


def pode_editar_completo():
    return perfil_atual() == "editor"


def pode_mover_pronto(status_atual):
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
        # Fallback para manter sincronismo entre abas/sessoes sem F5 manual.
        components.html(
            """
            <script>
              setTimeout(function() {
                window.parent.location.reload();
              }, 12000);
            </script>
            """,
            height=0,
            width=0,
        )
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


def atualizar_modelo(id_modelo, cliente, modelo, cores):
    df = carregar_dados()
    linha = df[df["ID"] == id_modelo]
    if linha.empty:
        return
    antigo = linha.iloc[0]
    pedido_id = int(antigo["Pedido"])
    total_grades = int(sum(int(v) for v in cores.values()))
    qtd = total_grades * PECAS_POR_GRADE

    df.loc[df["ID"] == id_modelo, "Cliente"] = cliente.strip()
    df.loc[df["ID"] == id_modelo, "Modelo"] = modelo.strip()
    df.loc[df["ID"] == id_modelo, "Qtd"] = qtd
    for coluna, valor in cores.items():
        df.loc[df["ID"] == id_modelo, coluna] = int(valor)
    salvar_dados(df)

    registrar_evento(
        acao="EDITAR_MODELO",
        pedido=pedido_id,
        modeloid=id_modelo,
        cliente=cliente.strip(),
        modelo=modelo.strip(),
        status_antes=str(antigo["Status"]),
        status_depois=str(antigo["Status"]),
        grades=total_grades,
        qtd=qtd,
        detalhes=f"Modelo editado. Antes: '{antigo['Modelo']}'.",
    )
    st.rerun()


def adicionar_modelo_ao_pedido(pedido_id, cliente, status, entrada, prazo_finalizacao, responsavel_lancamento, modelo, cores):
    df = carregar_dados()
    novo_id = int(df["ID"].max() + 1) if not df.empty else 1
    total_grades = int(sum(int(v) for v in cores.values()))
    qtd = total_grades * PECAS_POR_GRADE
    novo = {
        "ID": novo_id,
        "Pedido": pedido_id,
        "Cliente": cliente.strip(),
        "Modelo": modelo.strip(),
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
        cliente=cliente.strip(),
        modelo=modelo.strip(),
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
    finalizados_periodo = int((eventos_filtrados["Acao"] == "FINALIZAR_PEDIDO").sum())
    arquivados = int((eventos_filtrados["Acao"] == "ARQUIVAR_PEDIDO").sum())
    pendentes_periodo = max(lancados_periodo - finalizados_periodo - arquivados, 0)

    texto = []
    texto.append(f"- Pedidos pendentes no periodo selecionado: {int(pendentes_periodo)}")
    texto.append(f"- Pedidos finalizados no periodo selecionado: {int(finalizados_periodo)}")
    texto.append(f"- Pedidos arquivados no periodo selecionado: {int(arquivados)}")
    texto.append(f"- Movimentos de retrabalho (volta de etapa): {int(retrabalho)}")
    return "\n".join(texto)


aplicar_estilo()
inicializar_sessao_acesso()
st.markdown(
    """
    <div class="hero-box">
      <p class="hero-title">Chao de Fabrica - Monitoramento</p>
      <p class="hero-sub">Controle de pedidos por etapa, com rastreabilidade e relatorios.</p>
    </div>
    """,
    unsafe_allow_html=True,
)
monitorar_alteracoes()
render_login_sidebar()
pode_lancar = pode_lancar_pedido()
pode_editar = pode_editar_completo()

st.info(
    "Acesso atual: "
    + ("EDITOR (controle total)." if pode_editar else "CONSULTA (pode lancar e separar; sem edicao apos Pronto).")
)

with st.sidebar:
    st.header("Novo pedido")
    if not pode_lancar:
        st.info("Sem permissao para lancar pedidos.")
    cliente = st.text_input("Nome do cliente", disabled=not pode_lancar)
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
        st.markdown(f"**Modelo {idx + 1}**")
        modelo = st.text_input("Nome do modelo/grade", key=f"modelo_{idx}", disabled=not pode_lancar)
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

        if modelo.strip() and grades > 0:
            modelos_validos.append(
                {
                    "Modelo": modelo.strip(),
                    "Grades": {k: int(v) for k, v in entradas_cores.items()},
                    "Qtd": pecas,
                    "GradesTotal": grades,
                }
            )

    if st.button("Lancar pedido", disabled=not pode_lancar):
        if not cliente.strip():
            st.warning("Preencha o nome do cliente.")
        elif not modelos_validos:
            st.warning("Informe ao menos 1 modelo com grades > 0.")
        else:
            df = carregar_dados()
            novo_id = int(df["ID"].max() + 1) if not df.empty else 1
            novo_pedido = int(df["Pedido"].max() + 1) if not df.empty else 1
            entrada = datetime.now().strftime("%d/%m %H:%M")
            prazo_finalizacao = gerar_prazo_padrao()

            novos = []
            qtd_total = 0
            grades_total = 0
            for item in modelos_validos:
                novos.append(
                    {
                        "ID": novo_id,
                        "Pedido": novo_pedido,
                        "Cliente": cliente.strip(),
                        "Modelo": item["Modelo"],
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
                    cliente=cliente.strip(),
                    modelo=item["Modelo"],
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
                cliente=cliente.strip(),
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

abas = st.tabs(["Painel", "Relatorios"])

with abas[0]:
    prod_hoje = contador_producao_dia(eventos)
    pedidos_abertos = int(df_atual[df_atual["Status"] != "4. Finalizado"]["Pedido"].nunique())
    pedidos_finalizados = int(df_atual[df_atual["Status"] == "4. Finalizado"]["Pedido"].nunique())

    c1, c2, c3 = st.columns(3)
    c1.markdown(
        f"<div class='kpi-card'><p class='kpi-label'>Producao do dia (pecas finalizadas)</p><p class='kpi-value'>{prod_hoje}</p></div>",
        unsafe_allow_html=True,
    )
    c2.markdown(
        f"<div class='kpi-card'><p class='kpi-label'>Pedidos em aberto</p><p class='kpi-value'>{pedidos_abertos}</p></div>",
        unsafe_allow_html=True,
    )
    c3.markdown(
        f"<div class='kpi-card'><p class='kpi-label'>Pedidos finalizados</p><p class='kpi-value'>{pedidos_finalizados}</p></div>",
        unsafe_allow_html=True,
    )

    col1, col2, col3, col4 = st.columns(4)
    fases = {
        STATUS_FLUXO[0]: (col1, "Separar pedido"),
        STATUS_FLUXO[1]: (col2, "Em costura"),
        STATUS_FLUXO[2]: (col3, "Acabamento"),
        STATUS_FLUXO[3]: (col4, "Prontos"),
    }

    for status_chave, (coluna_st, titulo_visual) in fases.items():
        with coluna_st:
            st.markdown(f"### {titulo_visual}")
            st.markdown("---")
            pedidos_fase = df_atual[df_atual["Status"] == status_chave]

            for pedido_id in sorted(pedidos_fase["Pedido"].unique()):
                grupo = pedidos_fase[pedidos_fase["Pedido"] == pedido_id]
                principal = grupo.iloc[0]
                total_grades = int(sum(total_grades_row(r) for _, r in grupo.iterrows()))
                total_pecas = int(grupo["Qtd"].sum())

                card = st.container(border=True)
                card.markdown(f"**Pedido #{pedido_id}**")
                responsavel_lancamento = str(principal.get("ResponsavelLancamento", "")).strip() or "-"
                card.caption(f"Lancado por: {responsavel_lancamento}")
                card.caption(f"Cliente: {principal['Cliente']}")
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
                        key=f"del_model_{item_id}",
                        help="Excluir modelo",
                        disabled=not pode_editar,
                    ):
                        deletar_modelo(item_id)
                    for linha_cor in linhas_cores(item):
                        card.caption(linha_cor)
                    card.caption(f"Total: {total_grades_row(item)} grade(s) | {int(item['Qtd'])} peca(s)")

                edit_open_key = f"edit_open_{pedido_id}"
                add_open_key = f"add_open_{pedido_id}"
                if edit_open_key not in st.session_state:
                    st.session_state[edit_open_key] = False
                if add_open_key not in st.session_state:
                    st.session_state[add_open_key] = False

                btn_top_l, btn_top_r = card.columns(2)
                btn_bot_l, btn_bot_r = card.columns(2)
                anterior = status_anterior(status_chave)
                proximo = proximo_status(status_chave)

                if anterior:
                    if btn_top_l.button(
                        "Voltar",
                        key=f"back_{pedido_id}",
                        use_container_width=True,
                        disabled=not pode_editar,
                    ):
                        mover_pedido(int(pedido_id), anterior)
                else:
                    btn_top_l.write("")

                pode_pronto_este_pedido = pode_mover_pronto(status_chave)
                if proximo:
                    texto_botao = "Finalizar" if proximo == STATUS_FLUXO[-1] else "Pronto"
                    if btn_top_r.button(
                        texto_botao,
                        key=f"mv_{pedido_id}",
                        use_container_width=True,
                        disabled=not pode_pronto_este_pedido,
                    ):
                        mover_pedido(int(pedido_id), proximo)
                else:
                    if btn_top_r.button(
                        "Arquivar",
                        key=f"del_{pedido_id}",
                        use_container_width=True,
                        disabled=not pode_editar,
                    ):
                        deletar_pedido(int(pedido_id))

                if btn_bot_l.button(
                    "Editar",
                    key=f"toggle_edit_{pedido_id}",
                    use_container_width=True,
                    disabled=not pode_editar,
                ):
                    st.session_state[edit_open_key] = not st.session_state[edit_open_key]
                    st.session_state[add_open_key] = False

                if btn_bot_r.button(
                    "+ Modelo",
                    key=f"toggle_add_{pedido_id}",
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
                        key=f"edit_select_{pedido_id}",
                        disabled=not pode_editar,
                    )
                    item_edit = grupo[grupo["ID"] == item_id_edit].iloc[0]

                    with card.form(key=f"form_edit_{pedido_id}"):
                        edit_cliente = st.text_input(
                            "Cliente",
                            value=str(item_edit["Cliente"]),
                            key=f"edit_cliente_{pedido_id}",
                            disabled=not pode_editar,
                        )
                        edit_modelo = st.text_input(
                            "Modelo/Grade",
                            value=str(item_edit["Modelo"]),
                            key=f"edit_modelo_{pedido_id}",
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
                                key=f"edit_{coluna_cor}_{pedido_id}_{item_id_edit}",
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
                                atualizar_modelo(item_id_edit, edit_cliente, edit_modelo, edit_cores)

                if st.session_state[add_open_key] and pode_editar:
                    card.markdown("---")
                    card.markdown("**Acrescentar modelo neste pedido**")
                    with card.form(key=f"form_add_model_{pedido_id}"):
                        add_modelo = st.text_input("Novo modelo/grade", key=f"add_modelo_{pedido_id}", disabled=not pode_editar)
                        add_cores = {}
                        cols_add = st.columns(2)
                        for i, (nome_cor, coluna_cor) in enumerate(zip(CORES, COLUNAS_CORES)):
                            add_cores[coluna_cor] = cols_add[i % 2].number_input(
                                nome_cor,
                                min_value=0,
                                value=0,
                                step=1,
                                key=f"add_{coluna_cor}_{pedido_id}",
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
                                    status=str(principal["Status"]),
                                    entrada=str(principal["Entrada"]),
                                    prazo_finalizacao=str(principal.get("PrazoFinalizacao", gerar_prazo_padrao())),
                                    responsavel_lancamento=str(principal.get("ResponsavelLancamento", "")).strip()
                                    or str(st.session_state.get("usuario", "")),
                                    modelo=add_modelo,
                                    cores=add_cores,
                                )

with abas[1]:
    st.subheader("Janela de relatorios")
    abas_rel = st.tabs(["Producao", "Futuro 1", "Futuro 2"])

    with abas_rel[0]:
        if eventos.empty:
            st.info("Ainda nao existem eventos para relatorio.")
        else:
            if firebase_store.is_enabled():
                st.caption(f"Modo performance ativo: exibindo ate {LIMITE_EVENTOS_FIREBASE} eventos mais recentes.")
            d1 = pd.to_datetime(eventos["Data"], errors="coerce")
            min_data = d1.min().date() if not d1.isna().all() else datetime.now().date()
            max_data = d1.max().date() if not d1.isna().all() else datetime.now().date()
            colf1, colf2 = st.columns(2)
            data_ini = colf1.date_input("Data inicial", value=min_data, min_value=min_data, max_value=max_data)
            data_fim = colf2.date_input("Data final", value=max_data, min_value=min_data, max_value=max_data)
            if data_fim < data_ini:
                st.warning("Data final menor que data inicial. Ajuste os filtros.")
            else:
                mask = (pd.to_datetime(eventos["Data"], errors="coerce").dt.date >= data_ini) & (
                    pd.to_datetime(eventos["Data"], errors="coerce").dt.date <= data_fim
                )
                ev = eventos[mask].copy()

                c1, c2, c3, c4 = st.columns(4)
                c1.metric("Pedidos lancados", int((ev["Acao"] == "LANCAR_PEDIDO").sum()))
                c2.metric("Modelos lancados", int((ev["Acao"] == "LANCAR_MODELO").sum()))
                c3.metric("Pecas finalizadas", int(ev[(ev["Acao"] == "MOVER_STATUS") & (ev["StatusDepois"] == "4. Finalizado")]["Qtd"].sum()))
                c4.metric("Exclusoes", int(((ev["Acao"] == "EXCLUIR_MODELO") | (ev["Acao"] == "ARQUIVAR_PEDIDO")).sum()))

                st.markdown("**Resumo inteligente da producao**")
                st.markdown(resumo_relatorio_producao(df_atual, ev))

                st.markdown("**Historico de eventos (separacao ate arquivamento/exclusao)**")
                tabela = ev[
                    ["DataHora", "Pedido", "ModeloID", "Cliente", "Modelo", "Acao", "StatusAntes", "StatusDepois", "Grades", "Qtd", "Detalhes"]
                ].sort_values(by="DataHora", ascending=False)
                total_linhas = len(tabela)
                colp1, colp2 = st.columns(2)
                por_pagina = int(colp1.selectbox("Linhas por pagina", [50, 100, 200], index=1))
                total_paginas = max(1, (total_linhas + por_pagina - 1) // por_pagina)
                pagina = int(colp2.number_input("Pagina", min_value=1, max_value=total_paginas, value=1, step=1))
                ini = (pagina - 1) * por_pagina
                fim = ini + por_pagina
                st.caption(f"Mostrando {min(fim, total_linhas)} de {total_linhas} evento(s).")
                st.dataframe(tabela.iloc[ini:fim], use_container_width=True, hide_index=True)

    with abas_rel[1]:
        st.info("Espaco reservado para futuros relatorios.")

    with abas_rel[2]:
        st.info("Espaco reservado para futuros relatorios.")
