# app.py
import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd
import snowflake.connector
import altair as alt


st.title(":microbe: Casos de Doenças e Agravos no Espírito Santo")
st.write("Dados provenientes do SINAN (Zika e Chikungunya). Fonte: Governo do Estado do ES (apenas dados sobre zika e chikungunya de 2024 por enquanto)")

# Obtendo a sesssão do Snowflake
cnx = st.connection("snowflake")
session = cnx.session()

# Acessando a tabela, criando Dataframe Snowpark e convertando para Dataframa Pandas
df = session.table('saude_es.prototipo.casos_es').to_pandas()

# Criando filtros usando o Streamlit e Pandas
col1, col2 = st.columns(2)

with col1:
    doencas = sorted(df["DESCRICAO"].dropna().unique()) 
    doenca_sel = st.selectbox("Selecione a doença:", doencas)

with col2:
    municipios = ["Todos"] + sorted(df["MUNICIPIO_PACIENTE"].dropna().unique())
    municipio_sel = st.selectbox("Selecione o município:", municipios)

# Aplicando filtros
df_filtro = df[df["DESCRICAO"] == doenca_sel]
if municipio_sel != "Todos":
    df_filtro = df_filtro[df_filtro["MUNICIPIO_PACIENTE"] == municipio_sel]


# MÉTRICAS GERAIS
colA, colB, colC = st.columns(3)
colA.metric("Casos Totais", len(df_filtro))
colB.metric("Casos Confirmados", (df_filtro["CLASSIF_FINAL"].str.contains("Confirmado", case=False, na=False)).sum())
colC.metric("Óbitos", df_filtro["EVOLUC_CASO"].str.contains("Óbito", case=False, na=False).sum())

# ---------------------------
# GRÁFICO 1 — Evolução temporal
# ---------------------------
df_filtro["DATA_NOTIFICACAO"] = pd.to_datetime(df_filtro["DATA_NOTIFICACAO"], errors="coerce")
df_tempo = df_filtro.groupby(pd.Grouper(key="DATA_NOTIFICACAO", freq="W")).size().reset_index(name="CASOS")

chart1 = alt.Chart(df_tempo).mark_line(point=True).encode(
    x=alt.X("DATA_NOTIFICACAO:T", title="Data de Notificação"),
    y=alt.Y("CASOS:Q", title="Número de Casos"),
    tooltip=["DATA_NOTIFICACAO", "CASOS"]
).properties(
    title=f"Evolução semanal dos casos de {doenca_sel}"
)

st.altair_chart(chart1, use_container_width=True)

# ---------------------------
# GRÁFICO 2 — Casos por município
# ---------------------------
df_mun = df[df["DESCRICAO"] == doenca_sel].groupby("MUNICIPIO_PACIENTE").size().reset_index(name="CASOS")
df_mun = df_mun.sort_values("CASOS", ascending=False).head(15)

chart2 = alt.Chart(df_mun).mark_bar().encode(
    x=alt.X("CASOS:Q", title="Casos"),
    y=alt.Y("MUNICIPIO_PACIENTE:N", sort="-x", title="Município"),
    tooltip=["MUNICIPIO_PACIENTE", "CASOS"]
).properties(
    title=f"Top 15 municípios com mais casos de {doenca_sel}"
)

st.altair_chart(chart2, use_container_width=True)

