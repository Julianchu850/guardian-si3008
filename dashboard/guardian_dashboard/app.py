"""Dashboard "tres mundos": neutral, depredador y con guardián.

Ejecutar:  streamlit run guardian_dashboard/app.py
Variables: GUARDIAN_LAKE (s3://bucket/guardian o carpeta local),
           GUARDIAN_BACKEND (athena | duckdb), ATHENA_OUTPUT, ATHENA_DATABASE.
"""
from __future__ import annotations

import math
from datetime import date

import altair as alt
import pandas as pd
import streamlit as st

from guardian_dashboard import queries as q
from guardian_dashboard.backends import from_env

# Paleta validada (CVD y contraste sobre la superficie del dashboard): azul claro,
# naranja, azul marino. Cada mundo conserva su color en todos los gráficos.
COLORS = {"Neutral": "#5f8fd0", "Depredador": "#d0612a", "Con guardián": "#1f4fa0"}
INK, INK_2, GRID, SURFACE = "#0B3573", "#4a5a78", "#e4d9ca", "#FBF7F1"
FONT = "Source Sans Pro, Arial, sans-serif"

st.set_page_config(page_title="Guardián · tres mundos", page_icon="🛡️", layout="wide")


# --------------------------------------------------------------------------
# Datos
# --------------------------------------------------------------------------
@st.cache_resource(show_spinner=False)
def backend():
    return from_env()


@st.cache_data(ttl=300, show_spinner=False)
def run(sql: str) -> pd.DataFrame:
    return backend().query(sql)


@st.cache_data(ttl=300, show_spinner=False)
def model_metrics():
    return backend().model_metrics()


def fmt(v, kind="num", d=1):
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return "—"
    if kind == "pct":
        return f"{v * 100:.{d}f} %"
    if kind == "usd":
        return f"US$ {v:,.{d}f}"
    return f"{v:,.{d}f}"


# --------------------------------------------------------------------------
# Gráficos
# --------------------------------------------------------------------------
def _style(chart: alt.Chart, height: int = 260) -> alt.Chart:
    return (chart.properties(height=height, background=SURFACE,
                             padding={"left": 8, "right": 8, "top": 8, "bottom": 8})
            .configure_view(strokeWidth=0)
            .configure_axis(labelColor=INK_2, titleColor=INK_2, gridColor=GRID, domainColor=GRID,
                            tickColor=GRID, labelFont=FONT, titleFont=FONT, labelFontSize=12,
                            titleFontSize=12, titleFontWeight="normal")
            .configure_legend(labelColor=INK, titleColor=INK_2, labelFont=FONT, titleFont=FONT,
                              orient="top", labelFontSize=12, symbolType="square")
            .configure_title(color=INK, font=FONT, fontSize=15, anchor="start", fontWeight=600))


def _color(domain):
    return alt.Color("mundo:N", title=None, scale=alt.Scale(domain=domain,
                     range=[COLORS[d] for d in domain]))


def bars_by_world(df: pd.DataFrame, field: str, title: str, fmt_str: str, domain) -> alt.Chart:
    base = alt.Chart(df, title=title).encode(
        x=alt.X("mundo:N", sort=domain, title=None, axis=alt.Axis(labelAngle=0, ticks=False)),
        y=alt.Y(f"{field}:Q", title=None, axis=alt.Axis(format=fmt_str, tickCount=4, grid=True)),
        tooltip=[alt.Tooltip("mundo:N", title="Mundo"), alt.Tooltip(f"{field}:Q", title=title, format=fmt_str)])
    marks = base.mark_bar(size=34, cornerRadiusTopLeft=4, cornerRadiusTopRight=4).encode(
        color=_color(domain).legend(None))
    labels = base.mark_text(dy=-8, color=INK, font=FONT, fontSize=13, fontWeight=600).encode(
        text=alt.Text(f"{field}:Q", format=fmt_str))
    return _style(marks + labels, 220)


def daily_lines(df: pd.DataFrame, field: str, title: str, fmt_str: str, domain) -> alt.Chart:
    hover = alt.selection_point(fields=["fecha"], nearest=True, on="pointerover", empty=False,
                                clear="pointerout")
    base = alt.Chart(df, title=title).encode(
        x=alt.X("fecha:T", title=None, axis=alt.Axis(format="%d %b", tickCount="day", grid=False)),
        y=alt.Y(f"{field}:Q", title=None, axis=alt.Axis(format=fmt_str, tickCount=4)),
        color=_color(domain))
    lines = base.mark_line(strokeWidth=2)
    points = base.mark_point(size=60, filled=True, stroke=SURFACE, strokeWidth=2).encode(
        opacity=alt.condition(hover, alt.value(1), alt.value(0)))
    tips = [alt.Tooltip("fecha:T", title="Fecha", format="%d %b")] + [
        alt.Tooltip(f"{w}:Q", title=w, format=fmt_str) for w in domain]
    wide = df.pivot_table(index="fecha", columns="mundo", values=field).reset_index()
    rule = (alt.Chart(wide).mark_rule(color=INK_2, strokeWidth=1)
            .encode(x="fecha:T", opacity=alt.condition(hover, alt.value(0.5), alt.value(0)),
                    tooltip=tips).add_params(hover))
    return _style(lines + points + rule, 260)


def grouped_bars(df, x, x_title, field, title, fmt_str, domain, x_sort=None) -> alt.Chart:
    chart = alt.Chart(df, title=title).mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4).encode(
        x=alt.X(f"{x}:N", title=x_title, sort=x_sort, axis=alt.Axis(labelAngle=0, ticks=False)),
        xOffset=alt.XOffset("mundo:N", sort=domain, scale=alt.Scale(paddingInner=0.15)),
        y=alt.Y(f"{field}:Q", title=None, axis=alt.Axis(format=fmt_str, tickCount=4)),
        color=_color(domain),
        tooltip=[alt.Tooltip("mundo:N", title="Mundo"), alt.Tooltip(f"{x}:N", title=x_title),
                 alt.Tooltip(f"{field}:Q", title=title, format=fmt_str)])
    return _style(chart, 280)


def intervention_tiles(df: pd.DataFrame):
    cols = st.columns(len(df))
    for col, (_, row) in zip(cols, df.sort_values("accion").iterrows()):
        col.metric(q.ACTION_LABEL.get(row.accion, row.accion), f"{int(row.intervenciones):,}")
        col.caption(f"a {int(row.jugadores):,} jugadores")


def table(df: pd.DataFrame, label: str = "Ver datos"):
    with st.expander(label):
        st.dataframe(df, hide_index=True, use_container_width=True)


# --------------------------------------------------------------------------
# Página
# --------------------------------------------------------------------------
st.markdown(
    f"<h1 style='margin-bottom:0;color:{INK}'>Guardián</h1>"
    f"<p style='color:{INK_2};font-size:1.1rem;margin-top:4px'>Auditoría de monetización depredadora "
    "en un juego tipo Block Blast: el mismo juego en tres mundos.</p>", unsafe_allow_html=True)

try:
    pops = run(q.sql_populations())
except Exception as exc:  # sin tablas todavía
    st.error(f"No se pudo leer el data lake: {exc}. ¿Ya corrió el ETL (guardian_etl)?")
    st.stop()

pops["desde"] = pd.to_datetime(pops.desde).dt.date
pops["hasta"] = pd.to_datetime(pops.hasta).dt.date
pop_options = ["Todas"] + [p for p in pops.poblacion]
default_pop = next((p for p, m in zip(pops.poblacion, pops.mundos) if m >= 3), "Todas")

c1, c2, c3 = st.columns([1.2, 2, 2])
pop = c1.selectbox("Población (semilla)", pop_options, index=pop_options.index(default_pop),
                   help="Con la misma semilla, los mundos tienen exactamente los mismos jugadores.")
sel = pops if pop == "Todas" else pops[pops.poblacion == pop]
dmin, dmax = sel.desde.min(), sel.hasta.max()
dates = c2.date_input("Fechas", (dmin, dmax), min_value=dmin, max_value=dmax, format="YYYY-MM-DD")
worlds_sel = c3.multiselect("Mundos", [q.WORLD_LABEL[w] for w in q.WORLDS],
                            default=[q.WORLD_LABEL[w] for w in q.WORLDS])
if not isinstance(dates, (tuple, list)) or len(dates) != 2 or not worlds_sel:
    st.info("Elige un rango de fechas completo y al menos un mundo.")
    st.stop()
inv = {v: k for k, v in q.WORLD_LABEL.items()}
F = q.Filters(dates[0], dates[1], None if pop == "Todas" else pop, tuple(inv[w] for w in worlds_sel))

with st.spinner("Consultando el data lake…"):
    worlds = run(q.sql_worlds(F))
    forced = run(q.sql_forced_by_rule(F))
    player_rev = run(q.sql_player_revenue(F))
    daily = run(q.sql_daily(F))
if worlds.empty:
    st.warning("No hay datos para estos filtros.")
    st.stop()
M = q.world_metrics(worlds, forced, player_rev)
M["mundo"] = M.index.map(q.WORLD_LABEL)
domain = list(M.mundo)
if pop == "Todas" and len(domain) > 1:
    st.caption("⚠️ Con todas las poblaciones, cada mundo puede tener jugadores distintos. "
               "Para comparar los mismos jugadores, elige una semilla.")

# ---- Resumen -------------------------------------------------------------
st.subheader("Resumen")
cols = st.columns(len(M))
for col, (pol, r) in zip(cols, M.iterrows()):
    with col:
        st.markdown(f"<div style='border-top:6px solid {COLORS[r.mundo]};padding-top:6px'>"
                    f"<b style='font-size:1.15rem;color:{INK}'>{r.mundo}</b> "
                    f"<span style='color:{INK_2}'>· {int(r.jugadores)} jugadores</span></div>",
                    unsafe_allow_html=True)
        a, b = st.columns(2)
        a.metric("Ingreso por anuncios / jugador", fmt(r.anuncios_por_jugador, "usd", 3))
        b.metric("Anuncios por hora", fmt(r.anuncios_hora))
        a.metric("Manos letales / 100 partidas", fmt(r.letales_100_partidas, d=2))
        b.metric("Derrotas tras superar el récord", fmt(r.derrotas_tras_record, "pct"))

# ---- Ingreso vs daño -----------------------------------------------------
st.subheader("Ingreso vs. daño")
st.caption("Lo que gana el estudio (izquierda) frente a lo que sufre el jugador (derecha).")
g = st.columns(4)
specs = [("anuncios_por_jugador", "Ingreso por anuncios / jugador (US$)", ",.3f"),
         ("anuncios_hora", "Anuncios por hora de juego", ",.1f"),
         ("letales_100_partidas", "Manos letales / 100 partidas", ",.2f"),
         ("derrotas_tras_record", "Derrotas tras superar el récord", ".1%")]
for col, (field, title, f_) in zip(g, specs):
    col.altair_chart(bars_by_world(M.reset_index(), field, title, f_, domain),
                     use_container_width=True, theme=None)
show = M.reset_index().rename(columns={"policy": "política"})
table(show.round(4), "Ver la tabla completa por mundo")

# ---- Evolución diaria ----------------------------------------------------
st.subheader("Día a día")
D = q.daily_metrics(daily)
l, r = st.columns(2)
l.altair_chart(daily_lines(D, "ingreso_por_jugador", "Ingreso por jugador activo (US$)", ",.2f", domain),
               use_container_width=True, theme=None)
r.altair_chart(daily_lines(D, "anuncios_hora", "Anuncios por hora de juego", ",.1f", domain),
               use_container_width=True, theme=None)
table(D[["dt", "mundo", "jugadores", "ingreso_por_jugador", "anuncios_hora"]].round(
    {"ingreso_por_jugador": 3, "anuncios_hora": 1}))

# ---- Manipulación --------------------------------------------------------
st.subheader("Manipulación: qué regla la provocó")
FR = q.forced_by_rule_metrics(forced, worlds)
blocked = int(M.get("bloqueados", pd.Series(dtype=float)).sum())
l, r = st.columns([2, 1])
if FR.empty:
    l.info("No hay repartos manipulados con estos filtros.")
else:
    l.altair_chart(grouped_bars(FR, "regla_txt", None, "letales_100_partidas",
                                "Manos letales por 100 partidas", ",.2f", domain,
                                x_sort=list(q.RULE_LABEL.values())),
                   use_container_width=True, theme=None)
with r:
    st.markdown(f"<p style='color:{INK_2};margin-bottom:0'>Intentos de manipulación que el guardián "
                "bloqueó</p>", unsafe_allow_html=True)
    st.markdown(f"<p style='font-size:3rem;font-weight:700;color:{INK};margin:0'>{blocked:,}</p>",
                unsafe_allow_html=True)
    if "guarded" in M.index and "predatory" in M.index:
        p_, g_ = M.loc["predatory", "letales_100_partidas"], M.loc["guarded", "letales_100_partidas"]
        if p_ and not math.isnan(p_):
            st.markdown(f"Con el guardián, las manos letales bajan **{(1 - g_ / p_):.0%}** "
                        f"({p_:.2f} → {g_:.2f} por 100 partidas).")
    st.caption("Regla «Jugador rápido»: reparte piezas grandes, no imposibles; por eso no suma manos letales.")
table(FR[["mundo", "regla_txt", "repartos", "forzados", "bloqueados", "letales_100_partidas"]].round(3))

# ---- Perfiles ------------------------------------------------------------
st.subheader("¿A quién golpea? (verdad terreno del simulador)")
st.caption("Usa el perfil oculto de cada agente: un estudio real no lo conoce, el experimento sí.")
P = q.profile_metrics(run(q.sql_by_profile(F)), run(q.sql_forced_by_profile(F)))
order = [q.PROFILE_LABEL[k] for k in ("casual", "competitive", "spender", "loss_chaser", "whale")]
l, r = st.columns(2)
l.altair_chart(grouped_bars(P, "perfil_txt", None, "letales_100_partidas",
                            "Manos letales por 100 partidas", ",.2f", domain, x_sort=order),
               use_container_width=True, theme=None)
r.altair_chart(grouped_bars(P, "perfil_txt", None, "ingreso_por_jugador",
                            "Ingreso por jugador (US$)", ",.2f", domain, x_sort=order),
               use_container_width=True, theme=None)
table(P[["mundo", "perfil_txt", "jugadores", "partidas", "letales_100_partidas",
         "ingreso_por_jugador", "participacion_ingreso"]].round(3))

# ---- Guardián ------------------------------------------------------------
st.subheader("El guardián")
mm = model_metrics()
l, r = st.columns([1.1, 1])
with l:
    if not mm:
        st.info("Todavía no hay un modelo publicado (DAG guardian_entrenamiento).")
    else:
        t, b = mm["test"], mm["baseline_rule_test"]
        st.markdown(f"**Modelo `{mm['version']}`** · umbral {mm['threshold']:.3f} · probado con "
                    f"{mm['data']['test_rows']:,} repartos de {mm['data']['test_dates'][0]} a "
                    f"{mm['data']['test_dates'][-1]} ({mm['data']['test_positives']} manipulados)")
        cmp_ = pd.DataFrame({
            "": ["Modelo", "Regla escrita a mano"],
            "Precisión": [t["precision"], b["precision"]], "Recall": [t["recall"], b["recall"]],
            "Falsas alarmas": [t["confusion"]["fp"], b["confusion"]["fp"]],
            "Detectados": [f"{t['confusion']['tp']} de {t['confusion']['tp'] + t['confusion']['fn']}",
                           f"{b['confusion']['tp']} de {b['confusion']['tp'] + b['confusion']['fn']}"],
            "PR-AUC": [f"{t['pr_auc']:.2f}" if t.get("pr_auc") is not None else "—", "—"]})
        st.dataframe(cmp_, hide_index=True, use_container_width=True,
                     column_config={"Precisión": st.column_config.NumberColumn(format="percent"),
                                    "Recall": st.column_config.NumberColumn(format="percent")})
        imp = pd.DataFrame(mm["importance"], columns=["variable", "importancia"])
        imp = imp[imp.importancia > 0].head(8)
        chart = alt.Chart(imp, title="Qué mira el modelo (importancia por permutación)").mark_bar(
            color=COLORS["Con guardián"], cornerRadiusTopRight=4, cornerRadiusBottomRight=4, size=16
        ).encode(y=alt.Y("variable:N", sort="-x", title=None),
                 x=alt.X("importancia:Q", title=None, axis=alt.Axis(tickCount=4)),
                 tooltip=["variable", alt.Tooltip("importancia:Q", format=".3f")])
        st.altair_chart(_style(chart, 230), use_container_width=True, theme=None)
with r:
    emb = run(q.sql_interventions_embedded(F))
    stream = run(q.sql_interventions_stream())
    st.markdown("**Intervenciones del guardián embebido** (mundo con guardián)")
    if emb.empty:
        st.caption("Sin intervenciones con estos filtros.")
    else:
        intervention_tiles(emb)
    st.markdown("**Intervenciones en streaming** (tópico `interventions`, todo el histórico)")
    if stream.empty:
        st.caption("El servicio guardian-scorer todavía no ha publicado intervenciones.")
    else:
        intervention_tiles(stream)
        recent = run(q.sql_interventions_recent(12))
        recent["accion"] = recent.accion.map(q.ACTION_LABEL).fillna(recent.accion)
        recent["riesgo"] = recent.riesgo.map(lambda v: "—" if pd.isna(v) else f"{v:.3f}")
        recent["ts"] = recent.ts.astype(str).str[:19].str.replace("T", " ")
        st.dataframe(recent, hide_index=True, use_container_width=True)

# ---- Pie -------------------------------------------------------------------
with st.expander("Estado del data lake"):
    st.dataframe(run(q.sql_freshness()), hide_index=True, use_container_width=True)
st.caption(f"Fuente: {backend().name} · base `guardian` · datos simulados (los valores de anuncios y "
           "comportamiento son supuestos de calibración). Caso de referencia: Omolewa (2026), "
           "How Block Blast harvests its players for ad money.")
