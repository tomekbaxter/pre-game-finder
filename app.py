import os
import datetime as dt
from zoneinfo import ZoneInfo
from urllib.parse import quote_plus

import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime

from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, JsCode

# ============================================================
# PAGE CONFIG
# ============================================================

st.set_page_config(
    page_title="Pre-Game Finder",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# Remove Streamlit header bar + tighten padding
st.markdown(
    """
    <style>
    header[data-testid="stHeader"] { display: none; }
    .block-container { padding-top: 1.2rem !important; }
    </style>
    """,
    unsafe_allow_html=True,
)

TZ = ZoneInfo("Europe/London")
NOW = datetime.now(TZ).replace(tzinfo=None)

# ============================================================
# GLOBAL STYLING
# ============================================================

st.markdown(
    """
    <style>
    html, body, .stApp {
        background-color: #0e1117;
        color: #e6e6e6;
    }

    .block-container {
        padding-top: 3rem;
        padding-bottom: 0rem;
        padding-left: 1.4rem;
        padding-right: 1.4rem;
    }

    div.stButton > button {
        width: 100%;
        height: 3.1em;
        font-size: 1.05rem;
        font-weight: 600;
        border-radius: 8px;
        background-color: #111827;
        color: #e6e6e6;
        border: 1px solid #2a2f3a;
    }

    div.stButton > button:hover {
        background-color: #1a2233;
        border-color: #3b4252;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# ============================================================
# SUPABASE / POSTGRES CONNECTION (Secrets.txt)
# ============================================================

SECRETS_TXT_PATH = r"C:\Users\TomekBaxter\Dropbox\football_app\Secrets.txt"

def _read_kv_file(path: str) -> dict:
    out: dict[str, str] = {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            for raw in f:
                line = raw.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" not in line:
                    continue
                k, v = line.split("=", 1)
                out[k.strip()] = v.strip()
    except FileNotFoundError:
        return {}
    return out

def _build_db_url_from_txt(path: str) -> str:
    kv = _read_kv_file(path)

    host = kv.get("SUPABASE_HOST", "").strip()
    port = kv.get("SUPABASE_PORT", "").strip()
    db = kv.get("SUPABASE_DB", "").strip()
    user = kv.get("SUPABASE_USER", "").strip()
    pw = kv.get("SUPABASE_PASS", "").strip()

    missing = [
        k for k, val in {
            "SUPABASE_HOST": host,
            "SUPABASE_PORT": port,
            "SUPABASE_DB": db,
            "SUPABASE_USER": user,
            "SUPABASE_PASS": pw,
        }.items()
        if not val
    ]

    if missing:
        st.error(f"Secrets.txt is missing values for: {', '.join(missing)}")
        st.stop()

    user_q = quote_plus(user)
    pw_q = quote_plus(pw)

    return f"postgresql+psycopg2://{user_q}:{pw_q}@{host}:{port}/{db}"

def _get_db_url() -> str:
    # 1) Streamlit Cloud secrets (preferred)
    try:
        if "SUPABASE_DB_URL" in st.secrets and str(st.secrets["SUPABASE_DB_URL"]).strip():
            return str(st.secrets["SUPABASE_DB_URL"]).strip()

        needed = ["SUPABASE_HOST", "SUPABASE_PORT", "SUPABASE_DB", "SUPABASE_USER", "SUPABASE_PASS"]
        if all(k in st.secrets and str(st.secrets[k]).strip() for k in needed):
            host = str(st.secrets["SUPABASE_HOST"]).strip()
            port = str(st.secrets["SUPABASE_PORT"]).strip()
            db = str(st.secrets["SUPABASE_DB"]).strip()
            user = str(st.secrets["SUPABASE_USER"]).strip()
            pw = str(st.secrets["SUPABASE_PASS"]).strip()

            user_q = quote_plus(user)
            pw_q = quote_plus(pw)
            return f"postgresql+psycopg2://{user_q}:{pw_q}@{host}:{port}/{db}"
    except Exception:
        # If st.secrets isn't available for any reason, fall back to file below
        pass

    # 2) Local dev fallback (your Windows Secrets.txt)
    return _build_db_url_from_txt(SECRETS_TXT_PATH)


@st.cache_resource
def get_engine():
    return create_engine(
        _get_db_url(),
        pool_pre_ping=True,
        pool_size=5,
        max_overflow=5,
        future=True,
    )

ENGINE = get_engine()

def read_sql_df(sql: str, params: dict | None = None) -> pd.DataFrame:
    with ENGINE.begin() as conn:
        return pd.read_sql(text(sql), conn, params=params or {})

def read_sql_one(sql: str, params: dict | None = None) -> dict | None:
    df = read_sql_df(sql, params=params)
    if df.empty:
        return None
    return df.iloc[0].to_dict()

# ============================================================
# LOAD DATA
# ============================================================

@st.cache_data(ttl=60)
def load_fixtures() -> pd.DataFrame:
    sql = text(
        """
        SELECT
            eventid,
            hometeam,
            awayteam,
            league,
            date,
            kickoff,
            home,
            draw,
            away,
            comopp,
            sodd,
            xgh,
            xga,
            esoth,
            esota,
            hcosod,
            acosod,
            homewin,
            drawwin,
            awaywin,
            score,
            value
        FROM fixtures
        WHERE date >= CURRENT_DATE
        """
    )

    df = pd.read_sql(sql, engine)

    df = df.rename(
        columns={
            "eventid": "EventID",
            "hometeam": "HomeTeam",
            "awayteam": "AwayTeam",
            "league": "League",
            "date": "Date",
            "kickoff": "Kickoff",
            "home": "Home",
            "draw": "Draw",
            "away": "Away",
            "comopp": "ComOpp",
            "sodd": "SODD",
            "xgh": "XGH",
            "xga": "XGA",
            "esoth": "ESOTH",
            "esota": "ESOTA",
            "hcosod": "HCOSOD",
            "acosod": "ACOSOD",
            "homewin": "HomeWin%",
            "drawwin": "Draw%",
            "awaywin": "AwayWin%",
            "score": "Score",
            "value": "Value",
        }
    )

    # Build kickoff datetime (naive; assumed Europe/London local)
    df["KickoffDT"] = pd.to_datetime(
        df["Date"].astype(str) + " " + df["Kickoff"].astype(str),
        errors="coerce",
    )

    return df

# ============================================================
# GLOBAL FILTERS (ALWAYS APPLIED)
# ============================================================

def apply_global_filters(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    df = df.copy()

    # Odds present
    for c in ["Home", "Draw", "Away"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=["Home", "Draw", "Away"])
    df = df[(df["Home"] > 0) & (df["Draw"] > 0) & (df["Away"] > 0)]

    # Valid kickoff datetime and future only
    df = df[df["KickoffDT"].notna()]
    df = df[df["KickoffDT"] > NOW]

    return df.sort_values("KickoffDT")

# ============================================================
# FILTERS (KEEP LOGIC LOCAL TO EACH FUNCTION)
# ============================================================

def filter_all(df: pd.DataFrame) -> pd.DataFrame:
    return df

def filter_sodd(df: pd.DataFrame) -> pd.DataFrame:
    """
    SODD filter (NO COSOD):
    - Only cares about signal strength: abs(SODD) >= 7
    - Advantaged side determined by SODD sign
    - Applies odds acceptance curve + implied probability cap
    """

    S0 = 7.0
    S1 = 10.0
    ODDS0 = 1.60
    ODDS1 = 1.40
    PMAX_CAP = 0.80

    if df.empty:
        return df

    df = df.copy()

    required = ["SODD", "Home", "Away"]
    for c in required:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=required)
    if df.empty:
        return df

    s_abs = df["SODD"].abs()
    df = df[s_abs >= S0].copy()
    if df.empty:
        return df

    adv_home = df["SODD"] > 0
    adv_away = df["SODD"] < 0

    df = df[adv_home | adv_away].copy()
    if df.empty:
        return df

    adv_odds = pd.Series(index=df.index, dtype="float64")
    adv_odds.loc[adv_home] = df.loc[adv_home, "Home"]
    adv_odds.loc[adv_away] = df.loc[adv_away, "Away"]

    s_abs = df["SODD"].abs()

    if S1 > S0:
        required_odds = ODDS0 + (ODDS1 - ODDS0) * (s_abs - S0) / (S1 - S0)
    else:
        required_odds = pd.Series(ODDS1, index=df.index)

    required_odds = required_odds.clip(lower=ODDS1)

    df = df[adv_odds >= required_odds].copy()
    if df.empty:
        return df

    implied_prob = 1.0 / adv_odds
    df = df[implied_prob <= PMAX_CAP].copy()
    if df.empty:
        return df

    df["SODD_abs"] = s_abs
    df["AdvOdds"] = adv_odds
    df["RequiredOdds"] = required_odds
    df["ImpliedProb"] = implied_prob

    return df

def filter_sodd_cosod(df: pd.DataFrame) -> pd.DataFrame:
    """
    SODD + COSOD filter:
    - Lower SODD threshold (abs(SODD) >= S0)
    - Advantaged side determined by SODD sign
    - COSOD alignment required:
        advantaged COSOD > 0 and weaker COSOD < 0
    - Applies odds acceptance curve + implied probability cap
    """

    S0 = 3.0
    S1 = 7.0
    ODDS0 = 2.20
    ODDS1 = 1.40
    PMAX_CAP = 0.80

    if df.empty:
        return df

    df = df.copy()

    required = ["SODD", "Home", "Away", "HCOSOD", "ACOSOD"]
    for c in required:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=required)
    if df.empty:
        return df

    s_abs = df["SODD"].abs()
    df = df[s_abs >= S0].copy()
    if df.empty:
        return df

    adv_home = df["SODD"] > 0
    adv_away = df["SODD"] < 0
    df = df[adv_home | adv_away].copy()
    if df.empty:
        return df

    cosod_adv = pd.Series(index=df.index, dtype="float64")
    cosod_weak = pd.Series(index=df.index, dtype="float64")

    cosod_adv.loc[adv_home] = df.loc[adv_home, "HCOSOD"]
    cosod_weak.loc[adv_home] = df.loc[adv_home, "ACOSOD"]

    cosod_adv.loc[adv_away] = df.loc[adv_away, "ACOSOD"]
    cosod_weak.loc[adv_away] = df.loc[adv_away, "HCOSOD"]

    df = df[(cosod_adv > 1) & (cosod_weak < -1)].copy()
    if df.empty:
        return df

    adv_odds = pd.Series(index=df.index, dtype="float64")
    adv_odds.loc[adv_home] = df.loc[adv_home, "Home"]
    adv_odds.loc[adv_away] = df.loc[adv_away, "Away"]

    s_abs = df["SODD"].abs()
    if S1 > S0:
        required_odds = ODDS0 + (ODDS1 - ODDS0) * (s_abs - S0) / (S1 - S0)
    else:
        required_odds = pd.Series(ODDS1, index=df.index)

    required_odds = required_odds.clip(lower=ODDS1)

    df = df[adv_odds >= required_odds].copy()
    if df.empty:
        return df

    implied_prob = 1.0 / adv_odds
    df = df[implied_prob <= PMAX_CAP].copy()
    if df.empty:
        return df

    df["SODD_abs"] = s_abs
    df["AdvOdds"] = adv_odds
    df["RequiredOdds"] = required_odds
    df["ImpliedProb"] = implied_prob
    df["COSOD_Adv"] = cosod_adv
    df["COSOD_Weak"] = cosod_weak

    return df

def filter_xg_xsot(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    w_esot = 1.0
    w_xg = 0.8

    D0 = 3
    D1 = 5.0
    ODDS0 = 2.40
    ODDS1 = 1.40
    PMAX_CAP = 0.60

    df = df.copy()

    required = ["XGH", "XGA", "ESOTH", "ESOTA", "Home", "Away"]
    for c in required:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=required)
    if df.empty:
        return df

    esot_gap = df["ESOTH"] - df["ESOTA"]
    xg_gap = df["XGH"] - df["XGA"]
    D = (w_esot * esot_gap) + (w_xg * xg_gap)
    D_abs = D.abs()

    df = df[D_abs >= D0].copy()
    if df.empty:
        return df

    esot_gap = df["ESOTH"] - df["ESOTA"]
    xg_gap = df["XGH"] - df["XGA"]
    D = (w_esot * esot_gap) + (w_xg * xg_gap)
    D_abs = D.abs()

    adv_home = D > 0
    adv_away = D < 0
    df = df[adv_home | adv_away].copy()
    if df.empty:
        return df

    confirm = (
        (adv_home & (esot_gap > 0) & (xg_gap > 0)) |
        (adv_away & (esot_gap < 0) & (xg_gap < 0))
    )
    df = df[confirm].copy()
    if df.empty:
        return df

    esot_gap = df["ESOTH"] - df["ESOTA"]
    xg_gap = df["XGH"] - df["XGA"]
    D = (w_esot * esot_gap) + (w_xg * xg_gap)
    D_abs = D.abs()
    adv_home = D > 0
    adv_away = D < 0

    adv_odds = pd.Series(index=df.index, dtype="float64")
    adv_odds.loc[adv_home] = df.loc[adv_home, "Home"]
    adv_odds.loc[adv_away] = df.loc[adv_away, "Away"]

    if D1 > D0:
        required_odds = ODDS0 + (ODDS1 - ODDS0) * (D_abs - D0) / (D1 - D0)
    else:
        required_odds = pd.Series(ODDS1, index=df.index)
    required_odds = required_odds.clip(lower=ODDS1)

    df = df[adv_odds >= required_odds].copy()
    if df.empty:
        return df

    implied_prob = 1.0 / adv_odds
    df = df[implied_prob <= PMAX_CAP].copy()
    if df.empty:
        return df

    df["ESOT_Gap"] = esot_gap
    df["xG_Gap"] = xg_gap
    df["DomScore"] = D
    df["DomScore_abs"] = D_abs
    df["AdvOdds"] = adv_odds
    df["RequiredOdds"] = required_odds
    df["ImpliedProb"] = implied_prob

    return df

def filter_xwin_percent(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    MIN_ODDS = 1.60
    MIN_ABS_EDGE = 0.07
    MIN_REL_EDGE = 0.75

    df = df.copy()

    required = ["Home", "Draw", "Away", "HomeWin%", "AwayWin%", "Draw%"]
    for c in required:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=required)

    df = df[(df["HomeWin%"] > 0) & (df["AwayWin%"] > 0)]
    if df.empty:
        return df

    p_home_raw = 1 / df["Home"]
    p_draw_raw = 1 / df["Draw"]
    p_away_raw = 1 / df["Away"]
    overround = p_home_raw + p_draw_raw + p_away_raw

    p_home_mkt = p_home_raw / overround
    p_away_mkt = p_away_raw / overround

    p_home_model = df["HomeWin%"] / 100
    p_away_model = df["AwayWin%"] / 100

    home_abs_edge = p_home_model - p_home_mkt
    away_abs_edge = p_away_model - p_away_mkt

    home_rel_edge = home_abs_edge / p_home_mkt
    away_rel_edge = away_abs_edge / p_away_mkt

    home_value = (
        (df["Home"] >= MIN_ODDS) &
        (home_abs_edge >= MIN_ABS_EDGE) &
        (home_rel_edge >= MIN_REL_EDGE)
    )
    away_value = (
        (df["Away"] >= MIN_ODDS) &
        (away_abs_edge >= MIN_ABS_EDGE) &
        (away_rel_edge >= MIN_REL_EDGE)
    )

    df = df[home_value | away_value].copy()
    if df.empty:
        return df

    df["Home_MktProb"] = p_home_mkt
    df["Away_MktProb"] = p_away_mkt
    df["Home_ModelProb"] = p_home_model
    df["Away_ModelProb"] = p_away_model
    df["Home_AbsEdge"] = home_abs_edge
    df["Away_AbsEdge"] = away_abs_edge
    df["Home_RelEdge"] = home_rel_edge
    df["Away_RelEdge"] = away_rel_edge

    return df

def filter_head_to_head(df: pd.DataFrame) -> pd.DataFrame:
    """
    Head-to-Head UNDERDOG ADVANTAGE filter

    Shows fixtures where:
    - Teams played each other in the last 90 days (most recent H2H only)
    - Historical match has valid stats (not placeholder stats)
    - The CURRENT underdog (higher odds) had:
        * more shots on target
        * more dangerous attacks
      in that most recent H2H
    """

    if df.empty:
        return df

    LOOKBACK_DAYS = 90
    cutoff_date = (NOW - pd.Timedelta(days=LOOKBACK_DAYS)).date()

    sql = text(
        """
        SELECT
            "HomeTeam",
            "AwayTeam",
            "Date",
            "HomeGoals",
            "AwayGoals",
            "HomeShots",
            "AwayShots",
            "HomeShotsOn",
            "AwayShotsOn",
            "HomeDangerousAttacks",
            "AwayDangerousAttacks"
        FROM matchstats
        WHERE "Date" >= :cutoff_date
        """
    )

    h2h = pd.read_sql(sql, engine, params={"cutoff_date": cutoff_date})
    if h2h.empty:
        return df.iloc[0:0]

    h2h["Date"] = pd.to_datetime(h2h["Date"], errors="coerce")
    h2h = h2h.dropna(subset=["Date"])

    stat_cols = [
        "HomeGoals", "AwayGoals",
        "HomeShots", "AwayShots",
        "HomeShotsOn", "AwayShotsOn",
        "HomeDangerousAttacks", "AwayDangerousAttacks",
    ]
    for c in stat_cols:
        h2h[c] = pd.to_numeric(h2h[c], errors="coerce")
    h2h = h2h.dropna(subset=stat_cols)
    if h2h.empty:
        return df.iloc[0:0]

    total_goals = h2h["HomeGoals"] + h2h["AwayGoals"]
    total_shots = h2h["HomeShots"] + h2h["AwayShots"]
    total_sot = h2h["HomeShotsOn"] + h2h["AwayShotsOn"]

    h2h = h2h[
        (total_shots > total_goals) &
        (total_sot >= total_goals) &
        (total_shots >= 6)
    ].copy()

    if h2h.empty:
        return df.iloc[0:0]

    def make_pair_key(a: str, b: str) -> str:
        a = "" if pd.isna(a) else str(a).strip()
        b = "" if pd.isna(b) else str(b).strip()
        return "||".join(sorted([a, b]))

    h2h["PairKey"] = h2h.apply(lambda r: make_pair_key(r["HomeTeam"], r["AwayTeam"]), axis=1)

    h2h_latest = (
        h2h.sort_values("Date", ascending=False)
           .drop_duplicates("PairKey", keep="first")
           .copy()
    )

    df = df.copy()
    df["PairKey"] = df.apply(lambda r: make_pair_key(r["HomeTeam"], r["AwayTeam"]), axis=1)

    df = df.merge(
        h2h_latest[[
            "PairKey",
            "HomeTeam", "AwayTeam",
            "HomeShotsOn", "AwayShotsOn",
            "HomeDangerousAttacks", "AwayDangerousAttacks",
            "Date"
        ]].rename(columns={
            "HomeTeam": "H2H_HomeTeam",
            "AwayTeam": "H2H_AwayTeam",
            "HomeShotsOn": "H2H_HomeShotsOn",
            "AwayShotsOn": "H2H_AwayShotsOn",
            "HomeDangerousAttacks": "H2H_HomeDangerousAttacks",
            "AwayDangerousAttacks": "H2H_AwayDangerousAttacks",
            "Date": "H2H_Date",
        }),
        on="PairKey",
        how="inner",
    )

    if df.empty:
        return df

    df["UnderdogSide"] = None
    df.loc[df["Home"] > df["Away"], "UnderdogSide"] = "Home"
    df.loc[df["Away"] > df["Home"], "UnderdogSide"] = "Away"
    df = df[df["UnderdogSide"].notna()].copy()

    if df.empty:
        return df

    def underdog_edge_row(row) -> bool:
        ud_team = row["HomeTeam"] if row["UnderdogSide"] == "Home" else row["AwayTeam"]
        opp_team = row["AwayTeam"] if row["UnderdogSide"] == "Home" else row["HomeTeam"]

        if row["H2H_HomeTeam"] == ud_team and row["H2H_AwayTeam"] == opp_team:
            ud_sot = row["H2H_HomeShotsOn"]
            opp_sot = row["H2H_AwayShotsOn"]
            ud_dang = row["H2H_HomeDangerousAttacks"]
            opp_dang = row["H2H_AwayDangerousAttacks"]
        elif row["H2H_AwayTeam"] == ud_team and row["H2H_HomeTeam"] == opp_team:
            ud_sot = row["H2H_AwayShotsOn"]
            opp_sot = row["H2H_HomeShotsOn"]
            ud_dang = row["H2H_AwayDangerousAttacks"]
            opp_dang = row["H2H_HomeDangerousAttacks"]
        else:
            return False

        return (ud_sot > opp_sot) and (ud_dang > opp_dang)

    df = df[df.apply(underdog_edge_row, axis=1)].copy()
    return df

def filter_league_table(df: pd.DataFrame) -> pd.DataFrame:
    return df.iloc[0:0]

# ============================================================
# FILTER REGISTRY (7 BUTTONS)
# ============================================================

FILTERS = [
    ("ALL", "All Fixtures", filter_all),
    ("SODD", "SODD", filter_sodd),
    ("SCOSOD", "SODD + COSOD", filter_sodd_cosod),
    ("XG", "xG / xSOT", filter_xg_xsot),
    ("XWIN", "XWin%", filter_xwin_percent),
    ("H2H", "Head-to-Head", filter_head_to_head),
    ("LEAGUE", "League Table", filter_league_table),
]

# ============================================================
# HEADER
# ============================================================

st.markdown("## Pre-Game Finder")

# ============================================================
# BUTTON BAR (7 BUTTONS)
# ============================================================

if "active_filter" not in st.session_state:
    st.session_state.active_filter = "ALL"

cols = st.columns([1.2, 1, 1.4, 1.2, 1.1, 1.4, 1.4, 3.3])

for i, (key, label, _) in enumerate(FILTERS):
    with cols[i]:
        if st.button(label):
            st.session_state.active_filter = key

# ============================================================
# PIPELINE
# ============================================================

df = apply_global_filters(load_fixtures())

active_key = st.session_state.active_filter
active_fn = {k: fn for (k, _, fn) in FILTERS}[active_key]
df = active_fn(df)

if not df.empty:
    df["Date"] = df["KickoffDT"].dt.strftime("%d/%m/%Y")
    df["Kickoff"] = df["KickoffDT"].dt.strftime("%H:%M")

DISPLAY_COLS = [
    "EventID", "HomeTeam", "AwayTeam", "League",
    "Date", "Kickoff",
    "Home", "Draw", "Away", "ComOpp",
    "SODD", "HCOSOD", "ACOSOD", "XGH", "XGA", "ESOTH",
    "ESOTA", "HomeWin%", "Draw%", "AwayWin%", "Score", "Value",
]

df_view = df[DISPLAY_COLS] if not df.empty else pd.DataFrame(columns=DISPLAY_COLS)

st.markdown(f"**Fixtures ({len(df_view)})**")

# ============================================================
# AG GRID (TABLE)
# ============================================================

row_style = JsCode(
    """
    function(params) {
      return {
        background: params.node.rowIndex % 2 === 0 ? '#111827' : '#0e1117'
      };
    }
    """
)

custom_css = {
    ".ag-root-wrapper": {
        "border": "1px solid rgba(255,255,255,0.15)",
        "border-radius": "10px",
    },
    ".ag-header": {
        "background-color": "#1f2937",
        "border-bottom": "2px solid rgba(255,255,255,0.25)",
    },
    ".ag-header-cell-label": {
        "color": "#f0f2f5",
        "font-weight": "700",
        "font-size": "14px",
    },
    ".ag-cell": {
        "color": "#eaecef",
        "font-size": "13px",
        "white-space": "nowrap",
        "padding-left": "8px",
        "padding-right": "8px",
    },
}

gb = GridOptionsBuilder.from_dataframe(df_view)

for col in df_view.columns:
    gb.configure_column(
        col,
        sortable=True,
        filter=True,
        resizable=True,
        wrapText=False,
        autoHeight=False,
        minWidth=85,
    )

for col in ["HomeTeam", "AwayTeam", "League"]:
    gb.configure_column(col, minWidth=190)

gb.configure_grid_options(
    domLayout="normal",
    enableRangeSelection=True,
    enableCellTextSelection=True,
    suppressRowClickSelection=True,
    getRowStyle=row_style,
    rowHeight=30,
    headerHeight=36,
    onGridReady=JsCode(
        """
        function(params) {
            params.api.sizeColumnsToFit();
        }
        """
    ),
)

GRID_HEIGHT = max(140, 36 + 30 * (len(df_view) + 1))

AgGrid(
    df_view,
    gridOptions=gb.build(),
    height=GRID_HEIGHT,
    update_mode=GridUpdateMode.NO_UPDATE,
    theme="alpine-dark",
    allow_unsafe_jscode=True,
    custom_css=custom_css,
)

st.caption(
    "Drag to select cells -> Ctrl+C to copy. "
    "Pre-Game Finder - Supabase-backed - internal read-only tool."
)

# ============================================================
# EXPORT CURRENT VIEW TO CSV
# ============================================================

csv_bytes = df_view.to_csv(index=False).encode("utf-8")

st.download_button(
    label="Export table to CSV",
    data=csv_bytes,
    file_name=f"pre_game_finder_{active_key.lower()}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
    mime="text/csv",
)
