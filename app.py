import pandas as pd
import streamlit as st
from datetime import datetime
from zoneinfo import ZoneInfo

from sqlalchemy import create_engine, text
from streamlit.errors import StreamlitSecretNotFoundError

# ============================================================
# PAGE CONFIG
# ============================================================

st.set_page_config(
    page_title="Pre-Game Finder",
    layout="wide",
    initial_sidebar_state="collapsed",
)

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

    div[data-testid="stDataFrame"] {
        border: 1px solid rgba(255,255,255,0.15);
        border-radius: 10px;
        overflow: hidden;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# ============================================================
# SUPABASE / POSTGRES CONNECTION (STREAMLIT SECRETS ONLY)
# ============================================================

def _get_db_url() -> str:
    try:
        db_url = st.secrets.get("SUPABASE_DB_URL", "")
    except StreamlitSecretNotFoundError:
        st.error(
            "Missing Streamlit Secrets.\n\n"
            "Set SUPABASE_DB_URL in Streamlit Cloud or local secrets.toml."
        )
        st.stop()

    if not isinstance(db_url, str) or not db_url.strip():
        st.error(
            "SUPABASE_DB_URL is missing or empty in Streamlit Secrets."
        )
        st.stop()

    return db_url.strip()

@st.cache_resource
def get_engine():
    return create_engine(
        _get_db_url(),
        pool_pre_ping=True,
        pool_size=1,
        max_overflow=0,
        pool_timeout=15,
        pool_recycle=300,
        connect_args={"sslmode": "require"},
        future=True,
    )

ENGINE = get_engine()

def _db_healthcheck() -> None:
    try:
        with ENGINE.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception as e:
        st.error(f"Database connection failed: {e}")
        st.stop()

_db_healthcheck()

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

    df = pd.read_sql(sql, ENGINE)

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

    now = datetime.now(TZ).replace(tzinfo=None)

    df = df.copy()

    for c in ["Home", "Draw", "Away"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=["Home", "Draw", "Away"])
    df = df[(df["Home"] > 0) & (df["Draw"] > 0) & (df["Away"] > 0)]

    df = df[df["KickoffDT"].notna()]
    df = df[df["KickoffDT"] > now]

    return df.sort_values("KickoffDT")

# ============================================================
# FILTERS
# ============================================================

def filter_all(df: pd.DataFrame) -> pd.DataFrame:
    return df

def filter_sodd(df: pd.DataFrame) -> pd.DataFrame:
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
    if df.empty:
        return df

    now = datetime.now(TZ).replace(tzinfo=None)
    cutoff_date = (now - pd.Timedelta(days=90)).date()

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

    h2h = pd.read_sql(sql, ENGINE, params={"cutoff_date": cutoff_date})
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
# FILTER REGISTRY
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
# BUTTON BAR
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

df_view = df[DISPLAY_COLS].copy() if not df.empty else pd.DataFrame(columns=DISPLAY_COLS)

# ============================================================
# CLEAN DISPLAY TYPES
# ============================================================

numeric_cols = [
    "Home", "Draw", "Away", "ComOpp", "SODD", "HCOSOD", "ACOSOD",
    "XGH", "XGA", "ESOTH", "ESOTA", "HomeWin%", "Draw%", "AwayWin%", "Value"
]

for col in numeric_cols:
    if col in df_view.columns:
        df_view[col] = pd.to_numeric(df_view[col], errors="coerce")

for col in df_view.columns:
    if col not in numeric_cols:
        df_view[col] = df_view[col].fillna("").astype(str)

st.markdown(f"**Fixtures ({len(df_view)})**")

# ============================================================
# TABLE
# ============================================================

st.dataframe(
    df_view,
    use_container_width=True,
    height=700,
)

st.caption(
    "Use the table toolbar to search, sort, and download if available. "
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
