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

TZ = ZoneInfo("Europe/London")

# ============================================================
# GLOBAL STYLING
# ============================================================

st.markdown(
    """
    <style>
    header[data-testid="stHeader"] { display: none; }

    html, body, .stApp {
        background-color: #0e1117;
        color: #e6e6e6;
    }

    .block-container {
        padding-top: 1.1rem !important;
        padding-bottom: 0rem;
        padding-left: 0.8rem;
        padding-right: 0.8rem;
    }

    h2 {
        margin-bottom: 0.25rem;
    }

    div.stButton > button {
        width: 100%;
        height: 2.8em;
        font-size: 0.95rem;
        font-weight: 650;
        border-radius: 8px;
        background-color: #111827;
        color: #e6e6e6;
        border: 1px solid #2a2f3a;
        padding: 0.25rem 0.35rem;
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

    @media (max-width: 768px) {
        .block-container {
            padding-left: 0.35rem !important;
            padding-right: 0.35rem !important;
            padding-top: 0.7rem !important;
        }

        div.stButton > button {
            font-size: 0.72rem;
            height: 2.55em;
            padding: 0.1rem 0.15rem;
        }

        h2 {
            font-size: 1.15rem !important;
        }

        p, div, span {
            font-size: 0.85rem;
        }
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# ============================================================
# SUPABASE / POSTGRES CONNECTION
# ============================================================

def _get_db_url() -> str:
    try:
        db_url = st.secrets.get("SUPABASE_DB_URL", "")
    except StreamlitSecretNotFoundError:
        st.error("Missing Streamlit Secrets. Set SUPABASE_DB_URL.")
        st.stop()

    if not isinstance(db_url, str) or not db_url.strip():
        st.error("SUPABASE_DB_URL is missing or empty.")
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
# LOAD FIXTURES
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
            value,
            "XConvH",
            "XConvA"
        FROM fixtures
        WHERE date >= CURRENT_DATE
        """
    )

    df = pd.read_sql(sql, ENGINE)

    df = df.rename(columns={
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
        "XConvH": "XConvH",
        "XConvA": "XConvA",
        "xconvh": "XConvH",
        "xconva": "XConvA",
    })

    df["KickoffDT"] = pd.to_datetime(
        df["Date"].astype(str) + " " + df["Kickoff"].astype(str),
        errors="coerce",
    )

    return df


# ============================================================
# LOAD STANDINGS FIELDS
# ============================================================

@st.cache_data(ttl=60)
def load_team_standing_fields() -> pd.DataFrame:
    sql = text("""
        SELECT
            "League",
            "TeamName",
            "StandingPosition",
            "StandingPPG",
            "StandingGames"
        FROM list_of_teams
        WHERE
            "StandingGames" > 0
            AND "StandingPosition" > 0
            AND "StandingPPG" IS NOT NULL
    """)

    try:
        teams = pd.read_sql(sql, ENGINE)
    except Exception:
        return pd.DataFrame()

    if teams.empty:
        return teams

    teams["League"] = teams["League"].fillna("").astype(str).str.strip()
    teams["TeamName"] = teams["TeamName"].fillna("").astype(str).str.strip()

    for col in ["StandingPosition", "StandingPPG", "StandingGames"]:
        teams[col] = pd.to_numeric(teams[col], errors="coerce")

    teams = teams.dropna(subset=[
        "League",
        "TeamName",
        "StandingPosition",
        "StandingPPG",
        "StandingGames",
    ])

    return teams


def add_standing_fields(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    df = df.copy()

    for col in ["Home St.Pos", "Away St.Pos", "Home St.PPG", "Away St.PPG", "Home St.Games", "Away St.Games"]:
        if col not in df.columns:
            df[col] = pd.NA

    teams = load_team_standing_fields()

    if teams.empty:
        return df

    df["League"] = df["League"].fillna("").astype(str).str.strip()
    df["HomeTeam"] = df["HomeTeam"].fillna("").astype(str).str.strip()
    df["AwayTeam"] = df["AwayTeam"].fillna("").astype(str).str.strip()

    home_lookup = teams.rename(columns={
        "TeamName": "HomeTeam",
        "StandingPosition": "Home St.Pos",
        "StandingPPG": "Home St.PPG",
        "StandingGames": "Home St.Games",
    })

    df = df.drop(columns=["Home St.Pos", "Home St.PPG", "Home St.Games"], errors="ignore")

    df = df.merge(
        home_lookup[["League", "HomeTeam", "Home St.Pos", "Home St.PPG", "Home St.Games"]],
        on=["League", "HomeTeam"],
        how="left",
    )

    away_lookup = teams.rename(columns={
        "TeamName": "AwayTeam",
        "StandingPosition": "Away St.Pos",
        "StandingPPG": "Away St.PPG",
        "StandingGames": "Away St.Games",
    })

    df = df.drop(columns=["Away St.Pos", "Away St.PPG", "Away St.Games"], errors="ignore")

    df = df.merge(
        away_lookup[["League", "AwayTeam", "Away St.Pos", "Away St.PPG", "Away St.Games"]],
        on=["League", "AwayTeam"],
        how="left",
    )

    for col in ["Home St.Pos", "Away St.Pos", "Home St.PPG", "Away St.PPG", "Home St.Games", "Away St.Games"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["Home St.PPG"] = df["Home St.PPG"].round(2)
    df["Away St.PPG"] = df["Away St.PPG"].round(2)

    return df


# ============================================================
# GLOBAL FILTERS
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

    adv_home = df["SODD"] > 0
    adv_away = df["SODD"] < 0

    adv_odds = pd.Series(index=df.index, dtype="float64")
    adv_odds.loc[adv_home] = df.loc[adv_home, "Home"]
    adv_odds.loc[adv_away] = df.loc[adv_away, "Away"]

    s_abs = df["SODD"].abs()

    required_odds = ODDS0 + (ODDS1 - ODDS0) * (s_abs - S0) / (S1 - S0)
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

    adv_home = df["SODD"] > 0
    adv_away = df["SODD"] < 0

    cosod_adv = pd.Series(index=df.index, dtype="float64")
    cosod_weak = pd.Series(index=df.index, dtype="float64")

    cosod_adv.loc[adv_home] = df.loc[adv_home, "HCOSOD"]
    cosod_weak.loc[adv_home] = df.loc[adv_home, "ACOSOD"]
    cosod_adv.loc[adv_away] = df.loc[adv_away, "ACOSOD"]
    cosod_weak.loc[adv_away] = df.loc[adv_away, "HCOSOD"]

    df = df[(cosod_adv > 1) & (cosod_weak < -1)].copy()
    if df.empty:
        return df

    adv_home = df["SODD"] > 0
    adv_away = df["SODD"] < 0

    adv_odds = pd.Series(index=df.index, dtype="float64")
    adv_odds.loc[adv_home] = df.loc[adv_home, "Home"]
    adv_odds.loc[adv_away] = df.loc[adv_away, "Away"]

    s_abs = df["SODD"].abs()

    required_odds = ODDS0 + (ODDS1 - ODDS0) * (s_abs - S0) / (S1 - S0)
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

    D0 = 3.0
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

    required_odds = ODDS0 + (ODDS1 - ODDS0) * (D_abs - D0) / (D1 - D0)
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
            "AwayShotsOn"
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

    h2h["PairKey"] = h2h.apply(
        lambda r: make_pair_key(r["HomeTeam"], r["AwayTeam"]),
        axis=1,
    )

    h2h_latest = (
        h2h.sort_values("Date", ascending=False)
           .drop_duplicates("PairKey", keep="first")
           .copy()
    )

    df = df.copy()

    df["PairKey"] = df.apply(
        lambda r: make_pair_key(r["HomeTeam"], r["AwayTeam"]),
        axis=1,
    )

    df = df.merge(
        h2h_latest[[
            "PairKey",
            "HomeTeam",
            "AwayTeam",
            "HomeShotsOn",
            "AwayShotsOn",
            "Date",
        ]].rename(columns={
            "HomeTeam": "H2H_HomeTeam",
            "AwayTeam": "H2H_AwayTeam",
            "HomeShotsOn": "H2H_HomeShotsOn",
            "AwayShotsOn": "H2H_AwayShotsOn",
            "Date": "H2H_Date",
        }),
        on="PairKey",
        how="inner",
    )

    if df.empty:
        return df

    df["HigherOddsSide"] = None
    df.loc[df["Home"] > df["Away"], "HigherOddsSide"] = "Home"
    df.loc[df["Away"] > df["Home"], "HigherOddsSide"] = "Away"

    df = df[df["HigherOddsSide"].notna()].copy()

    if df.empty:
        return df

    def higher_odds_team_had_2x_sot(row) -> bool:
        high_team = row["HomeTeam"] if row["HigherOddsSide"] == "Home" else row["AwayTeam"]
        low_team = row["AwayTeam"] if row["HigherOddsSide"] == "Home" else row["HomeTeam"]

        if row["H2H_HomeTeam"] == high_team and row["H2H_AwayTeam"] == low_team:
            high_sot = row["H2H_HomeShotsOn"]
            low_sot = row["H2H_AwayShotsOn"]
        elif row["H2H_AwayTeam"] == high_team and row["H2H_HomeTeam"] == low_team:
            high_sot = row["H2H_AwayShotsOn"]
            low_sot = row["H2H_HomeShotsOn"]
        else:
            return False

        return high_sot > (2 * low_sot)

    df = df[df.apply(higher_odds_team_had_2x_sot, axis=1)].copy()

    if df.empty:
        return df

    df["H2H_HigherOddsSide"] = df["HigherOddsSide"]
    df["H2H_Date"] = pd.to_datetime(df["H2H_Date"], errors="coerce").dt.strftime("%d/%m/%Y")

    return df

def filter_league_table(df: pd.DataFrame) -> pd.DataFrame:
    """
    League Table filter:
    Shows fixtures where the valued side has:
    - at least 2 league places advantage
    - at least 1.10x the opponent's PPG
    - higher odds than the opponent
    - SODD in their favour:
        Home = positive SODD
        Away = negative SODD
    """

    if df.empty:
        return df

    df = df.copy()

    required = [
        "Home", "Away", "SODD",
        "Home St.Pos", "Away St.Pos",
        "Home St.PPG", "Away St.PPG",
        "Home St.Games", "Away St.Games",
    ]

    for col in required:
        if col not in df.columns:
            df[col] = pd.NA
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=required)

    if df.empty:
        return df

    MIN_GAMES = 5
    MIN_POSITION_GAP = 2
    MIN_PPG_RATIO = 1.10

    df = df[
        (df["Home St.Games"] >= MIN_GAMES) &
        (df["Away St.Games"] >= MIN_GAMES) &
        (df["Home St.PPG"] > 0) &
        (df["Away St.PPG"] > 0)
    ].copy()

    if df.empty:
        return df

    home_edge = (
        ((df["Away St.Pos"] - df["Home St.Pos"]) >= MIN_POSITION_GAP) &
        (df["Home St.PPG"] >= df["Away St.PPG"] * MIN_PPG_RATIO) &
        (df["Home"] > df["Away"]) &
        (df["SODD"] > 0)
    )

    away_edge = (
        ((df["Home St.Pos"] - df["Away St.Pos"]) >= MIN_POSITION_GAP) &
        (df["Away St.PPG"] >= df["Home St.PPG"] * MIN_PPG_RATIO) &
        (df["Away"] > df["Home"]) &
        (df["SODD"] < 0)
    )

    df = df[home_edge | away_edge].copy()

    if df.empty:
        return df

    df["LeagueTableSide"] = ""
    df.loc[home_edge, "LeagueTableSide"] = "Home"
    df.loc[away_edge, "LeagueTableSide"] = "Away"

    df["PositionGap"] = (
        df[["Home St.Pos", "Away St.Pos"]].max(axis=1)
        - df[["Home St.Pos", "Away St.Pos"]].min(axis=1)
    )

    df["PPGGap"] = (df["Home St.PPG"] - df["Away St.PPG"]).round(2)

    df["PPGRatio"] = (
        df[["Home St.PPG", "Away St.PPG"]].max(axis=1)
        / df[["Home St.PPG", "Away St.PPG"]].min(axis=1)
    ).round(2)

    return df

# ============================================================
# FILTER REGISTRY
# ============================================================

FILTERS = [
    ("ALL", "All", filter_all),
    ("SODD", "SODD", filter_sodd),
    ("SCOSOD", "SODD+COSOD", filter_sodd_cosod),
    ("XG", "xG/xSOT", filter_xg_xsot),
    ("XWIN", "XWin%", filter_xwin_percent),
    ("H2H", "H2H", filter_head_to_head),
    ("LEAGUE", "League", filter_league_table),
]

# ============================================================
# HEADER + BUTTONS
# ============================================================

st.markdown("## Pre-Game Finder")

if "active_filter" not in st.session_state:
    st.session_state.active_filter = "ALL"

cols = st.columns([1, 1, 1.35, 1, 1, 1, 1])

for i, (key, label, _) in enumerate(FILTERS):
    with cols[i]:
        if st.button(label):
            st.session_state.active_filter = key

# ============================================================
# PIPELINE
# ============================================================

df = apply_global_filters(load_fixtures())

# Adds these fields to every fixture before filters:
# Home St.Pos, Away St.Pos, Home St.PPG, Away St.PPG
df = add_standing_fields(df)

active_key = st.session_state.active_filter
active_fn = {k: fn for (k, _, fn) in FILTERS}[active_key]
df = active_fn(df)

if not df.empty:
    df["Date"] = df["KickoffDT"].dt.strftime("%d/%m")
    df["Kickoff"] = df["KickoffDT"].dt.strftime("%H:%M")

# ============================================================
# DISPLAY
# ============================================================

DISPLAY_COLS = [
    "EventID",
    "HomeTeam", "AwayTeam", "League",
    "Date", "Kickoff",
    "Home", "Draw", "Away",
    "ComOpp",
    "SODD", "HCOSOD", "ACOSOD",
    "Home St.Pos", "Away St.Pos",
    "Home St.PPG", "Away St.PPG",
    "XGH", "XGA", "ESOTH", "ESOTA",
    "XConvH", "XConvA",
    "HomeWin%", "Draw%", "AwayWin%",
]

if not df.empty:
    df_view = df.reindex(columns=DISPLAY_COLS).copy()
else:
    df_view = pd.DataFrame(columns=DISPLAY_COLS)
    
numeric_cols = [
    "Home", "Draw", "Away",
    "Home St.Pos", "Away St.Pos",
    "Home St.PPG", "Away St.PPG",
    "ComOpp", "SODD", "HCOSOD", "ACOSOD",
    "XGH", "XGA", "ESOTH", "ESOTA",
    "XConvH", "XConvA",
    "HomeWin%", "Draw%", "AwayWin%",
]

for col in numeric_cols:
    if col in df_view.columns:
        df_view[col] = pd.to_numeric(df_view[col], errors="coerce")

for col in df_view.columns:
    if col not in numeric_cols:
        df_view[col] = df_view[col].fillna("").astype(str)

st.markdown(f"**{active_key} Fixtures ({len(df_view)})**")

# ============================================================
# MOBILE-FRIENDLY TABLE
# ============================================================

st.dataframe(
    df_view,
    use_container_width=True,
    height=620,
    hide_index=True,
)

st.caption(
    "Pre-Game Finder - Supabase-backed - public read-only dashboard."
)

# ============================================================
# EXPORT
# ============================================================

csv_bytes = df_view.to_csv(index=False).encode("utf-8")

st.download_button(
    label="Export CSV",
    data=csv_bytes,
    file_name=f"pre_game_finder_{active_key.lower()}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
    mime="text/csv",
)
