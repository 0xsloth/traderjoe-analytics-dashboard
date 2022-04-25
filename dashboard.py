from typing import List, Literal
from decimal import Decimal
import altair as alt
import itables
import pandas as pd
import streamlit as st
import streamlit.components.v1 as components
from data_getter import (
    to_sjoe_users_df,
    to_vejoe_users_df,
    to_rjoe_users_df,
    to_vejoe_day_snapshots_df,
    to_sjoe_day_snapshots_df,
    to_rjoe_day_snapshots_df,
    to_vejoe_wars_df,
    data_gathering_loop,
    vejoe_wars,
)
from utils import load_json


import threading
def my_threaded_func():
    data_gathering_loop()

thread = threading.Thread(target=my_threaded_func)
thread.daemon = True
thread.start()


@st.cache(ttl=180)
def get_vejoe_wars() -> pd.DataFrame:
    vejoe_wars_raw = load_json("jsons/vejoe_wars.json")
    df = to_vejoe_wars_df(vejoe_wars_raw)
    return df


@st.cache(ttl=180)
def get_sjoe_users_df(is_minimal: bool = True) -> pd.DataFrame:
    # sjoe_users = sjoe_get_all_users()
    sjoe_users = load_json("jsons/sjoe_get_all_users.json")

    df_sjoe_users = to_sjoe_users_df(sjoe_users)
    if is_minimal:
        df_sjoe_users.drop(columns=["sJOE.total_JOE_deposit_fee", "sJOE.deposit_count", "sJOE.withdraw_count", "sJOE.claim_count"], inplace=True)
    return df_sjoe_users


@st.cache(ttl=180)
def get_vejoe_users_df(is_minimal: bool = True) -> pd.DataFrame:
    # vejoe_users = vejoe_get_all_users()
    vejoe_users = load_json("jsons/vejoe_get_all_users.json")

    df_vejoe_users = to_vejoe_users_df(vejoe_users)
    if is_minimal:
        df_vejoe_users.drop(columns=["veJOE.deposit_count", "veJOE.withdraw_count", "veJOE.claim_count"], inplace=True)
    return df_vejoe_users


@st.cache(ttl=180)
def get_rjoe_users_df(is_minimal: bool = True) -> pd.DataFrame:
    # rjoe_users = rjoe_get_all_users()
    rjoe_users = load_json("jsons/rjoe_get_all_users.json")

    df_rjoe_users = to_rjoe_users_df(rjoe_users)
    if is_minimal:
        df_rjoe_users.drop(columns=["rJOE.deposit_count", "rJOE.withdraw_count"], inplace=True)
        df_rjoe_users.drop(columns=["rJOE.rJOE_balance"], inplace=True)
    return df_rjoe_users


@st.cache(ttl=180)
def get_vejoe_day_snapshots_df(is_minimal: bool = True) -> pd.DataFrame:
    # vejoe_day_snapshots = vejoe_get_all_day_snapshots()
    vejoe_day_snapshots = load_json("jsons/vejoe_get_all_day_snapshots.json")

    df_vejoe_day_snapshots = to_vejoe_day_snapshots_df(vejoe_day_snapshots)
    if is_minimal:
        df_vejoe_day_snapshots.drop(columns=["veJOE.deposit_count", "veJOE.withdraw_count", "veJOE.claim_count"], inplace=True)
    return df_vejoe_day_snapshots


@st.cache(ttl=180)
def get_sjoe_day_snapshots_df(is_minimal: bool = True) -> pd.DataFrame:
    # sjoe_day_snapshots = sjoe_get_all_day_snapshots()
    sjoe_day_snapshots = load_json("jsons/sjoe_get_all_day_snapshots.json")

    df_sjoe_day_snapshots = to_sjoe_day_snapshots_df(sjoe_day_snapshots)
    if is_minimal:
        df_sjoe_day_snapshots.drop(columns=["sJOE.deposit_count", "sJOE.withdraw_count", "sJOE.claim_count", "sJOE.emergency_withdraw_count"], inplace=True)
    return df_sjoe_day_snapshots


@st.cache(ttl=180)
def get_rjoe_day_snapshots_df(is_minimal: bool = True) -> pd.DataFrame:
    # rjoe_day_snapshots = rjoe_get_all_day_snapshots()
    rjoe_day_snapshots = load_json("jsons/rjoe_get_all_day_snapshots.json")

    df_rjoe_day_snapshots = to_rjoe_day_snapshots_df(rjoe_day_snapshots)
    if is_minimal:
        df_rjoe_day_snapshots.drop(columns=["rJOE.deposit_count", "rJOE.withdraw_count"], inplace=True)
        df_rjoe_day_snapshots.drop(columns=["rJOE.total_rJOE_reward", "rJOE.change_rJOE_reward"], inplace=True)
    return df_rjoe_day_snapshots


@st.cache
def join_multiple_dfs(dfs: List[pd.DataFrame], how: Literal["left", "right", "inner", "outer",]) -> pd.DataFrame:
    assert len(dfs) >= 2, "pass at least 2 DataFrames"
    df = dfs.pop(0).copy()
    while True:
        df_other = dfs.pop(0).copy()
        df = df.join(df_other, how=how)
        if len(dfs) == 0:
            break
    return df


@st.cache
def make_datatable(
    dfs: List[pd.DataFrame],
    choices: List[bool],
) -> pd.DataFrame:
    assert len(dfs) == len(choices), "`dfs` and `choices` must have the same length"
    dfs_c = [
        df
        for df, choice in zip(dfs, choices)
        if choice
    ]

    if len(dfs_c) == 0:
        return pd.DataFrame()
    elif len(dfs_c) == 1:
        df_dashboard = dfs_c[0]
    else:
        df_dashboard = join_multiple_dfs(dfs_c, how="outer")

    df_dashboard = df_dashboard.applymap(lambda x: f"{x:.3f}".rstrip("0").rstrip("."), na_action="ignore")
    return df_dashboard


@st.cache
def make_users_datatable(
    dfs: List[pd.DataFrame],
    choices: List[bool],
) -> pd.DataFrame:
    datatable = make_datatable(dfs, choices).copy()
    datatable.fillna(Decimal("0"), inplace=True)
    datatable.columns = pd.MultiIndex.from_tuples([c.split(".", 1) for c in datatable.columns], names=["pool", "stats"])
    return datatable


@st.cache
def make_day_snapshots_datatable(
    dfs: List[pd.DataFrame],
    choices: List[bool],
) -> pd.DataFrame:
    datatable = make_datatable(dfs, choices).copy()
    datatable.reset_index(inplace=True)
    datatable = datatable.applymap(lambda x: float(x) if isinstance(x, Decimal) else x)
    return datatable


@st.cache
def query_day_snapshots_datatable(
    day_snapshots_datatable: pd.DataFrame,
    col_name: str,
) -> pd.DataFrame:
    cols = [
        c
        for c in day_snapshots_datatable.columns
        if c.endswith(col_name)
    ]

    dfs = []
    for col in cols:
        df = day_snapshots_datatable[["date", col]].copy()
        df.rename(columns={col: col_name}, inplace=True)
        df["pool"] = col.split(".", 1)[0]
        dfs.append(df)

    df = pd.concat(dfs)
    return df


@st.cache
def make_datatable_heading(
    labels: List[str],
    choices: List[bool],
) -> str:
    assert len(labels) == len(choices), "`labels` and `choices` must have the same length"
    labels_c = [
        label
        for label, choice in zip(labels, choices)
        if choice
    ]

    if len(labels_c) == 0:
        return "No Pool Selected"
    elif len(labels_c) == 1:
        label = labels_c[0]
        return f"{label} Pool"
    else:
        label = " & ".join(labels_c)
        return f"{label} Pools"


@st.cache(ttl=180)
def make_vejoe_wars_datatable() -> pd.DataFrame:
    from_address_to_platform = {
        "0xe7462905B79370389e8180E300F58f63D35B725F".lower(): "YieldYak",
        "0x1F2A8034f444dc55F963fb5925A9b6eb744EeE2c".lower(): "Beefy",
        "0xF30E775240D4137daEa097109FEA882C406D61cc".lower(): "NorthPole",
        "0x0E25c07748f727D6CCcD7D2711fD7bD13d13422d".lower(): "Vector",
    }

    datatable = vejoe_wars()
    datatable["address"] = datatable["address"].map(lambda x: from_address_to_platform.get(x, x))
    datatable = datatable.applymap(lambda x: f"{x:.3f}".rstrip("0").rstrip(".") if isinstance(x, Decimal) else x, na_action="ignore")
    datatable.set_index("address", inplace=True)
    return datatable


st.set_page_config(layout="wide")

st.title("TraderJoe Analytics")
# st.image("JOE Logo SVG.svg", caption=None, width=None, use_column_width=None, clamp=False, channels='RGB', output_format='auto')


# "with" notation
with st.sidebar:
    st.header("Content")
    st.markdown(
        """
        - [JOE Wars](#joe-wars)
            - [Platforms](#platforms)  
            - [Wallets](#wallets)  
            - [Platform Stats by Block Number](#platform-stats-by-block-number)  
        - [JOE Staking Pools](#joe-staking-pools)  
            - [Users](#users)  
            - [Daily Pool Snapshots](#daily-pool-snapshots)  
        """
    )

    st.subheader("Pools?")
    st.write("Select JOE staking pools you are interested in")
    is_vejoe_pool = st.checkbox("veJOE Pool", value=True)
    is_sjoe_pool = st.checkbox("sJOE Pool", value=True)
    is_rjoe_pool = st.checkbox("rJOE Pool", value=True)


df_sjoe_users = get_sjoe_users_df(is_minimal=True)
df_vejoe_users = get_vejoe_users_df(is_minimal=True)
df_rjoe_users = get_rjoe_users_df(is_minimal=True)


st.header("JOE Wars")
st.subheader("Platforms")
platforms_html = """
    <p>
        Platforms participating in JOE Wars
    </p>
    <p>
        <a>Beefy</a>
        <ul>
            <li><a href="https://snowtrace.io/address/0x1F2A8034f444dc55F963fb5925A9b6eb744EeE2c/">Wallet</a></li>
            <li><a href="https://beefy.finance/">Website</a></li>
            <li><a href="https://twitter.com/beefyfinance/">Twitter</a></li>
        </ul>
    </p>
    <p>
        <a>NorthPole</a>
        <ul>
            <li><a href="https://snowtrace.io/address/0xF30E775240D4137daEa097109FEA882C406D61cc/">Wallet</a></li>
            <li><a href="https://northpole.money/">Website</a></li>
            <li><a href="https://twitter.com/NorthPole_money/">Twitter</a></li>
        </ul>
    </p>
    <p>

        <a>Vector</a>
        <ul>
            <li><a href="https://snowtrace.io/address/0x0E25c07748f727D6CCcD7D2711fD7bD13d13422d/">Wallet</a></li>
            <li><a href="https://vectorfinance.io/">Website</a></li>
            <li><a href="https://twitter.com/vector_fi/">Twitter</a></li>
        </ul>
    </p>
    <p>
        <a>Yield Yak</a>
        <ul>
            <li><a href="https://snowtrace.io/address/0xe7462905B79370389e8180E300F58f63D35B725F/">Wallet</a></li>
            <li><a href="https://yieldyak.com/">Website</a></li>
            <li><a href="https://twitter.com/yieldyak_/">Twitter</a></li>
        </ul>
    </p>

"""
components.html(
    platforms_html,
    height=500,
)



st.subheader("Wallets")

# st.write("[Beefy](https://beefy.finance/)")

# st.write("[NorthPole](https://northpole.money/")

# st.write("[Vector](https://vectorfinance.io/")

# st.write("[YieldYak](https://yieldyak.com/")

vejoe_wars_datatable = make_vejoe_wars_datatable()


components.html(
    itables.javascript._datatables_repr_(
        vejoe_wars_datatable,
        maxBytes=0,
        classes=["cell-border", "hover", "order-column", "stripe"],
        columnDefs=[
            # {"className": "dt-head-center", "targets": "_all"},
            {"className": "dt-body-right", "targets": list(range(1, len(vejoe_wars_datatable.columns)+1))},
            {"className": "dt-body-left", "targets": 0},
        ],
    ),
    height=600,
    scrolling=True,
)


df_vejoe_wars = get_vejoe_wars()

st.subheader("Platform Stats by Block Number")

all_stake = alt.Chart(df_vejoe_wars).mark_line().encode(
    x=alt.X("block_number:Q", title="Block Number"),
    y=alt.Y("total_stake:Q", title="Staked JOE"),
    color="platform:N",
).properties(
    title="Staked JOE in veJOE Pool: Total + Platforms",
)
st.altair_chart(all_stake, use_container_width=True)

platforms_stake = alt.Chart(df_vejoe_wars[df_vejoe_wars["platform"] != "Pool"]).mark_line().encode(
    x=alt.X("block_number:Q", title="Block Number"),
    y=alt.Y("total_stake:Q", title="Staked JOE"),
    color="platform:N",
).properties(
    title="Staked JOE in veJOE: Platforms",
)
st.altair_chart(platforms_stake, use_container_width=True)


all_reward = alt.Chart(df_vejoe_wars).mark_line().encode(
    x=alt.X("block_number:Q", title="Block Number"),
    y=alt.Y("total_reward:Q", title="Accrued veJOE"),
    color="platform:N",
).properties(
    title="Accrued veJOE in veJOE: Total + Platforms",
)
st.altair_chart(all_reward, use_container_width=True)

platforms_reward = alt.Chart(df_vejoe_wars[df_vejoe_wars["platform"] != "Pool"]).mark_line().encode(
    x=alt.X("block_number:Q", title="Block Number"),
    y=alt.Y("total_reward:Q", title="Accrued veJOE"),
    color="platform:N",
).properties(
    title="Accrued veJOE in veJOE: Platforms",
)
st.altair_chart(platforms_reward, use_container_width=True)


st.header("JOE Staking Pools")
st.subheader("Users")


users_datatable_heading = make_datatable_heading(
    labels=[
        "veJOE",
        "sJOE",
        "rJOE",
    ],
    choices=[
        is_vejoe_pool,
        is_sjoe_pool,
        is_rjoe_pool,
    ]
)
users_datatable = make_users_datatable(
    dfs=[
        df_vejoe_users,
        df_sjoe_users,
        df_rjoe_users,
    ],
    choices=[
        is_vejoe_pool,
        is_sjoe_pool,
        is_rjoe_pool,
    ]
)


st.write(f"Showing: {users_datatable_heading}")

components.html(
    itables.javascript._datatables_repr_(
        users_datatable,
        maxBytes=0,
        classes=["cell-border", "hover", "nowrap", "order-column", "stripe"],
        columnDefs=[
            # {"className": "dt-head-center", "targets": "_all"},
            {"className": "dt-body-right", "targets": list(range(1, len(users_datatable.columns)+1))},
            {"className": "dt-body-left", "targets": 0},
        ],
    ),
    height=600,
    scrolling=True,
)


st.subheader("Daily Pool Snapshots")


df_sjoe_day_snapshots = get_sjoe_day_snapshots_df(is_minimal=True)
df_vejoe_day_snapshots = get_vejoe_day_snapshots_df(is_minimal=True)
df_rjoe_day_snapshots = get_rjoe_day_snapshots_df(is_minimal=True)


day_snapshots_datatable_heading = make_datatable_heading(
    labels=[
        "veJOE",
        "sJOE",
        "rJOE",
    ],
    choices=[
        is_vejoe_pool,
        is_sjoe_pool,
        is_rjoe_pool,
    ]
)
day_snapshots_datatable = make_day_snapshots_datatable(
    dfs=[
        df_vejoe_day_snapshots,
        df_sjoe_day_snapshots,
        df_rjoe_day_snapshots,
    ],
    choices=[
        is_vejoe_pool,
        is_sjoe_pool,
        is_rjoe_pool,
    ]
)


st.write(f"Showing: {day_snapshots_datatable_heading}")
st.write("Total User Count = Number of distinct addresses which deposited JOE to the pool.")
st.write("Active User Count = Number of distinct addresses which currently has positive JOE stake in the pool.")


def make_altair_chart(df: pd.DataFrame, yaxis_col_name: str, yaxis_title: str) -> alt.Chart:
    highlight = alt.selection(type="single", on="mouseover", fields=["pool"], nearest=True)
    base = alt.Chart(df).encode(
        x=alt.X("date:T", axis=alt.Axis(title="Date", format="%m-%d")),
        y=alt.Y(f"{yaxis_col_name}:Q", title=yaxis_title),
        color=alt.Color("pool:N", title="Pool"),
    ).properties(
        title=f"JOE Staking Pools: {yaxis_title}",
    )
    points = base.mark_circle().encode(opacity=alt.value(0)).add_selection(highlight)
    lines = base.mark_line().encode(
        size=alt.condition(~highlight, alt.value(1), alt.value(3))
    )
    chart = points + lines
    return chart


df_day_snapshots_total_joe_stake = query_day_snapshots_datatable(day_snapshots_datatable, col_name="total_JOE_stake")
df_day_snapshots_total_user_count = query_day_snapshots_datatable(day_snapshots_datatable, col_name="total_user_count")
df_day_snapshots_active_user_count = query_day_snapshots_datatable(day_snapshots_datatable, col_name="active_user_count")


st.altair_chart(make_altair_chart(df_day_snapshots_total_joe_stake, "total_JOE_stake", yaxis_title="Staked JOE"), use_container_width=True)
st.altair_chart(make_altair_chart(df_day_snapshots_total_user_count, "total_user_count", yaxis_title="Total User Count"), use_container_width=True)
st.altair_chart(make_altair_chart(df_day_snapshots_active_user_count, "active_user_count", yaxis_title="Active User Count"), use_container_width=True)
