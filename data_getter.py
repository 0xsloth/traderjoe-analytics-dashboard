from typing import Any, Dict, List, Optional
from decimal import Decimal
import os
import time
from gql import Client, gql
from gql.transport.requests import RequestsHTTPTransport
import pandas as pd
from utils import dump_json, load_json


SJOE_URL = "https://api.thegraph.com/subgraphs/name/0xsloth/sjoe-stake"
VEJOE_URL = "https://api.thegraph.com/subgraphs/name/0xsloth/vejoe-stake"
RJOE_URL = "https://api.thegraph.com/subgraphs/name/0xsloth/rjoe-stake"
VEJOE_BOOSTED_POOLS_URL = "https://api.thegraph.com/subgraphs/id/QmSJLBynLd1kzC2cLSPvLg4G5NGFZ1UCHztuee6FqUFQay"

JOE_DECIMALS = Decimal("18")
VEJOE_DECIMALS = Decimal("18")
RJOE_DECIMALS = Decimal("18")
USDC_DECIMALS = Decimal("6")


# Select your transport with a defined url endpoint
sjoe_transport = RequestsHTTPTransport(url=SJOE_URL)
vejoe_transport = RequestsHTTPTransport(url=VEJOE_URL)
vejoe_boosted_pools_transport = RequestsHTTPTransport(url=VEJOE_BOOSTED_POOLS_URL)
vejoe_boosted_pools_transport.connect()
rjoe_transport = RequestsHTTPTransport(url=RJOE_URL)

# Create a GraphQL client using the defined transport
sjoe_client = Client(transport=sjoe_transport, fetch_schema_from_transport=True)
vejoe_client = Client(transport=vejoe_transport, fetch_schema_from_transport=True)
rjoe_client = Client(transport=rjoe_transport, fetch_schema_from_transport=True)


def vejoe_wars() -> pd.DataFrame:
    joe_per_sec = Decimal("1833719582850521436") / Decimal("10") ** JOE_DECIMALS
    num_secs_in_day = Decimal(60 * 60 * 24)
    vejoe_users = load_json("jsons/vejoe_get_all_users.json")
    vejoe_users_boosted_pools = load_json("jsons/vejoe_get_all_users_boosted_pool_positions.json")
    df_vejoe_users_boosted_pools = to_vejoe_users_boosted_pools_df(vejoe_users_boosted_pools)
    df_vejoe_users = to_vejoe_users_df(vejoe_users)
    df = df_vejoe_users_boosted_pools.join(df_vejoe_users, how="outer")
    df["user_factor"] = (df["veJOE.veJOE_balance"] * df["user_lp_amount"]).fillna(Decimal("0")) ** (Decimal("0.5"))
    df["total_factor"] = df["user_factor"].groupby(df["pid"]).transform("sum")
    df["user_factor_ratio"] = df["user_factor"] / df["total_factor"]
    df["user_lp_ratio"] = df["user_lp_amount"] / df["total_lp_amount"]
    df["total_alloc_point"] = df.drop_duplicates(subset=["pid"])["alloc_point"].sum()
    df["boost_joe_per_sec"] = joe_per_sec * (df["alloc_point"] / df["total_alloc_point"]) * (df["veJOE_share_bp"] / Decimal("10000")) * df["user_factor_ratio"]
    df["base_joe_per_sec"] = joe_per_sec * (df["alloc_point"] / df["total_alloc_point"]) * ((Decimal("10000") - df["veJOE_share_bp"]) / Decimal("10000")) * df["user_lp_ratio"]
    df["pool_joe_per_sec"] = (df["base_joe_per_sec"] + df["boost_joe_per_sec"])
    df["user_joe_per_sec"] = df["pool_joe_per_sec"].groupby(df.index).transform("sum")
    df["user_joe_per_day"] = df["user_joe_per_sec"] * num_secs_in_day
    df.reset_index(inplace=True)
    df.drop_duplicates("address", inplace=True)
    df = df[["address", "veJOE.total_JOE_stake", "veJOE.veJOE_balance", "user_joe_per_day"]].fillna(Decimal("0"))
    df.rename(columns={"veJOE.total_JOE_stake": "JOE Stake", "veJOE.veJOE_balance": "veJOE Balance", "user_joe_per_day": "Daily JOE Reward"}, inplace=True)
    df["JOE Stake Rank"] = df["JOE Stake"].rank(method ="min", ascending=False)
    df["veJOE Balance Rank"] = df["veJOE Balance"].rank(method ="min", ascending=False)
    df["Daily JOE Reward Rank"] = df["Daily JOE Reward"].rank(method ="min", ascending=False)
    df["JOE Stake Percentage"] = (df["JOE Stake"] / df["JOE Stake"].sum()).apply(lambda x: f"{x:.3%}")
    df["veJOE Balance Percentage"] = (df["veJOE Balance"] / df["veJOE Balance"].sum()).apply(lambda x: f"{x:.3%}")
    df["Daily JOE Reward Percentage"] = (df["Daily JOE Reward"] / df["Daily JOE Reward"].sum()).apply(lambda x: f"{x:.3%}")
    return df


def vejoe_wars_at_multiple_block_numbers(
    min_block_number: int,
    max_block_number: int,
    step_block_number: int,
) -> List[Dict[str, Any]]:
    datas = []
    for block_number in range(min_block_number, max_block_number, step_block_number):
        datas.append(
            vejoe_wars_at_block_number(block_number)
        )
    
    return datas


def vejoe_wars_at_block_number(block_number: int) -> Dict[str, Any]:
    platforms = vejoe_get_platforms_at_block_number(block_number)
    pool = vejoe_get_pool_at_block_number(block_number)
    data = {
        **platforms,
        "Pool": {
            "block_number": block_number,
            "platform": "Pool",
            "address": None,
            "user": pool["pool"],
        },
    }
    return data


def vejoe_get_platforms_at_block_number(block_number: int) -> Dict[str, Any]:
    from_platform_to_address = {
        "YieldYak": "0xe7462905B79370389e8180E300F58f63D35B725F".lower(),
        "Beefy": "0x1F2A8034f444dc55F963fb5925A9b6eb744EeE2c".lower(),
        "NorthPole": "0xF30E775240D4137daEa097109FEA882C406D61cc".lower(),
        "Vector": "0x0E25c07748f727D6CCcD7D2711fD7bD13d13422d".lower(),
    }

    return {
        platform: {
            "block_number": block_number,
            "platform": platform,
            "address": address,
            **vejoe_get_user_at_block_number(address, block_number),
        }
        for platform, address in from_platform_to_address.items()
    }


def vejoe_get_pool_at_block_number(block_number: int) -> Dict[str, Any]:
    str_query = (
        """
        query getPool($address: ID!, $blockNumber: Int!) {
            pool(id: $address, block: {number: $blockNumber}) {
                id
                totalStake
                totalReward
            }
        }
        """
    )

    # Provide a GraphQL query
    query = gql(str_query)

    params = {
        "address": "0x25D85E17dD9e544F6E9F8D44F99602dbF5a97341",
        "blockNumber": block_number,
    }

    # Execute the query on the transport
    result = vejoe_client.execute(query, variable_values=params)
    return result


def vejoe_get_user_at_block_number(address: str, block_number: int) -> Dict[str, Any]:
    str_query = (
        """
        query getUser($address: ID!, $blockNumber: Int!) {
            user(id: $address, block: {number: $blockNumber}) {
                id
                totalStake
                totalReward
            }
        }
        """
    )

    # Provide a GraphQL query
    query = gql(str_query)

    params = {
        "address": address,
        "blockNumber": block_number,
    }

    # Execute the query on the transport
    result = vejoe_client.execute(query, variable_values=params)
    return result


def vejoe_get_users_boosted_pool_positions(last_id: Optional[str] = None) -> Dict[str, Any]:
    str_query_without_param = (
        """
        query getUsersBoostedPoolPositions {
            users(first: 1000, orderBy: id, orderDirection: asc) {
                id
                boostedPoolPositions(first: 1000) {
                boostedPool {
                    id
                    lpToken
                    allocPoint
                    veJoeShareBp
                    totalAmount
                }
                totalAmount
                }
            }
        }
        """
    )

    str_query_with_param = (
        """
        query getUsersBoostedPoolPositions($lastID: ID) {
            users(first: 1000, orderBy: id, orderDirection: asc, where: { id_gt: $lastID  }) {
                id
                boostedPoolPositions(first: 1000) {
                boostedPool {
                    id
                    lpToken
                    allocPoint
                    veJoeShareBp
                    totalAmount
                }
                totalAmount
                }
            }
        }
        """
    )

    str_query = str_query_with_param if last_id else str_query_without_param


    # Provide a GraphQL query
    query = gql(str_query)

    params = {
        "lastID": last_id,
    }

    # Execute the query on the transport
    result = vejoe_boosted_pools_transport.execute(query, variable_values=params)
    return result.data


def vejoe_get_all_users_boosted_pool_positions() -> List[Dict[str, Any]]:
    last_id = None

    all_users = []
    while True:
        data = vejoe_get_users_boosted_pool_positions(last_id)
        users = data["users"]

        if len(users) == 0:
            break

        last_id = users[-1]["id"]
        all_users.extend(users)
    
    return all_users


def vejoe_get_users(last_id: Optional[str] = None) -> Dict[str, Any]:
    str_query_without_param = (
        """
        query getUsers {
            users(first: 1000, orderBy: id, orderDirection: asc) {
                id
                totalStake
                totalReward
                depositCount
                withdrawCount
                claimCount
            }
        }
        """
    )

    str_query_with_param = (
        """
        query getUsers($lastID: ID) {
            users(first: 1000, orderBy: id, orderDirection: asc, where: { id_gt: $lastID  }) {
                id
                totalStake
                totalReward
                depositCount
                withdrawCount
                claimCount
            }
        }
        """
    )

    str_query = str_query_with_param if last_id else str_query_without_param


    # Provide a GraphQL query
    query = gql(str_query)

    params = {
        "lastID": last_id,
    }

    # Execute the query on the transport
    result = vejoe_client.execute(query, variable_values=params)
    return result


def vejoe_get_all_users() -> List[Dict[str, Any]]:
    last_id = None

    all_users = []
    while True:
        data = vejoe_get_users(last_id)
        users = data["users"]

        if len(users) == 0:
            break

        last_id = users[-1]["id"]
        all_users.extend(users)
    
    return all_users


def sjoe_get_users(last_id: Optional[str] = None) -> Dict[str, Any]:
    str_query_without_param = (
        """
        query getUsers {
            users(first: 1000, orderBy: id, orderDirection: asc) {
                id
                totalStake
                totalFee
                rewards {
                    rewardToken {
                        id
                        name
                        symbol
                        decimals
                    }
                    totalReward
                }
                depositCount
                withdrawCount
                claimCount
            }
        }
        """
    )

    str_query_with_param = (
        """
        query getUsers($lastID: ID) {
            users(first: 1000, orderBy: id, orderDirection: asc, where: { id_gt: $lastID  }) {
                id
                totalStake
                totalFee
                rewards {
                    rewardToken {
                        id
                        name
                        symbol
                        decimals
                    }
                    totalReward
                }
                depositCount
                withdrawCount
                claimCount
            }
        }
        """
    )

    str_query = str_query_with_param if last_id else str_query_without_param


    # Provide a GraphQL query
    query = gql(str_query)

    params = {
        "lastID": last_id,
    }

    # Execute the query on the transport
    result = sjoe_client.execute(query, variable_values=params)
    return result


def sjoe_get_all_users() -> List[Dict[str, Any]]:
    last_id = None

    all_users = []
    while True:
        data = sjoe_get_users(last_id)
        
        users = data["users"]

        if len(users) == 0:
            break

        last_id = users[-1]["id"]
        all_users.extend(users)
    
    return all_users


def rjoe_get_users(last_id: Optional[str] = None) -> Dict[str, Any]:
    str_query_without_param = (
        """
        query getUsers {
            users(first: 1000, orderBy: id, orderDirection: asc) {
                id
                totalStake
                totalReward
                depositCount
                withdrawCount
            }
        }
        """
    )

    str_query_with_param = (
        """
        query getUsers($lastID: ID) {
            users(first: 1000, orderBy: id, orderDirection: asc, where: { id_gt: $lastID  }) {
                id
                totalStake
                totalReward
                depositCount
                withdrawCount
            }
        }
        """
    )

    str_query = str_query_with_param if last_id else str_query_without_param


    # Provide a GraphQL query
    query = gql(str_query)

    params = {
        "lastID": last_id,
    }

    # Execute the query on the transport
    result = rjoe_client.execute(query, variable_values=params)
    return result


def rjoe_get_all_users() -> List[Dict[str, Any]]:
    last_id = None

    all_users = []
    while True:
        data = rjoe_get_users(last_id)
        users = data["users"]

        if len(users) == 0:
            break

        last_id = users[-1]["id"]
        all_users.extend(users)
    
    return all_users


def to_sjoe_users_df(sjoe_users: List[Dict[str, Any]]) -> pd.DataFrame:
    sjoe_users_parsed = (
        {
            "address": sjoe_user["id"],
            "sJOE.total_JOE_stake": Decimal(sjoe_user["totalStake"]),
            "sJOE.total_JOE_deposit_fee": Decimal(sjoe_user["totalFee"]),
            "sJOE.deposit_count": Decimal(sjoe_user["depositCount"]),
            "sJOE.withdraw_count": Decimal(sjoe_user["withdrawCount"]),
            "sJOE.claim_count": Decimal(sjoe_user["claimCount"]),
            "sJOE.total_rewards": {
                reward_dict["rewardToken"]["symbol"]: Decimal(reward_dict["totalReward"]) / Decimal("10") ** Decimal(reward_dict["rewardToken"]["decimals"])
                # reward_dict["rewardToken"]["id"]: Decimal(reward_dict["totalReward"])
                for reward_dict in sjoe_user["rewards"]
            }
        }
        for sjoe_user in sjoe_users
    )

    df_sjoe_users = pd.json_normalize(sjoe_users_parsed, sep=".")

    reward_cols = [c for c in df_sjoe_users.columns if c.startswith("sJOE.total_rewards")]
    df_sjoe_users[reward_cols] = df_sjoe_users[reward_cols].fillna(Decimal("0"))

    cols_in_terms_of_joe = [
        "sJOE.total_JOE_stake",
        "sJOE.total_JOE_deposit_fee",
    ]

    df_sjoe_users[cols_in_terms_of_joe] = df_sjoe_users[cols_in_terms_of_joe] / Decimal("10") ** JOE_DECIMALS
    df_sjoe_users.set_index(keys=["address"], inplace=True)

    return df_sjoe_users


def to_vejoe_users_boosted_pools_df(vejoe_users_boosted_pools: List[Dict[str, Any]]) -> pd.DataFrame:
    vejoe_users_boosted_pools_parsed = [
        {
            "address": user["id"],
            "lp_token": boosted_pool_position["boostedPool"]["lpToken"],
            "pid": int(boosted_pool_position["boostedPool"]["id"]),
            "veJOE_share_bp": Decimal(boosted_pool_position["boostedPool"]["veJoeShareBp"]),
            "alloc_point": Decimal(boosted_pool_position["boostedPool"]["allocPoint"]),
            "total_lp_amount": Decimal(boosted_pool_position["boostedPool"]["totalAmount"]),
            "user_lp_amount": Decimal(boosted_pool_position["totalAmount"]),
        }
        for user in vejoe_users_boosted_pools
        for boosted_pool_position in user["boostedPoolPositions"]
    ]

    df = pd.DataFrame(vejoe_users_boosted_pools_parsed)

    df.set_index(keys=["address"], inplace=True)

    return df


def to_vejoe_users_df(vejoe_users: List[Dict[str, Any]]) -> pd.DataFrame:
    vejoe_users_parsed = (
        {
            "address": vejoe_user["id"],
            "veJOE.total_JOE_stake": Decimal(vejoe_user["totalStake"]),
            "veJOE.veJOE_balance": Decimal(vejoe_user["totalReward"]),
            "veJOE.deposit_count": Decimal(vejoe_user["depositCount"]),
            "veJOE.withdraw_count": Decimal(vejoe_user["withdrawCount"]),
            "veJOE.claim_count": Decimal(vejoe_user["claimCount"]),
        }
        for vejoe_user in vejoe_users
    )

    df_vejoe_users = pd.json_normalize(vejoe_users_parsed, sep=".")

    cols_in_terms_of_joe = [
        "veJOE.total_JOE_stake",
    ]

    cols_in_terms_of_vejoe = [
        "veJOE.veJOE_balance",
    ]

    df_vejoe_users[cols_in_terms_of_joe] = df_vejoe_users[cols_in_terms_of_joe] / Decimal("10") ** JOE_DECIMALS
    df_vejoe_users[cols_in_terms_of_vejoe] = df_vejoe_users[cols_in_terms_of_vejoe] / Decimal("10") ** VEJOE_DECIMALS
    df_vejoe_users.set_index(keys=["address"], inplace=True)

    return df_vejoe_users


def to_rjoe_users_df(rjoe_users: List[Dict[str, Any]]) -> pd.DataFrame:
    rjoe_users_parsed = (
        {
            "address": rjoe_user["id"],
            "rJOE.total_JOE_stake": Decimal(rjoe_user["totalStake"]),
            "rJOE.rJOE_balance": Decimal(rjoe_user["totalReward"]),
            "rJOE.deposit_count": Decimal(rjoe_user["depositCount"]),
            "rJOE.withdraw_count": Decimal(rjoe_user["withdrawCount"]),
        }
        for rjoe_user in rjoe_users
    )

    df_rjoe_users = pd.json_normalize(rjoe_users_parsed, sep=".")

    cols_in_terms_of_joe = [
        "rJOE.total_JOE_stake",
    ]

    cols_in_terms_of_vejoe = [
        "rJOE.rJOE_balance",
    ]

    df_rjoe_users[cols_in_terms_of_joe] = df_rjoe_users[cols_in_terms_of_joe] / Decimal("10") ** JOE_DECIMALS
    df_rjoe_users[cols_in_terms_of_vejoe] = df_rjoe_users[cols_in_terms_of_vejoe] / Decimal("10") ** RJOE_DECIMALS
    df_rjoe_users.set_index(keys=["address"], inplace=True)

    return df_rjoe_users




def vejoe_get_day_snapshots(last_period_start_unix: Optional[int] = None) -> Dict[str, Any]:
    str_query_without_param = (
        """
        query getDaySnapshots {
            daySnapshots(first: 1000, orderBy: periodStartUnix, orderDirection: asc) {
                id
                periodStartUnix
                totalStake
                changeInStake
                totalReward
                changeInReward
                totalUserCount
                activeUserCount
                depositCount
                withdrawCount
                claimCount
            }
        }
        """
    )

    str_query_with_param = (
        """
        query getDaySnapshots($lastPeriodStartUnix: Int!) {
            daySnapshots(first: 1000, orderBy: periodStartUnix, orderDirection: asc, where: { periodStartUnix_gt: $lastPeriodStartUnix  }) {
                id
                periodStartUnix
                totalStake
                changeInStake
                totalReward
                changeInReward
                totalUserCount
                activeUserCount
                depositCount
                withdrawCount
                claimCount
            }
        }
        """
    )


    str_query = str_query_with_param if last_period_start_unix else str_query_without_param


    # Provide a GraphQL query
    query = gql(str_query)

    params = {
        "lastPeriodStartUnix": last_period_start_unix,
    }

    # Execute the query on the transport
    result = vejoe_client.execute(query, variable_values=params)
    return result


def vejoe_get_all_day_snapshots() -> List[Dict[str, Any]]:
    last_period_start_unix = None

    all_day_snapshots = []
    while True:
        data = vejoe_get_day_snapshots(last_period_start_unix)
        daySnapshots = data["daySnapshots"]

        if len(daySnapshots) == 0:
            break

        last_period_start_unix = daySnapshots[-1]["periodStartUnix"]
        all_day_snapshots.extend(daySnapshots)
    
    return all_day_snapshots


def sjoe_get_day_snapshots(last_period_start_unix: Optional[int] = None) -> Dict[str, Any]:
    str_query_without_param = (
        """
        query getDaySnapshots {
            daySnapshots(first: 1000, orderBy: periodStartUnix, orderDirection: asc) {
                id
                dayIndex
                periodStartUnix
                totalStake
                changeInStake
                totalFee
                changeInFee
                rewards {
                    rewardToken {
                        id
                        name
                        symbol
                        decimals
                    }
                    totalReward
                    changeInReward
                }
                totalUserCount
                activeUserCount
                depositCount
                withdrawCount
                emergencyWithdrawCount
                claimCount
            }
        }
        """
    )

    str_query_with_param = (
        """
        query getDaySnapshots($lastPeriodStartUnix: Int!) {
            daySnapshots(first: 1000, orderBy: periodStartUnix, orderDirection: asc, where: { periodStartUnix_gt: $lastPeriodStartUnix  }) {
                id
                dayIndex
                periodStartUnix
                totalStake
                changeInStake
                totalFee
                changeInFee
                rewards {
                    rewardToken {
                        id
                        name
                        symbol
                        decimals
                    }
                    totalReward
                    changeInReward
                }
                totalUserCount
                activeUserCount
                depositCount
                withdrawCount
                emergencyWithdrawCount
                claimCount
            }
        }
        """
    )


    str_query = str_query_with_param if last_period_start_unix else str_query_without_param


    # Provide a GraphQL query
    query = gql(str_query)

    params = {
        "lastPeriodStartUnix": last_period_start_unix,
    }

    # Execute the query on the transport
    result = sjoe_client.execute(query, variable_values=params)
    return result


def sjoe_get_all_day_snapshots() -> List[Dict[str, Any]]:
    last_period_start_unix = None

    all_day_snapshots = []
    while True:
        data = sjoe_get_day_snapshots(last_period_start_unix)
        daySnapshots = data["daySnapshots"]

        if len(daySnapshots) == 0:
            break

        last_period_start_unix = daySnapshots[-1]["periodStartUnix"]
        all_day_snapshots.extend(daySnapshots)
    
    return all_day_snapshots


def rjoe_get_day_snapshots(last_period_start_unix: Optional[int] = None) -> Dict[str, Any]:
    str_query_without_param = (
        """
        query getDaySnapshots {
            daySnapshots(first: 1000, orderBy: periodStartUnix, orderDirection: asc) {
                id
                periodStartUnix
                totalStake
                changeInStake
                totalReward
                changeInReward
                totalUserCount
                activeUserCount
                depositCount
                withdrawCount
            }
        }
        """
    )

    str_query_with_param = (
        """
        query getDaySnapshots($lastPeriodStartUnix: Int!) {
            daySnapshots(first: 1000, orderBy: periodStartUnix, orderDirection: asc, where: { periodStartUnix_gt: $lastPeriodStartUnix  }) {
                id
                periodStartUnix
                totalStake
                changeInStake
                totalReward
                changeInReward
                totalUserCount
                activeUserCount
                depositCount
                withdrawCount
            }
        }
        """
    )


    str_query = str_query_with_param if last_period_start_unix else str_query_without_param


    # Provide a GraphQL query
    query = gql(str_query)

    params = {
        "lastPeriodStartUnix": last_period_start_unix,
    }

    # Execute the query on the transport
    result = rjoe_client.execute(query, variable_values=params)
    return result


def rjoe_get_all_day_snapshots() -> List[Dict[str, Any]]:
    last_period_start_unix = None

    all_day_snapshots = []
    while True:
        data = rjoe_get_day_snapshots(last_period_start_unix)
        daySnapshots = data["daySnapshots"]

        if len(daySnapshots) == 0:
            break

        last_period_start_unix = daySnapshots[-1]["periodStartUnix"]
        all_day_snapshots.extend(daySnapshots)
    
    return all_day_snapshots


def to_vejoe_day_snapshots_df(vejoe_day_snapshots: List[Dict[str, Any]]) -> pd.DataFrame:
    vejoe_day_snapshots_parsed = (
        {
            "period_start_unix": int(vejoe_day_snapshot["periodStartUnix"]),
            "veJOE.total_JOE_stake": Decimal(vejoe_day_snapshot["totalStake"]),
            "veJOE.total_veJOE_reward": Decimal(vejoe_day_snapshot["totalReward"]),
            "veJOE.change_JOE_stake": Decimal(vejoe_day_snapshot["changeInStake"]),
            "veJOE.change_veJOE_reward": Decimal(vejoe_day_snapshot["changeInReward"]),
            "veJOE.total_user_count": Decimal(vejoe_day_snapshot["totalUserCount"]),
            "veJOE.active_user_count": Decimal(vejoe_day_snapshot["activeUserCount"]),
            "veJOE.deposit_count": Decimal(vejoe_day_snapshot["depositCount"]),
            "veJOE.withdraw_count": Decimal(vejoe_day_snapshot["withdrawCount"]),
            "veJOE.claim_count": Decimal(vejoe_day_snapshot["claimCount"]),
        }
        for vejoe_day_snapshot in vejoe_day_snapshots
    )

    df_vejoe_day_snapshots = pd.json_normalize(vejoe_day_snapshots_parsed, sep=".")

    # df_vejoe_day_snapshots["date"] = pd.to_datetime(df_vejoe_day_snapshots["period_start_unix"], unit="s").dt.date
    df_vejoe_day_snapshots["date"] = pd.to_datetime(df_vejoe_day_snapshots["period_start_unix"], unit="s")
    df_vejoe_day_snapshots.drop(columns=["period_start_unix"], inplace=True)

    cols_in_terms_of_joe = [
        "veJOE.total_JOE_stake",
        "veJOE.change_JOE_stake",
    ]

    cols_in_terms_of_vejoe = [
        "veJOE.total_veJOE_reward",
        "veJOE.change_veJOE_reward",
    ]

    df_vejoe_day_snapshots[cols_in_terms_of_joe] = df_vejoe_day_snapshots[cols_in_terms_of_joe] / Decimal("10") ** JOE_DECIMALS
    df_vejoe_day_snapshots[cols_in_terms_of_vejoe] = df_vejoe_day_snapshots[cols_in_terms_of_vejoe] / Decimal("10") ** VEJOE_DECIMALS
    df_vejoe_day_snapshots.set_index(keys=["date"], inplace=True)

    return df_vejoe_day_snapshots


def to_sjoe_day_snapshots_df(sjoe_day_snapshots: List[Dict[str, Any]]) -> pd.DataFrame:
    sjoe_day_snapshots_parsed = (
        {
            "period_start_unix": int(sjoe_day_snapshot["periodStartUnix"]),
            "sJOE.total_JOE_stake": Decimal(sjoe_day_snapshot["totalStake"]),
            "sJOE.total_JOE_fee": Decimal(sjoe_day_snapshot["totalFee"]),
            "sJOE.change_JOE_stake": Decimal(sjoe_day_snapshot["changeInStake"]),
            "sJOE.change_JOE_fee": Decimal(sjoe_day_snapshot["changeInFee"]),
            "sJOE.total_user_count": Decimal(sjoe_day_snapshot["totalUserCount"]),
            "sJOE.active_user_count": Decimal(sjoe_day_snapshot["activeUserCount"]),
            "sJOE.deposit_count": Decimal(sjoe_day_snapshot["depositCount"]),
            "sJOE.withdraw_count": Decimal(sjoe_day_snapshot["withdrawCount"]),
            "sJOE.emergency_withdraw_count": Decimal(sjoe_day_snapshot["emergencyWithdrawCount"]),
            "sJOE.claim_count": Decimal(sjoe_day_snapshot["claimCount"]),
            "sJOE.total_rewards": {
                reward_dict["rewardToken"]["symbol"]: Decimal(reward_dict["totalReward"]) / Decimal("10") ** Decimal(reward_dict["rewardToken"]["decimals"])
                for reward_dict in sjoe_day_snapshot["rewards"]
            },
            "sJOE.change_in_rewards": {
                reward_dict["rewardToken"]["symbol"]: Decimal(reward_dict["changeInReward"]) / Decimal("10") ** Decimal(reward_dict["rewardToken"]["decimals"])
                for reward_dict in sjoe_day_snapshot["rewards"]
            }
        }
        for sjoe_day_snapshot in sjoe_day_snapshots
    )

    df_sjoe_day_snapshots = pd.json_normalize(sjoe_day_snapshots_parsed, sep=".")

    # df_vejoe_day_snapshots["date"] = pd.to_datetime(df_vejoe_day_snapshots["period_start_unix"], unit="s").dt.date
    df_sjoe_day_snapshots["date"] = pd.to_datetime(df_sjoe_day_snapshots["period_start_unix"], unit="s")
    df_sjoe_day_snapshots.drop(columns=["period_start_unix"], inplace=True)

    cols_in_terms_of_joe = [
        "sJOE.total_JOE_stake",
        "sJOE.total_JOE_fee",
        "sJOE.change_JOE_stake",
        "sJOE.change_JOE_fee",
    ]

    df_sjoe_day_snapshots[cols_in_terms_of_joe] = df_sjoe_day_snapshots[cols_in_terms_of_joe] / Decimal("10") ** JOE_DECIMALS
    df_sjoe_day_snapshots.set_index(keys=["date"], inplace=True)

    return df_sjoe_day_snapshots


def to_rjoe_day_snapshots_df(rjoe_day_snapshots: List[Dict[str, Any]]) -> pd.DataFrame:
    rjoe_day_snapshots_parsed = (
        {
            "period_start_unix": int(rjoe_day_snapshot["periodStartUnix"]),
            "rJOE.total_JOE_stake": Decimal(rjoe_day_snapshot["totalStake"]),
            "rJOE.total_rJOE_reward": Decimal(rjoe_day_snapshot["totalReward"]),
            "rJOE.change_JOE_stake": Decimal(rjoe_day_snapshot["changeInStake"]),
            "rJOE.change_rJOE_reward": Decimal(rjoe_day_snapshot["changeInReward"]),
            "rJOE.total_user_count": Decimal(rjoe_day_snapshot["totalUserCount"]),
            "rJOE.active_user_count": Decimal(rjoe_day_snapshot["activeUserCount"]),
            "rJOE.deposit_count": Decimal(rjoe_day_snapshot["depositCount"]),
            "rJOE.withdraw_count": Decimal(rjoe_day_snapshot["withdrawCount"]),
        }
        for rjoe_day_snapshot in rjoe_day_snapshots
    )

    df_rjoe_day_snapshots = pd.json_normalize(rjoe_day_snapshots_parsed, sep=".")

    df_rjoe_day_snapshots["date"] = pd.to_datetime(df_rjoe_day_snapshots["period_start_unix"], unit="s")
    df_rjoe_day_snapshots.drop(columns=["period_start_unix"], inplace=True)

    cols_in_terms_of_joe = [
        "rJOE.total_JOE_stake",
        "rJOE.change_JOE_stake",
    ]

    cols_in_terms_of_rjoe = [
        "rJOE.total_rJOE_reward",
        "rJOE.change_rJOE_reward",
    ]

    df_rjoe_day_snapshots[cols_in_terms_of_joe] = df_rjoe_day_snapshots[cols_in_terms_of_joe] / Decimal("10") ** JOE_DECIMALS
    df_rjoe_day_snapshots[cols_in_terms_of_rjoe] = df_rjoe_day_snapshots[cols_in_terms_of_rjoe] / Decimal("10") ** RJOE_DECIMALS
    df_rjoe_day_snapshots.set_index(keys=["date"], inplace=True)

    return df_rjoe_day_snapshots


def to_vejoe_wars_df(vejoe_wars_raw: List[Dict[str, Any]]) -> pd.DataFrame:
    vejoe_wars_parsed = [
        {
            "block_number": platform_block_data["block_number"],
            "platform": platform_block_data["platform"],
            "total_stake": Decimal(platform_block_data["user"]["totalStake"]) if platform_block_data.get("user") is not None else None,
            "total_reward": Decimal(platform_block_data["user"]["totalReward"]) if platform_block_data.get("user") is not None else None,
        }
        for block_data in vejoe_wars_raw
        for platform_block_data in block_data.values()
    ]
    df = pd.DataFrame(vejoe_wars_parsed)

    df["total_reward"] = df["total_reward"] / Decimal("10") ** VEJOE_DECIMALS
    df["total_stake"] = df["total_stake"] / Decimal("10") ** JOE_DECIMALS

    df["total_stake"] = df["total_stake"].astype(float)
    df["total_reward"] = df["total_reward"].astype(float)
    return df


def data_gathering_loop() -> None:
    if not os.path.exists("jsons/vejoe_wars.json"):
        data = vejoe_wars_at_multiple_block_numbers(12200000, 13760000, 10000)
        dump_json(data, "jsons/vejoe_wars.json")


    funcs_and_json_files = [
        (vejoe_get_all_users, "jsons/vejoe_get_all_users.json"),
        (vejoe_get_all_users_boosted_pool_positions, "jsons/vejoe_get_all_users_boosted_pool_positions.json"),
        (sjoe_get_all_users, "jsons/sjoe_get_all_users.json"),
        (rjoe_get_all_users, "jsons/rjoe_get_all_users.json"),
        (vejoe_get_all_day_snapshots, "jsons/vejoe_get_all_day_snapshots.json"),
        (sjoe_get_all_day_snapshots, "jsons/sjoe_get_all_day_snapshots.json"),
        (rjoe_get_all_day_snapshots, "jsons/rjoe_get_all_day_snapshots.json"),
        (vejoe_wars, "jsons/vejoe_wars.json"),
    ]

    block_number_step_size = 1000

    while True:
        for func, path in funcs_and_json_files:
            print(f"{func}")
            try:
                if func == vejoe_wars:
                    data = load_json(path)
                    last_block_number = data[-1]["Pool"]["block_number"]
                    block_number = last_block_number + block_number_step_size
                    newData = vejoe_wars_at_block_number(block_number)
                    data.append(newData)
                    dump_json(data, path)
                else:
                    data = func()
                    dump_json(data, path)
            except Exception as e:
                import traceback
                traceback.print_exc()
                pass
        # sleep for 1 minute
        time.sleep(1 * 60)
