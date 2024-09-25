from pydantic import BaseModel
import polars as pl


def covered_call_stg(df: pl.DataFrame, fee: Fee):
    df = df.filter(pl.col("bid_price_c") > 0, pl.col("ask_ua") > 0, pl.col("t") > 5)
    df = df.with_columns([
        (pl.col("ask_ua") * (1 + fee.long_ua)).alias("ask_ua_net"),
        (pl.col("bid_price_c") * (1 - fee.short_call)).alias("bid_price_c_net"),
        (pl.col("k") * (1 - fee.exercise)).alias("k_net"),
    ])

    df = df.with_columns(
        max_pot_profit=pl.col("k_net") - pl.col("ask_ua_net") + pl.col("bid_price_c_net"),
        max_pot_loss=pl.col("bid_price_c_net") - pl.col("ask_ua_net"),
        break_even=pl.col("ask_ua_net") - pl.col("bid_price_c_net"),
    )
    df = df.with_columns(
        pl.struct(["bid_price_c_net", "k_net", "ask_ua_net"]).map_elements(
            lambda x: -max(x["ask_ua_net"] - x["k_net"], 0) + x["bid_price_c_net"], return_dtype=pl.Float64).alias(
            "current_profit")
    )

    df = df.with_columns(
        pct_break_even=(pl.col("break_even") / pl.col("ask_ua_net") - 1) * 100,
        pct_mpp=pl.col("max_pot_profit") / pl.col("break_even") * 100,
        pct_cp=pl.col("current_profit") / pl.col("break_even") * 100,
    )
    df = df.with_columns(
        pct_status=(pl.col("k_net") / pl.col("ask_ua_net") - 1) * 100,
        ytm_mpp=((((pl.col("pct_mpp") / 100 + 1) ** (1 / pl.col("t"))) ** 365) - 1) * 100,
        ytm_cp=((((pl.col("pct_cp") / 100 + 1) ** (1 / pl.col("t"))) ** 365) - 1) * 100
    )

    return df