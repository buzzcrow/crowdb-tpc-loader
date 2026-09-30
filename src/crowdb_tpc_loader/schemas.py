"""Explicit benchmark inventories and logical schemas; no inferred table completeness.

I = signed 32/64-bit benchmark INTEGER/identifier (width is preserved, never cast).
L = signed int64; S = Arrow string/large_string; D = date32.
D<p>_<s> = exact decimal precision and scale. CHAR/VARCHAR are represented as UTF-8.
TPC-DS deliberately preserves the standard `s_tax_precentage` spelling.
"""
from __future__ import annotations

from .models import Column


def columns(spec: str) -> tuple[Column, ...]:
    return tuple(Column(*token.split(":")) for token in spec.split())


TPCH: dict[str, tuple[Column, ...]] = {
    "region": columns("r_regionkey:I r_name:S r_comment:S"),
    "nation": columns("n_nationkey:I n_name:S n_regionkey:I n_comment:S"),
    "supplier": columns("s_suppkey:I s_name:S s_address:S s_nationkey:I s_phone:S s_acctbal:D15_2 s_comment:S"),
    "customer": columns("c_custkey:I c_name:S c_address:S c_nationkey:I c_phone:S c_acctbal:D15_2 c_mktsegment:S c_comment:S"),
    "part": columns("p_partkey:I p_name:S p_mfgr:S p_brand:S p_type:S p_size:I p_container:S p_retailprice:D15_2 p_comment:S"),
    "partsupp": columns("ps_partkey:I ps_suppkey:I ps_availqty:I ps_supplycost:D15_2 ps_comment:S"),
    "orders": columns("o_orderkey:I o_custkey:I o_orderstatus:S o_totalprice:D15_2 o_orderdate:D o_orderpriority:S o_clerk:S o_shippriority:I o_comment:S"),
    "lineitem": columns("l_orderkey:I l_partkey:I l_suppkey:I l_linenumber:I l_quantity:D15_2 l_extendedprice:D15_2 l_discount:D15_2 l_tax:D15_2 l_returnflag:S l_linestatus:S l_shipdate:D l_commitdate:D l_receiptdate:D l_shipinstruct:S l_shipmode:S l_comment:S"),
}

TPCDS: dict[str, tuple[Column, ...]] = {
    "call_center": columns("""
        cc_call_center_sk:I cc_call_center_id:S cc_rec_start_date:D cc_rec_end_date:D
        cc_closed_date_sk:I cc_open_date_sk:I cc_name:S cc_class:S cc_employees:I cc_sq_ft:I
        cc_hours:S cc_manager:S cc_mkt_id:I cc_mkt_class:S cc_mkt_desc:S cc_market_manager:S
        cc_division:I cc_division_name:S cc_company:I cc_company_name:S cc_street_number:S
        cc_street_name:S cc_street_type:S cc_suite_number:S cc_city:S cc_county:S cc_state:S
        cc_zip:S cc_country:S cc_gmt_offset:D5_2 cc_tax_percentage:D5_2
    """),
    "catalog_page": columns("""
        cp_catalog_page_sk:I cp_catalog_page_id:S cp_start_date_sk:I cp_end_date_sk:I
        cp_department:S cp_catalog_number:I cp_catalog_page_number:I cp_description:S cp_type:S
    """),
    "catalog_returns": columns("""
        cr_returned_date_sk:I cr_returned_time_sk:I cr_item_sk:I cr_refunded_customer_sk:I
        cr_refunded_cdemo_sk:I cr_refunded_hdemo_sk:I cr_refunded_addr_sk:I cr_returning_customer_sk:I
        cr_returning_cdemo_sk:I cr_returning_hdemo_sk:I cr_returning_addr_sk:I cr_call_center_sk:I
        cr_catalog_page_sk:I cr_ship_mode_sk:I cr_warehouse_sk:I cr_reason_sk:I cr_order_number:L
        cr_return_quantity:I cr_return_amount:D7_2 cr_return_tax:D7_2 cr_return_amt_inc_tax:D7_2
        cr_fee:D7_2 cr_return_ship_cost:D7_2 cr_refunded_cash:D7_2 cr_reversed_charge:D7_2
        cr_store_credit:D7_2 cr_net_loss:D7_2
    """),
    "catalog_sales": columns("""
        cs_sold_date_sk:I cs_sold_time_sk:I cs_ship_date_sk:I cs_bill_customer_sk:I cs_bill_cdemo_sk:I
        cs_bill_hdemo_sk:I cs_bill_addr_sk:I cs_ship_customer_sk:I cs_ship_cdemo_sk:I cs_ship_hdemo_sk:I
        cs_ship_addr_sk:I cs_call_center_sk:I cs_catalog_page_sk:I cs_ship_mode_sk:I cs_warehouse_sk:I
        cs_item_sk:I cs_promo_sk:I cs_order_number:L cs_quantity:I cs_wholesale_cost:D7_2
        cs_list_price:D7_2 cs_sales_price:D7_2 cs_ext_discount_amt:D7_2 cs_ext_sales_price:D7_2
        cs_ext_wholesale_cost:D7_2 cs_ext_list_price:D7_2 cs_ext_tax:D7_2 cs_coupon_amt:D7_2
        cs_ext_ship_cost:D7_2 cs_net_paid:D7_2 cs_net_paid_inc_tax:D7_2 cs_net_paid_inc_ship:D7_2
        cs_net_paid_inc_ship_tax:D7_2 cs_net_profit:D7_2
    """),
    "customer": columns("""
        c_customer_sk:I c_customer_id:S c_current_cdemo_sk:I c_current_hdemo_sk:I c_current_addr_sk:I
        c_first_shipto_date_sk:I c_first_sales_date_sk:I c_salutation:S c_first_name:S c_last_name:S
        c_preferred_cust_flag:S c_birth_day:I c_birth_month:I c_birth_year:I c_birth_country:S
        c_login:S c_email_address:S c_last_review_date_sk:I
    """),
    "customer_address": columns("""
        ca_address_sk:I ca_address_id:S ca_street_number:S ca_street_name:S ca_street_type:S
        ca_suite_number:S ca_city:S ca_county:S ca_state:S ca_zip:S ca_country:S
        ca_gmt_offset:D5_2 ca_location_type:S
    """),
    "customer_demographics": columns("""
        cd_demo_sk:I cd_gender:S cd_marital_status:S cd_education_status:S cd_purchase_estimate:I
        cd_credit_rating:S cd_dep_count:I cd_dep_employed_count:I cd_dep_college_count:I
    """),
    "date_dim": columns("""
        d_date_sk:I d_date_id:S d_date:D d_month_seq:I d_week_seq:I d_quarter_seq:I d_year:I d_dow:I
        d_moy:I d_dom:I d_qoy:I d_fy_year:I d_fy_quarter_seq:I d_fy_week_seq:I d_day_name:S
        d_quarter_name:S d_holiday:S d_weekend:S d_following_holiday:S d_first_dom:I d_last_dom:I
        d_same_day_ly:I d_same_day_lq:I d_current_day:S d_current_week:S d_current_month:S
        d_current_quarter:S d_current_year:S
    """),
    "household_demographics": columns("hd_demo_sk:I hd_income_band_sk:I hd_buy_potential:S hd_dep_count:I hd_vehicle_count:I"),
    "income_band": columns("ib_income_band_sk:I ib_lower_bound:I ib_upper_bound:I"),
    "inventory": columns("inv_date_sk:I inv_item_sk:I inv_warehouse_sk:I inv_quantity_on_hand:I"),
    "item": columns("""
        i_item_sk:I i_item_id:S i_rec_start_date:D i_rec_end_date:D i_item_desc:S i_current_price:D7_2
        i_wholesale_cost:D7_2 i_brand_id:I i_brand:S i_class_id:I i_class:S i_category_id:I i_category:S
        i_manufact_id:I i_manufact:S i_size:S i_formulation:S i_color:S i_units:S i_container:S
        i_manager_id:I i_product_name:S
    """),
    "promotion": columns("""
        p_promo_sk:I p_promo_id:S p_start_date_sk:I p_end_date_sk:I p_item_sk:I p_cost:D15_2
        p_response_target:I p_promo_name:S p_channel_dmail:S p_channel_email:S p_channel_catalog:S
        p_channel_tv:S p_channel_radio:S p_channel_press:S p_channel_event:S p_channel_demo:S
        p_channel_details:S p_purpose:S p_discount_active:S
    """),
    "reason": columns("r_reason_sk:I r_reason_id:S r_reason_desc:S"),
    "ship_mode": columns("sm_ship_mode_sk:I sm_ship_mode_id:S sm_type:S sm_code:S sm_carrier:S sm_contract:S"),
    "store": columns("""
        s_store_sk:I s_store_id:S s_rec_start_date:D s_rec_end_date:D s_closed_date_sk:I s_store_name:S
        s_number_employees:I s_floor_space:I s_hours:S s_manager:S s_market_id:I s_geography_class:S
        s_market_desc:S s_market_manager:S s_division_id:I s_division_name:S s_company_id:I
        s_company_name:S s_street_number:S s_street_name:S s_street_type:S s_suite_number:S
        s_city:S s_county:S s_state:S s_zip:S s_country:S s_gmt_offset:D5_2 s_tax_precentage:D5_2
    """),
    "store_returns": columns("""
        sr_returned_date_sk:I sr_return_time_sk:I sr_item_sk:I sr_customer_sk:I sr_cdemo_sk:I
        sr_hdemo_sk:I sr_addr_sk:I sr_store_sk:I sr_reason_sk:I sr_ticket_number:L sr_return_quantity:I
        sr_return_amt:D7_2 sr_return_tax:D7_2 sr_return_amt_inc_tax:D7_2 sr_fee:D7_2
        sr_return_ship_cost:D7_2 sr_refunded_cash:D7_2 sr_reversed_charge:D7_2 sr_store_credit:D7_2
        sr_net_loss:D7_2
    """),
    "store_sales": columns("""
        ss_sold_date_sk:I ss_sold_time_sk:I ss_item_sk:I ss_customer_sk:I ss_cdemo_sk:I ss_hdemo_sk:I
        ss_addr_sk:I ss_store_sk:I ss_promo_sk:I ss_ticket_number:L ss_quantity:I ss_wholesale_cost:D7_2
        ss_list_price:D7_2 ss_sales_price:D7_2 ss_ext_discount_amt:D7_2 ss_ext_sales_price:D7_2
        ss_ext_wholesale_cost:D7_2 ss_ext_list_price:D7_2 ss_ext_tax:D7_2 ss_coupon_amt:D7_2
        ss_net_paid:D7_2 ss_net_paid_inc_tax:D7_2 ss_net_profit:D7_2
    """),
    "time_dim": columns("t_time_sk:I t_time_id:S t_time:I t_hour:I t_minute:I t_second:I t_am_pm:S t_shift:S t_sub_shift:S t_meal_time:S"),
    "warehouse": columns("""
        w_warehouse_sk:I w_warehouse_id:S w_warehouse_name:S w_warehouse_sq_ft:I w_street_number:S
        w_street_name:S w_street_type:S w_suite_number:S w_city:S w_county:S w_state:S w_zip:S
        w_country:S w_gmt_offset:D5_2
    """),
    "web_page": columns("""
        wp_web_page_sk:I wp_web_page_id:S wp_rec_start_date:D wp_rec_end_date:D wp_creation_date_sk:I
        wp_access_date_sk:I wp_autogen_flag:S wp_customer_sk:I wp_url:S wp_type:S wp_char_count:I
        wp_link_count:I wp_image_count:I wp_max_ad_count:I
    """),
    "web_returns": columns("""
        wr_returned_date_sk:I wr_returned_time_sk:I wr_item_sk:I wr_refunded_customer_sk:I
        wr_refunded_cdemo_sk:I wr_refunded_hdemo_sk:I wr_refunded_addr_sk:I wr_returning_customer_sk:I
        wr_returning_cdemo_sk:I wr_returning_hdemo_sk:I wr_returning_addr_sk:I wr_web_page_sk:I
        wr_reason_sk:I wr_order_number:L wr_return_quantity:I wr_return_amt:D7_2 wr_return_tax:D7_2
        wr_return_amt_inc_tax:D7_2 wr_fee:D7_2 wr_return_ship_cost:D7_2 wr_refunded_cash:D7_2
        wr_reversed_charge:D7_2 wr_account_credit:D7_2 wr_net_loss:D7_2
    """),
    "web_sales": columns("""
        ws_sold_date_sk:I ws_sold_time_sk:I ws_ship_date_sk:I ws_item_sk:I ws_bill_customer_sk:I
        ws_bill_cdemo_sk:I ws_bill_hdemo_sk:I ws_bill_addr_sk:I ws_ship_customer_sk:I ws_ship_cdemo_sk:I
        ws_ship_hdemo_sk:I ws_ship_addr_sk:I ws_web_page_sk:I ws_web_site_sk:I ws_ship_mode_sk:I
        ws_warehouse_sk:I ws_promo_sk:I ws_order_number:L ws_quantity:I ws_wholesale_cost:D7_2
        ws_list_price:D7_2 ws_sales_price:D7_2 ws_ext_discount_amt:D7_2 ws_ext_sales_price:D7_2
        ws_ext_wholesale_cost:D7_2 ws_ext_list_price:D7_2 ws_ext_tax:D7_2 ws_coupon_amt:D7_2
        ws_ext_ship_cost:D7_2 ws_net_paid:D7_2 ws_net_paid_inc_tax:D7_2 ws_net_paid_inc_ship:D7_2
        ws_net_paid_inc_ship_tax:D7_2 ws_net_profit:D7_2
    """),
    "web_site": columns("""
        web_site_sk:I web_site_id:S web_rec_start_date:D web_rec_end_date:D web_name:S web_open_date_sk:I
        web_close_date_sk:I web_class:S web_manager:S web_mkt_id:I web_mkt_class:S web_mkt_desc:S
        web_market_manager:S web_company_id:I web_company_name:S web_street_number:S web_street_name:S
        web_street_type:S web_suite_number:S web_city:S web_county:S web_state:S web_zip:S web_country:S
        web_gmt_offset:D5_2 web_tax_percentage:D5_2
    """),
}


def inventory(benchmark: str) -> dict[str, tuple[Column, ...]]:
    if benchmark == "tpch":
        return TPCH
    if benchmark == "tpcds":
        return TPCDS
    raise ValueError(f"Unknown benchmark: {benchmark}")


def arrow_schema(benchmark: str, table: str):
    """Canonical probe/test schema. Real tables preserve the validated generator's schema."""
    import pyarrow as pa

    primitive = {"I": pa.int64(), "L": pa.int64(), "S": pa.string(), "D": pa.date32()}
    fields = []
    for col in inventory(benchmark)[table]:
        kind = col.kind
        if kind.startswith("D") and "_" in kind:
            precision, scale = map(int, kind[1:].split("_"))
            dtype = pa.decimal128(precision, scale)
        else:
            dtype = primitive[kind]
        fields.append(pa.field(col.name, dtype, nullable=True))
    return pa.schema(fields)
