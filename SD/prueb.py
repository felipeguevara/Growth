#//----------------------------
#//LIBRARIES
    #Math
import math
    #Numeric Python
import numpy as np
    #Pandas (dataframes)
import pandas as pd
    #datetime for fate manipulation
from datetime import date, datetime, timedelta  
    #Regex for advanced string matching
import re
    #for time related stuff
import time
    #json library
import json
    #Analyst tools
import sys
sys.path.append('../')
from analysts_tools.growth import *
    #Procurement tools
#from procurement_lib import send_slack_notification
#from procurement_lib import redash
    #redash and slack conections 
from analysts_tools.redash_methods import *
from slack_notification import send_slack_notification
    #work with str
import ast
from procurement_lib import GoogleSheet, logging
    
from tabulate import tabulate
    #environment parameteres
import os 
from dotenv import load_dotenv
load_dotenv()

#//------------------------------------------------------------------------
#//Functions

#extracts de skus from the strings of the offer's order_item_match_rule
def detectSKUs(vsr):
    #Se hace el SPLIT y se convierte de str(list) -> list
    res = ast.literal_eval(vsr.split('?id,')[1].split(').size')[0])
    res = list([int(x) for x in res])
    return res

def getchannel(city):
    if city in ['BOG', 'BAQ', 'MDE']:
        return 'CO'
    elif city in ['CMX', 'GDL', 'PBC', 'MTY']:
        return 'MX'
    else:
        return 'BR'
    
#matches the city and skus of the local hooks in the stock data
def getready(city):
    #join the skus of the city hooks that are active
    local_offers = offers[(offers['region_code']==city)&(offers['automatically_added']=='true')]
    skus = []
    for string in local_offers['order_item_match_rule'].tolist():
        skus = skus + detectSKUs(string)
    #//Check stock for the current city and detected skus
    local_stocks = stockOut[(stockOut['region_code']==city)&(stockOut['sku_id'].isin(skus))].reset_index(drop=True)
    
    stockoutList = pd.DataFrame()
    nearStockoutList = pd.DataFrame()
    for i in local_stocks.index:
        if local_stocks.iloc[i]['disponible'] == 0:
            stockoutList = stockoutList.append(local_stocks.iloc[i][['name']])
        elif local_stocks.iloc[i]['disponible'] < 15:
            nearStockoutList = nearStockoutList.append(local_stocks.iloc[i][['disponible', 'name']])

    environment = os.environ.get('environment', 'DEV_SAMPLE')
    people = {'BOG': ["felipe"], 'BAQ': ["felipe"], 'MDE': ["felipe"], 
              'CMX': ["felipe","david"], 'GDL': ["felipe","david"], 'PBC': ["felipe","david"], 'MTY': ["felipe","david"], 
              'SPO': ["felipe","vittoria"], 'BHZ': ["felipe","vittoria"], 'CWB': ["felipe","vittoria"], 'VCP': ["felipe","vittoria"]
             }.get(city, [])
    
    message ="""
Hook Stockouts en {city}: 
    
{a}
    
    
Hooks with Stockout Risk en {city}: 
    
    
{b}
    
    
""".format(city=city, a=tabulate(stockoutList, showindex=False, headers=stockoutList.columns), b=tabulate(nearStockoutList, showindex=False, headers=nearStockoutList.columns))
    
    send_slack_notification(getchannel(city), message, people, ':thisisfine:')


if __name__ == "__main__":

    #//-------------------------------------------------------------
    #//Catalog discs (regions?)
    city_dict = {-110002:'BOG', -110000:'BAQ', -110004:'CMX', -110020:'VCP',
                 -110008:'GDL', -110010:'BHZ', -110018:'MDE', -110016:'CWB',
                 -110006:'SPO', -110014:'MTY', -110012:'PBC'}
    #//---------------------------------------------------------
    #//import data
        #//stock skus
    stockOut = get_fresh_query_result("https://internal-redash.federate.frubana.com/",93617,'SeoGHWmDUaaBi7VXje1s9zYNiMD1VHQ1K1DYOxiF',{},20)
        #//discounts skus
    offers = get_fresh_query_result("https://internal-redash.federate.frubana.com/",93618,'SeoGHWmDUaaBi7VXje1s9zYNiMD1VHQ1K1DYOxiF',{},20)
    #//--------------------------------------------------------

    #//apply region code
    offers['region_code'] = offers['catalog_disc'].apply(lambda x: city_dict[x])

    #//run for all cities with data
    for i in offers.region_code.unique():
        getready(i) 
        
    #//---------------------------------------------------------ALERTA DE HOOKS POR VENCER -----------------------------------------------
    #//---------------------------------------------------------ALERTA DE HOOKS POR VENCER -----------------------------------------------
    #//---------------------------------------------------------ALERTA DE HOOKS POR VENCER -----------------------------------------------
    hooks = get_fresh_query_result("https://internal-redash.federate.frubana.com/",93991,'SeoGHWmDUaaBi7VXje1s9zYNiMD1VHQ1K1DYOxiF',{},20)
    tomorrow = (date.today() + timedelta(days=1)).strftime("%Y-%m-%d")
    hooks_alert = hooks[(hooks['end_date']==tomorrow) & (hooks['automatically_added']==True)]
    sent = hooks_alert[['site_identifier_value','offer_id', 'offer_name']]
    message ="""
Hooks end_date == Tomorrow: 
    
{a}
    
""".format(a=tabulate(sent, showindex=False, headers=sent.columns))
    environment = os.environ.get('environment', 'DEV_SAMPLE')
    people = {'ALL': ["felipe","vittoria","david"]}.get('ALL', [])
    send_slack_notification('END_HOOKS', message, people, ':alert:')
    
    #//---------------------------------------------------------ALERTA DE BUDGET MERMA -----------------------------------------------
    #//---------------------------------------------------------ALERTA DE BUDGET MERMA -----------------------------------------------
    #//---------------------------------------------------------ALERTA DE BUDGET MERMA -----------------------------------------------
    query = """
    -- 93058
    -- Se toma como base el query 2718
    WITH 
    ful as (

        SELECT DISTINCT
            bo.order_id,
            ffg.close_date

        FROM postgres_broadleaf_federate."broadleaf.blc_order"                    bo
        INNER JOIN postgres_broadleaf_federate."broadleaf.blc_fulfillment_group"  bfg     ON bfg.order_id = bo.order_id
        INNER JOIN postgres_broadleaf_federate."broadleaf.blc_fulfillment_order"  bfo     ON bfo.fulfillment_group_id = bfg.fulfillment_group_id
        INNER JOIN postgres_broadleaf_federate."broadleaf.fb_fulfillment_group"   ffg     ON ffg.fulfillment_group_id = bfg.fulfillment_group_id
        INNER JOIN postgres_broadleaf_federate."broadleaf.fb_order"               fo      ON fo.order_id = bo.order_id
        INNER JOIN postgres_broadleaf_federate."broadleaf.blc_order_payment"      bop     ON bop.order_id = bo.order_id
        INNER JOIN postgres_broadleaf_federate."broadleaf.blc_site"               s       ON s.site_id = bo.site_disc
        LEFT JOIN  postgres_broadleaf_federate."broadleaf.fb_order_type"          fot     ON fot.fb_order_type_id=fo.fb_order_type_id
        LEFT JOIN  postgres_broadleaf_federate."broadleaf.blc_order_adjustment"   ba      ON ba.order_id = bo.order_id

        WHERE fo.fb_order_status_id IN (1,6,7,8)
            AND extract(year from ffg.close_date) = extract(year from CURRENT_DATE) 
            AND extract(month from ffg.close_date) = extract(month from CURRENT_DATE) 
            --AND extract(day from ffg.close_date) = extract(day from CURRENT_DATE) 
            AND bo.order_status = 'SUBMITTED'
            AND bfo.status NOT IN ('ARCHIVED','CANCELLED')
            AND bop.archived = 'N'
            AND (fot.name IS NULL OR fot.name <> 'REFUND')

        GROUP BY bo.order_id, ffg.close_date
    ),

    products_discounts AS (

        SELECT DISTINCT 
            s.site_identifier_value AS region_code,
            TO_CHAR(ful.close_date, 'YYYY-mm') AS month,
            CASE
                WHEN ( (LOWER(boida.adjustment_reason) ILIKE '%merma%') AND ((COALESCE(bcat2.name, bcat.name) = 'Frutas & Verduras') OR (COALESCE(bcat2.name, bcat.name) = 'Frutas e Verduras')) ) THEN 'MERMA FRUVER'
                WHEN ( (LOWER(boida.adjustment_reason) ILIKE '%merma%') AND (COALESCE(bcat2.name, bcat.name) NOT IN ('Frutas & Verduras','Frutas e Verduras')) ) THEN 'MERMA MKTPLC'
                WHEN ( (LOWER(boida.adjustment_reason) ILIKE '%acm%') OR (LOWER(boida.adjustment_reason) ILIKE '%kof%') OR (LOWER(boida.adjustment_reason) ILIKE '%campana%') ) THEN 'DCTO. PROVEEDOR'
                WHEN boida.adjustment_reason IS NULL THEN 'NO DISCOUNTS'
            ELSE 'GROWTH & OTHERS' END AS responsable,
            CASE WHEN s.site_identifier_value = 'CMX' then (SUM(boida.adjustment_value*boipd.quantity*foi.step_unit)*1.0/19.19)
                WHEN s.site_identifier_value = 'GDL' then (SUM(boida.adjustment_value*boipd.quantity*foi.step_unit)*1.0/19.19)
                WHEN s.site_identifier_value = 'PBC' then (SUM(boida.adjustment_value*boipd.quantity*foi.step_unit)*1.0/19.19)
                WHEN s.site_identifier_value = 'MTY' then (SUM(boida.adjustment_value*boipd.quantity*foi.step_unit)*1.0/19.19)
                WHEN s.site_identifier_value = 'SPO' then (SUM(boida.adjustment_value*boipd.quantity*foi.step_unit)*1.0/3.88)
                WHEN s.site_identifier_value = 'BHZ' then (SUM(boida.adjustment_value*boipd.quantity*foi.step_unit)*1.0/3.88)
                WHEN s.site_identifier_value = 'CWB' then (SUM(boida.adjustment_value*boipd.quantity*foi.step_unit)*1.0/3.88)
                WHEN s.site_identifier_value = 'VCP' then (SUM(boida.adjustment_value*boipd.quantity*foi.step_unit)*1.0/3.88)
            ELSE (SUM(boida.adjustment_value*boipd.quantity*foi.step_unit)*1.0/3000) END AS discount_usd--,
            --SUM(boida.adjustment_value*boipd.quantity*foi.step_unit) AS products_discount ---MIRAR CON OSCARIN
            --COUNT(boi.order_item_id) as items,
            --COUNT(distinct boi.order_item_id) as check_items

        FROM postgres_broadleaf_federate."broadleaf.blc_order"                          bo
        INNER JOIN postgres_broadleaf_federate."broadleaf.blc_site"                     s       ON s.site_id = bo.site_disc
        INNER JOIN postgres_broadleaf_federate."broadleaf.blc_order_item"               boi     ON boi.order_id=bo.order_id
        INNER JOIN postgres_broadleaf_federate."broadleaf.fb_order_item"                foi     ON boi.order_item_id= foi.order_item_id
        INNER JOIN postgres_broadleaf_federate."broadleaf.fb_order"                     fo      ON fo.order_id = bo.order_id
        INNER JOIN postgres_broadleaf_federate."broadleaf.blc_order_item_price_dtl"     boipd   ON boipd.order_item_id=boi.order_item_id
        INNER JOIN ful                                                                          ON ful.order_id = bo.order_id
        INNER JOIN postgres_broadleaf_federate."broadleaf.blc_discrete_order_item"      bdoi    ON bdoi.order_item_id = boi.order_item_id
        INNER JOIN postgres_broadleaf_federate."broadleaf.blc_sku"                      bs      ON bs.sku_id = bdoi.sku_id
        INNER JOIN postgres_broadleaf_federate."broadleaf.blc_product"                  bp      ON bs.addl_product_id = bp.product_id
        INNER JOIN postgres_broadleaf_federate."broadleaf.blc_category"                 bcat    ON bcat.category_id = bp.default_category_id 
        INNER JOIN postgres_broadleaf_federate."broadleaf.blc_category_xref"            bcx     ON bcx.sub_category_id = bp.default_category_id AND bcx.archived='N' AND bcx.sndbx_tier is NULL
        INNER JOIN postgres_broadleaf_federate."broadleaf.blc_category"                 bcat2   ON bcx.category_id = bcat2.category_id
        INNER JOIN postgres_broadleaf_federate."broadleaf.blc_order_item_dtl_adj"       boida   ON boida.order_item_price_dtl_id=boipd.order_item_price_dtl_id

        WHERE fb_order_status_id IN (1,6,7,8)
            AND bo.order_status = 'SUBMITTED'
           -- FIX SUPER DESCUENTOS
            AND bcat2.category_id not in ('110873','-1000','100768','100765','100815') --ids de super descuentos en cada país
            AND bcat.name <> 'Oferton Frubana' AND bcat2.name <> 'Oferton Frubana'

        GROUP BY 1,2,3
    ),

    products_gmv AS (

        SELECT 
            s.site_identifier_value AS region_code,
            TO_CHAR(ful.close_date, 'YYYY-mm') AS month,
            CASE WHEN s.site_identifier_value = 'CMX' then (SUM(COALESCE(bo.order_subtotal,0) + COALESCE(fo.total_tax_iva,0) + COALESCE(bo.total_shipping,0))*1.0/19.19)
                WHEN s.site_identifier_value = 'GDL' then (SUM(COALESCE(bo.order_subtotal,0) + COALESCE(fo.total_tax_iva,0) + COALESCE(bo.total_shipping,0))*1.0/19.19)
                WHEN s.site_identifier_value = 'PBC' then (SUM(COALESCE(bo.order_subtotal,0) + COALESCE(fo.total_tax_iva,0) + COALESCE(bo.total_shipping,0))*1.0/19.19)
                WHEN s.site_identifier_value = 'MTY' then (SUM(COALESCE(bo.order_subtotal,0) + COALESCE(fo.total_tax_iva,0) + COALESCE(bo.total_shipping,0))*1.0/19.19)
                WHEN s.site_identifier_value = 'SPO' then (SUM(COALESCE(bo.order_subtotal,0) + COALESCE(fo.total_tax_iva,0) + COALESCE(bo.total_shipping,0))*1.0/3.88)
                WHEN s.site_identifier_value = 'BHZ' then (SUM(COALESCE(bo.order_subtotal,0) + COALESCE(fo.total_tax_iva,0) + COALESCE(bo.total_shipping,0))*1.0/3.88)
                WHEN s.site_identifier_value = 'CWB' then (SUM(COALESCE(bo.order_subtotal,0) + COALESCE(fo.total_tax_iva,0) + COALESCE(bo.total_shipping,0))*1.0/3.88)
                WHEN s.site_identifier_value = 'VCP' then (SUM(COALESCE(bo.order_subtotal,0) + COALESCE(fo.total_tax_iva,0) + COALESCE(bo.total_shipping,0))*1.0/3.88)
            ELSE (SUM(COALESCE(bo.order_subtotal,0) + COALESCE(fo.total_tax_iva,0) + COALESCE(bo.total_shipping,0))*1.0/3000) END AS gmv_usd

        FROM postgres_broadleaf_federate."broadleaf.blc_order"          bo 
        --------------------- FIX TRAE MÁS ÓRDENES ---------------------
        --LEFT JOIN blc_order_adjustment ba ON ba.order_id = bo.order_id NO SE PUEDE USAR DIRECTO SINO A TRAVES DE SUBQUERY
        INNER JOIN postgres_broadleaf_federate."broadleaf.blc_site"     s   ON s.site_id = bo.site_disc
        INNER JOIN postgres_broadleaf_federate."broadleaf.fb_order"     fo  ON fo.order_id = bo.order_id
        INNER JOIN ful                                                      ON ful.order_id = bo.order_id

        -- WHERE bo.order_id in (
        --     SELECT distinct bo.order_id
        --         FROM blc_order bo
        --         INNER JOIN blc_site s ON s.site_id = bo.site_disc
        --         INNER JOIN blc_fulfillment_group bfg ON bfg.order_id = bo.order_id
        --         INNER JOIN blc_fulfillment_order bfo ON bfo.fulfillment_group_id = bfg.fulfillment_group_id
        --         INNER JOIN fb_fulfillment_group ffg ON ffg.fulfillment_group_id = bfg.fulfillment_group_id
        --         INNER JOIN fb_order fo ON fo.order_id = bo.order_id
        --         INNER JOIN blc_order_payment bop ON bop.order_id = bo.order_id

        --         WHERE 
        --         ffg.close_date BETWEEN (DATE(CURRENT_DATE) - interval '3 month') AND (DATE(CURRENT_DATE) + interval '1 week')
        --             AND fo.fb_order_status_id IN (1,6,7,8)
        --             AND bo.order_status = 'SUBMITTED'
        --             AND bfo.status NOT IN ('ARCHIVED','CANCELLED')
        --             AND bop.archived = 'N'
        --                     ) 

        GROUP BY 1,2
    ),

    base_plus_target AS (

    SELECT 
        pg.region_code AS "city",
        pg.month AS "month",
        pd.responsable,
        pg.gmv_usd,
        pd.discount_usd,
        pd.discount_usd*100.00/pg.gmv_usd AS per_discounts_over_gmv,
        CASE
        ---------------------- TARGET GROWTH --------------------------------------
            WHEN (pd.region_code IN ('BOG','BHZ','CWB','VCP') AND pd.responsable = 'GROWTH & OTHERS')             THEN 3.80
            WHEN (pd.region_code IN ('BAQ','MDE','CMX','GDL','PBC') AND pd.responsable = 'GROWTH & OTHERS')       THEN 3.88
            WHEN (pd.region_code = 'SPO' AND pd.responsable = 'GROWTH & OTHERS')                                  THEN 3.84
        ---------------------- TARGET FRUVER --------------------------------------
            WHEN (pd.region_code IN ('BOG','BHZ','MDE','CMX','GDL','PBC') AND pd.responsable = 'MERMA FRUVER')    THEN 0.08
            WHEN (pd.region_code = 'BAQ' AND pd.responsable = 'MERMA FRUVER')                                     THEN 0.04
            WHEN (pd.region_code = 'SPO' AND pd.responsable = 'MERMA FRUVER')                                     THEN 0.10
            WHEN (pd.region_code IN ('CWB','VCP') AND pd.responsable = 'MERMA FRUVER')                            THEN 0.12
        ---------------------- TARGET 3PL --------------------------------------
            WHEN (pd.region_code IN ('BOG','BHZ') AND pd.responsable = 'MERMA MKTPLC')                               THEN 0.12
            WHEN (pd.region_code IN ('BAQ','CWB','VCP') AND pd.responsable = 'MERMA MKTPLC')                         THEN 0.08
            WHEN (pd.region_code IN ('MDE','CMX','GDL','PBC') AND pd.responsable = 'MERMA MKTPLC')                   THEN 0.04
            WHEN (pd.region_code = 'SPO' AND pd.responsable = 'MERMA MKTPLC')                                        THEN 0.06
        ELSE 0 END AS target_over_gmv

    FROM products_gmv pg
    INNER JOIN products_discounts pd ON pg.region_code = pd.region_code AND pg.month = pd.month 
    )
    --a
    SELECT
        *,
        CASE
            WHEN target_over_gmv = 0 THEN 'NA'
            WHEN (target_over_gmv - per_discounts_over_gmv) < 0 THEN 'ALERTA!!!'
            WHEN (target_over_gmv - per_discounts_over_gmv) >= 0 THEN 'OK'
        END AS status
    FROM base_plus_target
    --ORDER BY "month::filter" DESC
    """
    dataframe = read_connection_data_warehouse.runQuery(query)
    
    cities = ['BOG', 'BAQ', 'MDE']
    date = datetime.today().replace(day=1).strftime('%Y-%m-%d')
    dataframe['month'] = dataframe['month'].apply(lambda x: datetime.strptime(x, '%Y-%m').strftime('%Y-%m-%d'))
    info_hooks = dataframe[(dataframe['city'].isin(cities)) & (dataframe['month'] == date) & (dataframe['responsable'].str.contains("merma", case=False))]
    inffo = info_hooks[['city','responsable','target_over_gmv','per_discounts_over_gmv','status']].sort_values(by=['city','responsable'])

    message = """
INFO BUDGET MERMA: 
    
{a}
    
""".format(a=tabulate(inffo, showindex=False, headers=inffo.columns))
                                                                                                               
    environment = os.environ.get('environment', 'DEV_SAMPLE')
    people = {'ALL': ["juanita","juan da","juan di","felipe"]}.get('ALL', [])
    send_slack_notification('ALERT_MERMA', message, people, ':thisisfine:')

    #//---------------------------------------------------------ALERTA DE BUDGET INVESTMENT -----------------------------------------------
    #//---------------------------------------------------------ALERTA DE BUDGET INVESTMENT -----------------------------------------------
    #//---------------------------------------------------------ALERTA DE BUDGET INVESTMENT -----------------------------------------------
    paramss_csv=GoogleSheet("1Q8PtNX9Nj0Ha2sZwmW7fhp08aLN2Etw0hjU456bNiXA")
    city_paramss = paramss_csv.get_as_dataframe("Info")
    
    #Se corre el query con la info de additional investment
    query = """
    WITH 

    ful as (

        SELECT DISTINCT
            bo.order_id,
            ffg.close_date
            
        FROM postgres_broadleaf_federate."broadleaf.blc_order"                      bo
        INNER JOIN postgres_broadleaf_federate."broadleaf.blc_fulfillment_group"    bfg ON bfg.order_id = bo.order_id
        INNER JOIN postgres_broadleaf_federate."broadleaf.blc_fulfillment_order"    bfo ON bfo.fulfillment_group_id = bfg.fulfillment_group_id
        INNER JOIN postgres_broadleaf_federate."broadleaf.fb_fulfillment_group"     ffg ON ffg.fulfillment_group_id = bfg.fulfillment_group_id
        INNER JOIN postgres_broadleaf_federate."broadleaf.fb_order"                 fo  ON fo.order_id = bo.order_id
        INNER JOIN postgres_broadleaf_federate."broadleaf.blc_order_payment"        bop ON bop.order_id = bo.order_id
        INNER JOIN postgres_broadleaf_federate."broadleaf.blc_site"                 s   ON s.site_id = bo.site_disc
        LEFT JOIN  postgres_broadleaf_federate."broadleaf.fb_order_type"            fot ON fot.fb_order_type_id=fo.fb_order_type_id
        LEFT JOIN  postgres_broadleaf_federate."broadleaf.blc_order_adjustment"     ba  ON ba.order_id = bo.order_id
        
        WHERE fo.fb_order_status_id IN (1,6,7,8)
            AND extract(year from ffg.close_date) = extract(year from CURRENT_DATE) 
            AND extract(month from ffg.close_date) = extract(month from CURRENT_DATE)
            AND extract(week from ffg.close_date) = extract(week from CURRENT_DATE)
            AND s.site_identifier_value IN ('BOG','SPO','MDE')
            AND bo.order_status = 'SUBMITTED'
            AND bfo.status NOT IN ('ARCHIVED','CANCELLED')
            AND bop.archived = 'N'
            AND (fot.name IS NULL OR fot.name <> 'REFUND')
            
    ),

    all_data AS (

    SELECT 
        s.site_identifier_value as region_code,
        TO_CHAR(ful.close_date, 'WW') AS week,
        CASE
            WHEN (boida.adjustment_reason ILIKE '%Investment%')THEN 'ADDITIONAL INVESTMENT'
        ELSE 'OTHERS' END AS responsable,
        boida.adjustment_reason AS discount,
        bcat2.name as cat,
        bcat.name as subcat,
        bs.addl_product_id,
        bs2.name,
        CASE 
            WHEN bs.addl_product_id = 282263 THEN 0.04
            WHEN bs.addl_product_id = 429315 THEN 0.04
            WHEN bs.addl_product_id = 282279 THEN 0.04
            WHEN bs.addl_product_id = 282287 THEN 0.05
            WHEN bs.addl_product_id = 282610 THEN 0.03
            WHEN bs.addl_product_id = 319095 THEN 0
            WHEN bs.addl_product_id = 395331 THEN 0
            WHEN bs.addl_product_id = 280175 THEN 0
            WHEN bs.addl_product_id = 280159 THEN 0.06
            WHEN bs.addl_product_id = 280163 THEN 0
            WHEN bs.addl_product_id = 280162 THEN 0
            WHEN bs.addl_product_id = 280166 THEN 0.05
            WHEN bs.addl_product_id = 280172 THEN 0
            WHEN bs.addl_product_id = 303714 THEN 0
            WHEN bs.addl_product_id = 280160 THEN 0.07
            WHEN bs.addl_product_id = 338423 THEN 0
            WHEN bs.addl_product_id = 280171 THEN 0
            WHEN bs.addl_product_id = 287486 THEN 0.06
            WHEN bs.addl_product_id = 343750 THEN 0
            WHEN bs.addl_product_id = 280184 THEN 0
            WHEN bs.addl_product_id = 280165 THEN 0.04
            WHEN bs.addl_product_id = 280198 THEN 0
            WHEN bs.addl_product_id = 280195 THEN 0
            WHEN bs.addl_product_id = 280178 THEN 0
            WHEN bs.addl_product_id = 280201 THEN 0.05
            WHEN bs.addl_product_id = 280182 THEN 0.05
            WHEN bs.addl_product_id = 280161 THEN 0.05
            WHEN bs.addl_product_id = 290271 THEN 0.04
            WHEN bs.addl_product_id = 180632 THEN 0.04
            WHEN bs.addl_product_id = 225171 THEN 0.03
            WHEN bs.addl_product_id = -200654 THEN 0.04
            WHEN bs.addl_product_id = -200916 THEN 0.06
            WHEN bs.addl_product_id = -200152 THEN 0
            WHEN bs.addl_product_id = -200000 THEN 0
            WHEN bs.addl_product_id = -200004 THEN 0
            WHEN bs.addl_product_id = -200166 THEN 0
            WHEN bs.addl_product_id = 50311 THEN 0
            WHEN bs.addl_product_id = -200012 THEN 0.08
            WHEN bs.addl_product_id = -200178 THEN 0
            WHEN bs.addl_product_id = -200014 THEN 0
            WHEN bs.addl_product_id = -200898 THEN 0
            WHEN bs.addl_product_id = -200752 THEN 0
            WHEN bs.addl_product_id = -200722 THEN 0
            WHEN bs.addl_product_id = -200764 THEN 0
            WHEN bs.addl_product_id = -200028 THEN 0.06
            WHEN bs.addl_product_id = -200114 THEN 0
            WHEN bs.addl_product_id = -200048 THEN 0
            WHEN bs.addl_product_id = -200050 THEN 0
            WHEN bs.addl_product_id = 46983 THEN 0
            WHEN bs.addl_product_id = 55680 THEN 0
            WHEN bs.addl_product_id = 116701 THEN 0
            WHEN bs.addl_product_id = 116695 THEN 0
            WHEN bs.addl_product_id = 25443 THEN 0
            WHEN bs.addl_product_id = 395328 THEN 0
            WHEN bs.addl_product_id = 121443 THEN 0
            WHEN bs.addl_product_id = -200084 THEN 0
            WHEN bs.addl_product_id = -200484 THEN 0
            WHEN bs.addl_product_id = -200086 THEN 0.07
            WHEN bs.addl_product_id = 136259 THEN 0.05
            WHEN bs.addl_product_id = 65666 THEN 0.12
            WHEN bs.addl_product_id = 274509 THEN 0
            WHEN bs.addl_product_id = 270111 THEN 0
            WHEN bs.addl_product_id = 242062 THEN 0.07
            WHEN bs.addl_product_id = 19739 THEN 0.07
            WHEN bs.addl_product_id = 373411 THEN 0.03
            WHEN bs.addl_product_id = 193561 THEN 0.08
            WHEN bs.addl_product_id = 311459 THEN 0
            WHEN bs.addl_product_id = 194518 THEN 0.03
            WHEN bs.addl_product_id = 197660 THEN 0
            WHEN bs.addl_product_id = 280418 THEN 0.03
            WHEN bs.addl_product_id = 130229 THEN 0.03
            WHEN bs.addl_product_id = 413151 THEN 0
            WHEN bs.addl_product_id = 275011 THEN 0
            WHEN bs.addl_product_id = 60440 THEN 0
            WHEN bs.addl_product_id = 421531 THEN 0
            WHEN bs.addl_product_id = 123459 THEN 0.06
            WHEN bs.addl_product_id = 205461 THEN 0
            WHEN bs.addl_product_id = 313963 THEN 0.06
            WHEN bs.addl_product_id = -202941 THEN 0.05
            WHEN bs.addl_product_id = -202908 THEN 0.06
            WHEN bs.addl_product_id = 60544 THEN 0.06
            WHEN bs.addl_product_id = 132134 THEN 0.06
            WHEN bs.addl_product_id = 95430 THEN 0.06
            WHEN bs.addl_product_id = -202958 THEN 0
            WHEN bs.addl_product_id = -202929 THEN 0
            WHEN bs.addl_product_id = -202940 THEN 0
            WHEN bs.addl_product_id = -202931 THEN 0
            WHEN bs.addl_product_id = -202972 THEN 0
            WHEN bs.addl_product_id = -202906 THEN 0
            WHEN bs.addl_product_id = 59565 THEN 0
            WHEN bs.addl_product_id = 197110 THEN 0
        ELSE 0 END AS hook,
        sum(boi.quantity*foi.step_unit) as quantity,
        count(distinct bo.order_id) as ordenes,
        count(distinct bo.customer_id) as usuarios,
        CASE 
            --WHEN s.site_identifier_value = 'CMX' then ((SUM(boi.quantity * foi.step_unit * boi.sale_price) + coalesce(SUM(foi.total_tax_iva),0))*1.0/19.65)
            --WHEN s.site_identifier_value = 'GDL' then ((SUM(boi.quantity * foi.step_unit * boi.sale_price) + coalesce(SUM(foi.total_tax_iva),0))*1.0/19.65)
            --WHEN s.site_identifier_value = 'PBC' then ((SUM(boi.quantity * foi.step_unit * boi.sale_price) + coalesce(SUM(foi.total_tax_iva),0))*1.0/19.65)
            --WHEN s.site_identifier_value = 'MTY' then ((SUM(boi.quantity * foi.step_unit * boi.sale_price) + coalesce(SUM(foi.total_tax_iva),0))*1.0/19.65)
            WHEN s.site_identifier_value = 'SPO' then ((SUM(boi.quantity * foi.step_unit * boi.sale_price) + coalesce(SUM(foi.total_tax_iva),0))*1.0/4.75)
            --WHEN s.site_identifier_value = 'BHZ' then ((SUM(boi.quantity * foi.step_unit * boi.sale_price) + coalesce(SUM(foi.total_tax_iva),0))*1.0/4.75)
            --WHEN s.site_identifier_value = 'CWB' then ((SUM(boi.quantity * foi.step_unit * boi.sale_price) + coalesce(SUM(foi.total_tax_iva),0))*1.0/4.75)
            --WHEN s.site_identifier_value = 'VCP' then ((SUM(boi.quantity * foi.step_unit * boi.sale_price) + coalesce(SUM(foi.total_tax_iva),0))*1.0/4.75)
        ELSE ((SUM(boi.quantity * foi.step_unit * boi.sale_price) + coalesce(SUM(foi.total_tax_iva),0))*1.0/3776) END AS net_gmv_usd,
        CASE 
            --WHEN s.site_identifier_value = 'CMX' then ((COALESCE(SUM(boida.adjustment_value*boipd.quantity*foi.step_unit),0))*1.0/19.65)
            --WHEN s.site_identifier_value = 'GDL' then ((COALESCE(SUM(boida.adjustment_value*boipd.quantity*foi.step_unit),0))*1.0/19.65)
            --WHEN s.site_identifier_value = 'PBC' then ((COALESCE(SUM(boida.adjustment_value*boipd.quantity*foi.step_unit),0))*1.0/19.65)
            --WHEN s.site_identifier_value = 'MTY' then ((COALESCE(SUM(boida.adjustment_value*boipd.quantity*foi.step_unit),0))*1.0/19.65)
            WHEN s.site_identifier_value = 'SPO' then ((COALESCE(SUM(boida.adjustment_value*boipd.quantity*foi.step_unit),0))*1.0/4.75)
            --WHEN s.site_identifier_value = 'BHZ' then ((COALESCE(SUM(boida.adjustment_value*boipd.quantity*foi.step_unit),0))*1.0/4.75)
            --WHEN s.site_identifier_value = 'CWB' then ((COALESCE(SUM(boida.adjustment_value*boipd.quantity*foi.step_unit),0))*1.0/4.75)
            --WHEN s.site_identifier_value = 'VCP' then ((COALESCE(SUM(boida.adjustment_value*boipd.quantity*foi.step_unit),0))*1.0/4.75)
        ELSE ((COALESCE(SUM(boida.adjustment_value*boipd.quantity*foi.step_unit),0))*1.0/3776) END AS net_discounts_usd
        
    FROM 
        postgres_broadleaf_federate."broadleaf.blc_fulfillment_group_item"                  bfgi
        INNER JOIN postgres_broadleaf_federate."broadleaf.blc_fulfillment_group"            bfg     ON bfgi.fulfillment_group_id = bfg.fulfillment_group_id
        INNER JOIN postgres_broadleaf_federate."broadleaf.fb_fulfillment_group"             ffg     ON ffg.fulfillment_group_id = bfg.fulfillment_group_id
        INNER JOIN postgres_broadleaf_federate."broadleaf.blc_order"                        bo      ON bo.order_id = bfg.order_id
        INNER JOIN postgres_broadleaf_federate."broadleaf.blc_site"                         s       ON bo.site_disc = s.site_id
        INNER JOIN postgres_broadleaf_federate."broadleaf.blc_order_item"                   boi     ON bfgi.order_item_id= boi.order_item_id
        INNER JOIN postgres_broadleaf_federate."broadleaf.fb_order_item"                    foi     ON boi.order_item_id= foi.order_item_id
        INNER JOIN postgres_broadleaf_federate."broadleaf.fb_owner"                         fow     ON fow.owner_id = ffg.owner_id
        INNER JOIN postgres_broadleaf_federate."broadleaf.fb_order"                         fo      ON fo.order_id = bo.order_id
        INNER JOIN ful                                                                              ON ful.order_id = bo.order_id
        LEFT JOIN  postgres_broadleaf_federate."broadleaf.fb_order_type"                    fot     ON fot.fb_order_type_id=fo.fb_order_type_id
        INNER JOIN postgres_broadleaf_federate."broadleaf.blc_discrete_order_item"          bdoi    ON bdoi.order_item_id = boi.order_item_id
        INNER JOIN postgres_broadleaf_federate."broadleaf.blc_sku"                          bs      ON bs.sku_id = bdoi.sku_id
        INNER JOIN postgres_broadleaf_federate."broadleaf.blc_product"                      bp      ON bs.addl_product_id = bp.product_id
        LEFT JOIN postgres_broadleaf_federate."broadleaf.blc_sku"                           bs2     ON bs2.sku_id = bp.default_sku_id --Conectar la tarjeta con el sku que guarda la info
        left join postgres_broadleaf_federate."broadleaf.blc_category_xref"                 bcx     ON bcx.sub_category_id = bp.default_category_id and bcx.archived='N' and bcx.sndbx_tier is null and bcx.default_reference = 'true'
        left join postgres_broadleaf_federate."broadleaf.blc_category"                      bcat    ON bp.default_category_id = bcat.category_id
        left join postgres_broadleaf_federate."broadleaf.blc_category"                      bcat2   ON bcx.category_id = bcat2.category_id
        LEFT JOIN postgres_broadleaf_federate."broadleaf.blc_order_item_price_dtl"          boipd   ON boipd.order_item_id=boi.order_item_id
        LEFT JOIN postgres_broadleaf_federate."broadleaf.blc_order_item_dtl_adj"            boida   ON boida.order_item_price_dtl_id=boipd.order_item_price_dtl_id
    WHERE 
        bo.order_status='SUBMITTED'
        AND fo.fb_order_status_id IN (1,6,7,8)
        AND (fot.name IS NULL OR fot.name <> 'REFUND')
        AND bcat2.name in ('Abarrotes','Aseo e Higiene','Bebidas','Congelados','Desechables','Lácteos & Huevos','Carne, Pollo & Pescados', 'Frutas & Verduras', 'Bebidas','Mercearia','Limpeza e Higiene','Laticínios e Ovos','Frutas e Verduras','Congelados','Descartáveis','Carnes, Aves e Peixes')
        AND (boida.adjustment_reason ILIKE '%Investment%')
    GROUP BY 1,2,3,4,5,6,7,8
    )

    SELECT 
        *,
        net_discounts_usd/net_gmv_usd AS investment_value,
        net_discounts_usd*ROUND((hook/(net_discounts_usd/net_gmv_usd)),8) AS hook_discount_usd,
        net_discounts_usd - (net_discounts_usd*ROUND((hook/(net_discounts_usd/net_gmv_usd)),8)) AS investment_discount_usd

    FROM all_data
    WHERE responsable IN ('ADDITIONAL INVESTMENT')
    """
    dataframe = read_connection_data_warehouse.runQuery(query)
    
    
    ##LIMPIO Y ARREGLO LA INFORMACION PARA EL MENSAJE!
    data_df = pd.merge(city_paramss,dataframe, left_on=['card_id','city'], right_on=['addl_product_id','region_code'])#.drop(columns = ['id_tarjeta'])
    data_df = data_df[['week','city','cat','subcat','name','target_gmv','target_discount','quantity','net_gmv_usd', 'net_discounts_usd','hook_discount_usd', 'investment_discount_usd']]
    data_df["quantity"] = data_df["quantity"].astype(np.float64)
    data_df["net_gmv_usd"] = data_df["net_gmv_usd"].astype(np.float64)
    data_df["net_discounts_usd"] = data_df["net_discounts_usd"].astype(np.float64)
    data_df["investment_discount_usd"] = data_df["investment_discount_usd"].astype(np.float64)
    data_df["% cumplimiento GMV"] = data_df.net_gmv_usd/data_df.target_gmv
    data_df["% cumplimiento Discount"] = data_df.investment_discount_usd/data_df.target_discount


    data_df['Accion'] = np.where(
         data_df["% cumplimiento GMV"]>1, 
        'Apagar Descuento', 
         np.where(
            data_df["% cumplimiento Discount"]>1, 
            'Alerta!!', 
            'OK'
        )
    )

    data_df["% cumplimiento GMV"] = pd.Series(['{:.2%}'.format(val) for val in data_df["% cumplimiento GMV"]], index = data_df.index) 
    data_df["% cumplimiento Discount"] = pd.Series(['{:.2%}'.format(val) for val in data_df["% cumplimiento Discount"]], index = data_df.index) 
    
    final_df = data_df[(data_df["Accion"] != "OK")][['week','city','cat','subcat','name','% cumplimiento GMV','% cumplimiento Discount','Accion']]


    for city in ['BOG','MDE','SPO']:
        a = final_df[final_df.city == city]

        b = [f"""{x[4]}:
        --> % cumplimiento GMV: {x[5]} 
        --> % cumplimiento Discount: {x[6]}

        *ACCION:* {x[7]} 
        """ for x in a.values]

        b = "\n".join(b)

        message = f"""
        Week {final_df.week.unique()[0]} - Investment Alert for {city}:

            {b}
        
        --------------------------------------------------------------
        --------------------------------------------------------------
        --------------------------------------------------------------
        """
        # ------------------------- INFO FINAL -------------------------------------------------
        # ------------------------- INFO FINAL -------------------------------------------------
        # ------------------------- INFO FINAL -------------------------------------------------

        environment = os.environ.get('environment', 'DEV_SAMPLE')
        people = {'ALL': ["felipe"]}.get('ALL', [])
        send_slack_notification('ALERT_BUDGET', message, people, ':thisisfine:')
    
    