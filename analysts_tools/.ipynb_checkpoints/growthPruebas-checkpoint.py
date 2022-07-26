import numpy as np
import pandas as pd

import sys
# route='/srv/scratch/'
# sys.path.append(route)
# route='/srv/scratch/analyst_community/'
# sys.path.append(route)
from analystcommunity import read_connection_data_warehouse

def data_ventas_query(ciudad, tipo_cambio, desde, hasta, tipo_negocio):
    """
    Owner: Tomas
    Está función trae la data de ventas, para segmentos de restaurantes, para una ciudad en un rango de fecha determinado.
    
    Argumentos:
    ciudad: codigo de ciudad de una de las ciudades en las que está Frubana ("BOG", "CMX", etc.)
    tipo_cambio: Tipo de cambio determinado por la compañía para pasar de moneda local a dolares el valor de las transacciones.
    desde: fecha desde la cual se traerá la data, el formato debe ser "%Y-%m-%d"
    hasta: fecha hasta la cual se traerá la data, el formato debe ser "%Y-%m-%d"
    
    Resultados:
    dataframe: dataframe con la data de ventas solicitada
    """
    
    query = """
    SELECT 
        bo.submit_date,
        ffg.close_date,
        TO_CHAR(ffg.close_date, 'YYYY-mm') AS month,
        bo.order_id,
        boi.order_item_id,
        ffg.tracking_code,
        bo.customer_id,
        bo.email_address,
        boi.name,
        (boi.quantity * foi.step_unit * boi.sale_price)/{tipo_cambio} as gmv_usd,
        COALESCE((baid.adjustment_value * boip.quantity * foi.step_unit)/{tipo_cambio}, 0) as discount_applied,
        boi.quantity* foi.step_unit as cant,
        UPPER(fpro.unit) as unidades,
        bs.sku_id,
        bs.addl_product_id as padre_sku_id,
        s.site_identifier_value as region_code,
        CASE
            WHEN COALESCE(bcat2.name, bcat.name) ILIKE 'Proteínas%' THEN 'Proteínas'
            ELSE COALESCE(bcat2.name, bcat.name) 
        END AS category,
        CASE
            WHEN bcat.name ILIKE 'Pollo%' THEN 'Pollo'
            WHEN bcat.name ILIKE 'Res%' THEN 'Res'
            WHEN bcat.name ILIKE 'Cerdo%' THEN 'Cerdo'
            ELSE bcat.name
        END AS subcat,
        baid.adjustment_reason
    FROM postgres_broadleaf_federate."broadleaf.blc_fulfillment_group_item"     bfgi
    INNER JOIN postgres_broadleaf_federate."broadleaf.blc_fulfillment_group"    bfg        ON bfgi.fulfillment_group_id = bfg.fulfillment_group_id
    INNER JOIN postgres_broadleaf_federate."broadleaf.fb_fulfillment_group"     ffg        ON ffg.fulfillment_group_id = bfg.fulfillment_group_id
    INNER JOIN postgres_broadleaf_federate."broadleaf.blc_order"                bo         ON bo.order_id = bfg.order_id
    INNER JOIN postgres_broadleaf_federate."broadleaf.blc_site"                 s          ON bo.site_disc = s.site_id
    INNER JOIN postgres_broadleaf_federate."broadleaf.blc_order_item"           boi        ON bfgi.order_item_id= boi.order_item_id
    INNER JOIN postgres_broadleaf_federate."broadleaf.fb_order_item"            foi        ON boi.order_item_id= foi.order_item_id
    INNER JOIN postgres_broadleaf_federate."broadleaf.fb_owner"                 fow        ON fow.owner_id = ffg.owner_id
    INNER JOIN postgres_broadleaf_federate."broadleaf.fb_order"                 fo         ON fo.order_id = bo.order_id
    INNER JOIN postgres_broadleaf_federate."broadleaf.blc_fulfillment_order"    bfo        ON bfo.fulfillment_group_id= bfg.fulfillment_group_id
    INNER JOIN postgres_broadleaf_federate."broadleaf.blc_discrete_order_item"  bdoi       ON bdoi.order_item_id = boi.order_item_id
    INNER JOIN postgres_broadleaf_federate."broadleaf.blc_sku"                  bs         ON bs.sku_id = bdoi.sku_id
    INNER JOIN postgres_broadleaf_federate."broadleaf.blc_product"              bp         ON bs.addl_product_id = bp.product_id
    INNER JOIN postgres_broadleaf_federate."broadleaf.blc_category"             bcat       ON bcat.category_id = bp.default_category_id
    INNER JOIN postgres_broadleaf_federate."broadleaf.fb_product"               fpro       ON fpro.product_id = bp.product_id
    LEFT JOIN  postgres_broadleaf_federate."broadleaf.blc_order_payment"        bop        ON bop.order_id = bo.order_id
    LEFT JOIN  postgres_broadleaf_federate."broadleaf.fb_order_type"            fot        ON fot.fb_order_type_id=fo.fb_order_type_id
    LEFT JOIN  postgres_broadleaf_federate."broadleaf.fb_customer"              fc         ON fc.customer_id = bo.customer_id
    LEFT JOIN  postgres_broadleaf_federate."broadleaf.fb_business_type"         bt         ON fc.business_type_id = bt.business_type_id
    LEFT JOIN  postgres_broadleaf_federate."broadleaf.blc_category_xref"        bcx        ON bcx.sub_category_id = bp.default_category_id AND bcx.archived='N' AND bcx.sndbx_tier is NULL
    LEFT JOIN postgres_broadleaf_federate."broadleaf.blc_category"             bcat2      ON bcx.category_id = bcat2.category_id
    LEFT JOIN postgres_broadleaf_federate."broadleaf.blc_order_item_price_dtl"  boip       ON  boi.order_item_id = boip.order_item_id
    LEFT JOIN postgres_broadleaf_federate."broadleaf.blc_order_item_dtl_adj"    baid       ON  baid.order_item_price_dtl_id = boip.order_item_price_dtl_id

    WHERE 
       (ffg.close_date >= '{desde} 00:00:01' AND ffg.close_date <= '{hasta} 23:59:59')
       AND bo.order_status='SUBMITTED'
       AND fo.fb_order_status_id IN (1,6,7,8)
       AND s.site_identifier_value IN ('{ciudad}')
       AND bfo.status NOT IN ('ARCHIVED','CANCELLED')
       AND bop.archived = 'N'
       AND (fot.name IS NULL OR fot.name <> 'REFUND')
       AND fc.business_type_id IN (1)
       -- FIX SUPER DESCUENTOS
       AND bcat2.category_id not in ('110873','-1000','100768','100765','100815') --ids de super descuentos en cada país
       AND bcat.name <> 'Oferton Frubana' AND bcat2.name <> 'Oferton Frubana'
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
    ORDER BY 1
    """.format(tipo_cambio=tipo_cambio, desde=desde, hasta=hasta, ciudad=ciudad, tipo_negocio=tipo_negocio)
    
    dataframe = read_connection_data_warehouse.runQuery(query)
    dataframe[["gmv_usd", "cant"]] = dataframe[["gmv_usd", "cant"]].astype(float)
    dataframe["submit_date"] = pd.to_datetime(dataframe["submit_date"])
    dataframe["close_date"] = pd.to_datetime(dataframe["close_date"])
    
    return dataframe
def data_ventas_query_full(desde, hasta, tipo_negocio):
    """
    Owner: Andres
    Está función trae la data de ventas, para segmentos de restaurantes, para una ciudad en un rango de fecha determinado.
    
    Argumentos:
    ciudad: codigo de ciudad de una de las ciudades en las que está Frubana ("BOG", "CMX", etc.)
    tipo_cambio: Tipo de cambio determinado por la compañía para pasar de moneda local a dolares el valor de las transacciones.
    desde: fecha desde la cual se traerá la data, el formato debe ser "%Y-%m-%d"
    hasta: fecha hasta la cual se traerá la data, el formato debe ser "%Y-%m-%d"
    
    Resultados:
    dataframe: dataframe con la data de ventas solicitada
    """
    exchange_rate_dict = {
    "BOG":3000,
    "BAQ":3000,
    "CMX":19.19,
    "SPO":3.88,
    "GDL":19.19
    }
    
    query = """
    SELECT 
        bo.submit_date,
        ffg.close_date,
        TO_CHAR(ffg.close_date, 'YYYY-mm') AS month,
        bo.order_id,
        boi.order_item_id,
        ffg.tracking_code,
        bo.customer_id,
        bo.email_address,
        boi.name,
        (boi.quantity * foi.step_unit * boi.sale_price)/{tipo_cambio} as gmv_usd,
        COALESCE((baid.adjustment_value * boip.quantity * foi.step_unit)/{tipo_cambio}, 0) as discount_applied,
        boi.quantity* foi.step_unit as cant,
        UPPER(fpro.unit) as unidades,
        bs.sku_id,
        bs.addl_product_id as padre_sku_id,
        s.site_identifier_value as region_code,
        CASE
            WHEN COALESCE(bcat2.name, bcat.name) ILIKE 'Proteínas%' THEN 'Proteínas'
            ELSE COALESCE(bcat2.name, bcat.name) 
        END AS category,
        CASE
            WHEN bcat.name ILIKE 'Pollo%' THEN 'Pollo'
            WHEN bcat.name ILIKE 'Res%' THEN 'Res'
            WHEN bcat.name ILIKE 'Cerdo%' THEN 'Cerdo'
            ELSE bcat.name
        END AS subcat,
        baid.adjustment_reason
    FROM postgres_broadleaf_federate."broadleaf.blc_fulfillment_group_item"     bfgi
    INNER JOIN postgres_broadleaf_federate."broadleaf.blc_fulfillment_group"    bfg        ON bfgi.fulfillment_group_id = bfg.fulfillment_group_id
    INNER JOIN postgres_broadleaf_federate."broadleaf.fb_fulfillment_group"     ffg        ON ffg.fulfillment_group_id = bfg.fulfillment_group_id
    INNER JOIN postgres_broadleaf_federate."broadleaf.blc_order"                bo         ON bo.order_id = bfg.order_id
    INNER JOIN postgres_broadleaf_federate."broadleaf.blc_site"                 s          ON bo.site_disc = s.site_id
    INNER JOIN postgres_broadleaf_federate."broadleaf.blc_order_item"           boi        ON bfgi.order_item_id= boi.order_item_id
    INNER JOIN postgres_broadleaf_federate."broadleaf.fb_order_item"            foi        ON boi.order_item_id= foi.order_item_id
    INNER JOIN postgres_broadleaf_federate."broadleaf.fb_owner"                 fow        ON fow.owner_id = ffg.owner_id
    INNER JOIN postgres_broadleaf_federate."broadleaf.fb_order"                 fo         ON fo.order_id = bo.order_id
    INNER JOIN postgres_broadleaf_federate."broadleaf.blc_fulfillment_order"    bfo        ON bfo.fulfillment_group_id= bfg.fulfillment_group_id
    INNER JOIN postgres_broadleaf_federate."broadleaf.blc_discrete_order_item"  bdoi       ON bdoi.order_item_id = boi.order_item_id
    INNER JOIN postgres_broadleaf_federate."broadleaf.blc_sku"                  bs         ON bs.sku_id = bdoi.sku_id
    INNER JOIN postgres_broadleaf_federate."broadleaf.blc_product"              bp         ON bs.addl_product_id = bp.product_id
    INNER JOIN postgres_broadleaf_federate."broadleaf.blc_category"             bcat       ON bcat.category_id = bp.default_category_id
    INNER JOIN postgres_broadleaf_federate."broadleaf.fb_product"               fpro       ON fpro.product_id = bp.product_id
    LEFT JOIN  postgres_broadleaf_federate."broadleaf.blc_order_payment"        bop        ON bop.order_id = bo.order_id
    LEFT JOIN  postgres_broadleaf_federate."broadleaf.fb_order_type"            fot        ON fot.fb_order_type_id=fo.fb_order_type_id
    LEFT JOIN  postgres_broadleaf_federate."broadleaf.fb_customer"              fc         ON fc.customer_id = bo.customer_id
    LEFT JOIN  postgres_broadleaf_federate."broadleaf.fb_business_type"         bt         ON fc.business_type_id = bt.business_type_id
    LEFT JOIN  postgres_broadleaf_federate."broadleaf.blc_category_xref"        bcx        ON bcx.sub_category_id = bp.default_category_id AND bcx.archived='N' AND bcx.sndbx_tier is NULL
    LEFT JOIN postgres_broadleaf_federate."broadleaf.blc_category"             bcat2      ON bcx.category_id = bcat2.category_id
    LEFT JOIN postgres_broadleaf_federate."broadleaf.blc_order_item_price_dtl"  boip       ON  boi.order_item_id = boip.order_item_id
    LEFT JOIN postgres_broadleaf_federate."broadleaf.blc_order_item_dtl_adj"    baid       ON  baid.order_item_price_dtl_id = boip.order_item_price_dtl_id

    WHERE 
       (ffg.close_date >= '{desde} 00:00:01' AND ffg.close_date <= '{hasta} 23:59:59')
       AND bo.order_status='SUBMITTED'
       AND fo.fb_order_status_id IN (1,6,7,8)
       AND bfo.status NOT IN ('ARCHIVED','CANCELLED')
       AND bop.archived = 'N'
       AND (fot.name IS NULL OR fot.name <> 'REFUND')
       AND fc.business_type_id IN (1)
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
    ORDER BY 1

    """.format(tipo_cambio=1, desde=desde, hasta=hasta, tipo_negocio=tipo_negocio)
    
    dataframe = read_connection_data_warehouse.runQuery(query)
    dataframe[["gmv_usd","discount_applied", "cant"]] = dataframe[["gmv_usd","discount_applied", "cant"]].astype(float)
    
    dataframe["gmv_usd"] = dataframe[["gmv_usd","region_code"]].apply(lambda x: x.gmv_usd/exchange_rate_dict.get(x.region_code, 1),axis = 1)
    dataframe["discount_applied"] = dataframe[["discount_applied","region_code"]].apply(lambda x: x.discount_applied/exchange_rate_dict.get(x.region_code, 1),axis = 1)
    
    dataframe["submit_date"] = pd.to_datetime(dataframe["submit_date"])
    dataframe["close_date"] = pd.to_datetime(dataframe["close_date"])
    
    return dataframe

def users_birthday(ciudad):
    """
    Owner: Tomas
    Esta función busca, en el historico de ordenes de los usuarios cuando se realizó la primera orden. Esta fecha será el birthday
    del usuario
    
    Argumentos:
    ciudad: codigo de ciudad de una de las ciudades en las que está Frubana ("BOG", "CMX", etc.)
    
    Resultados:
    ub: diccionario key-> customer_id y value-> birthday_date
    """
    
    query = """
    SELECT
        bo.customer_id AS customer_id,
        MIN(bo.submit_date) AS birthday
    FROM postgres_broadleaf_federate."broadleaf.blc_fulfillment_group_item"     bfgi
    INNER JOIN postgres_broadleaf_federate."broadleaf.blc_fulfillment_group"    bfg       ON bfgi.fulfillment_group_id = bfg.fulfillment_group_id
    INNER JOIN postgres_broadleaf_federate."broadleaf.fb_fulfillment_group"     ffg       ON ffg.fulfillment_group_id = bfg.fulfillment_group_id
    INNER JOIN postgres_broadleaf_federate."broadleaf.blc_order"                bo        ON bo.order_id = bfg.order_id
    INNER JOIN postgres_broadleaf_federate."broadleaf.blc_site"                 s         ON bo.site_disc = s.site_id
    INNER JOIN postgres_broadleaf_federate."broadleaf.blc_order_item"           boi       ON bfgi.order_item_id= boi.order_item_id
    INNER JOIN postgres_broadleaf_federate."broadleaf.fb_order_item"            foi       ON boi.order_item_id= foi.order_item_id
    INNER JOIN postgres_broadleaf_federate."broadleaf.fb_owner"                 fow       ON fow.owner_id = ffg.owner_id
    INNER JOIN postgres_broadleaf_federate."broadleaf.fb_order"                 fo        ON fo.order_id = bo.order_id
    INNER JOIN postgres_broadleaf_federate."broadleaf.blc_fulfillment_order"    bfo       ON bfo.fulfillment_group_id= bfg.fulfillment_group_id
    INNER JOIN postgres_broadleaf_federate."broadleaf.blc_discrete_order_item"  bdoi      ON bdoi.order_item_id = boi.order_item_id
    INNER JOIN postgres_broadleaf_federate."broadleaf.blc_sku"                  bs        ON bs.sku_id = bdoi.sku_id
    INNER JOIN postgres_broadleaf_federate."broadleaf.blc_product"              bp        ON bs.addl_product_id = bp.product_id
    INNER JOIN postgres_broadleaf_federate."broadleaf.blc_category"             bcat      ON bcat.category_id = bp.default_category_id
    INNER JOIN postgres_broadleaf_federate."broadleaf.fb_product"               fpro      ON fpro.product_id = bp.product_id
    LEFT JOIN  postgres_broadleaf_federate."broadleaf.blc_order_payment"        bop       ON bop.order_id = bo.order_id
    LEFT JOIN  postgres_broadleaf_federate."broadleaf.fb_order_type"            fot       ON fot.fb_order_type_id=fo.fb_order_type_id
    LEFT JOIN  postgres_broadleaf_federate."broadleaf.fb_customer"              fc        ON fc.customer_id = bo.customer_id
    LEFT JOIN  postgres_broadleaf_federate."broadleaf.fb_business_type"         bt        ON fc.business_type_id = bt.business_type_id

    WHERE bo.order_status='SUBMITTED'
        AND fo.fb_order_status_id IN (1,6,7,8)
        AND s.site_identifier_value IN ('{ciudad}')
        AND bfo.status NOT IN ('ARCHIVED','CANCELLED')
        AND bop.archived = 'N'
        AND (fot.name IS NULL OR fot.name <> 'REFUND')
        AND fc.business_type_id IN (1)
    GROUP BY bo.customer_id
    """.format(ciudad=ciudad)

    dataframe = read_connection_data_warehouse.runQuery(query)
    ub = dict(zip(dataframe["customer_id"], dataframe["birthday"]))
    return ub

def sacar_non_buyers(data_ventas, filtros, especificar_rango="No"):
    """
    Owner: Tomas
    Esta función permite sacar la lista de customers nonbuyers de categorias, subcategorias o skus especificos.
    
    Argumentos:
    data_ventas: data de ventas que entrega la función data_ventas_query.
    filtros: diccionario con el siguiente formato ->
    {
    "1":{"nombre":<Nombre deseado para el subconjunto>,
    "featureFiltrado":category, subcat o sku_id,
    "valueFiltrado":lista de categorias, subcat o sku_id a analizar},

    "2":{"nombre":<Nombre deseado para el subconjunto>,
    "featureFiltrado":category, subcat o sku_id,
    "valueFiltrado":lista de categorias, subcat o sku_id a analizar}
    }
    especificar_rango (opcional): En caso de no querer analizar la totalidad del tiempo que hay en data_ventas_query se puede usar
    un diccionario para delimitar el rango de timpo. El formato debe tener la siguiente estructura -> {"desde":"YYYY-MM-DD", "hasta":"YYYY-MM-DD"}
    
    Resultados:
    non_buyers_dataframe: dataframe con los nonbuyers para los n grupos seleccionados con la variable filtros durante el rango de fechas espcificado.
    """
    total_users = data_ventas["customer_id"].unique().tolist()
    if especificar_rango == "No":
        data_analisis = data_ventas.copy()
    elif type(especificar_rango) == type({}):
        data_analisis = data_ventas[(data_ventas["close_date"] >= especificar_rango["desde"]) & (data_ventas["close_date"] <= especificar_rango["hasta"])].copy()
    else:
        raise Exception("""
        En caso de determinar un rango de fechas personalizado se debe usar un diccionario
        con la estructura {"desde":"YYYY-MM-DD", "hasta":"YYYY-MM-DD"}""")
        
    non_buyers_dict = {}
    for i in filtros.keys():
        ids_buyers = data_analisis[data_analisis[filtros[i]["featureFiltrado"]].isin(filtros[i]["valueFiltrado"])]["customer_id"].unique().tolist()
        ids_non_buyers = set(total_users).difference(set(ids_buyers))
        assert len(total_users) == len(ids_buyers) + len(ids_non_buyers)
        non_buyers_dict[filtros[i]['nombre']] = ids_non_buyers
        assert filtros[i]['valueFiltrado'] not in data_analisis[data_analisis['customer_id'].isin(non_buyers_dict[filtros[i]['nombre']])][filtros[i]['featureFiltrado']].unique().tolist()
    
    non_buyers_dataframe = pd.DataFrame.from_dict(non_buyers_dict, orient='index').T
    return non_buyers_dataframe


def data_clevertap(desde, hasta):
    """
    Owner: Tomas
    Está función trae la data de clevertap para saber cuales usuarios han 1) entrado a la aplicación y 2) entrado a SuperDescuentos
    
    Argumentos:
    desde: fecha desde la cual se traerá la data, el formato debe ser "%Y-%m-%d"
    hasta: fecha hasta la cual se traerá la data, el formato debe ser "%Y-%m-%d"
    
    Resultados:
    dataframe: dataframe con la data de clevertap solicitada
    """
    
    query = """SELECT
    "profile.identity" AS customer_id,
    eventname,
    "eventprops.categoryname" as category_name,
    "eventprops.frcategoryname" as category_name2
    FROM clevertap
    WHERE (to_date(ts::varchar, 'YYYYMMDDHH24MISS') >= '{desde}' AND to_date(ts::varchar, 'YYYYMMDDHH24MISS') <= '{hasta}')
    AND (eventname IN ('FRF Home View') OR ("eventprops.frcategoryname" ILIKE '%S%per%descuentos%' OR "eventprops.categoryname" ILIKE '%S%per%descuentos%'))
    AND "profile.identity" IS NOT NULL
    AND "profile.identity" NOT ILIKE ('%,%')
    GROUP BY 1,2,3,4
    """.format(desde=desde, hasta=hasta)
    
    dataframe = read_connection_data_warehouse.runQuery(query)
    dataframe["customer_id"] = dataframe["customer_id"].astype(int)
    return dataframe


def analisis_detallado_clevertap(data_analizar, desde, hasta):
    """
    Owner: Tomas
    Esta función, a partir de un dataframe compuesta UNICAMENTE por customers_id, analiza para un rango de fechas
    determinado quienes han 1) entrado al app 2) entrado al app y a superdescuentos 3) no ha entrado a superdescuentos (incluye los que no han entrado al app)
    
    Argumentos:
    data_analizar: dataframe compuesta unicamente por customer_ids
    desde: fecha desde la cual se traerá la data, el formato debe ser "%Y-%m-%d"
    hasta: fecha hasta la cual se traerá la data, el formato debe ser "%Y-%m-%d"
    
    Resultados:
    dict_resumen_dataframe: dataFrame con los customer_ids de cada conjunto del analisis.
    """
    all_users_list = list(set([item for items in data_analizar.values for item in items if str(item) != 'nan']))
    clevertap = data_clevertap(desde=desde, hasta=hasta)
    
    dict_resumen = {}
    user_in_app = clevertap[clevertap["customer_id"].isin(all_users_list)]["customer_id"].unique().tolist()
    dict_resumen["users_in_app"] = user_in_app
    user_in_app_and_superdescuentos = clevertap[(clevertap["customer_id"].isin(all_users_list)) & (clevertap['eventname'] != 'FRF Home View')]["customer_id"].unique().tolist()
    dict_resumen["user_in_app_and_superdescuentos"] = user_in_app_and_superdescuentos
    assert len(all_users_list) - len(user_in_app_and_superdescuentos) == len(list(set(all_users_list).difference(set(user_in_app_and_superdescuentos))))
    users_not_in_superdescuentos = list(set(all_users_list).difference(set(user_in_app_and_superdescuentos)))
    dict_resumen["users_not_in_superdescuentos"] = users_not_in_superdescuentos
    
    resumen_dataframe = pd.DataFrame.from_dict(dict_resumen, orient='index').T
    return resumen_dataframe

def asignar_cuartil(data_ventas, lista_customer_id=None):
    """
    Owner: Tomas
    Esta función permite segmentar los clientes, en función de la data de ventas para un periodo determinado, en el cuartil al que
    pertenecen. Si se especifica una lista de clientes, el analisis se hará solo para esos clientes; en caso contrario se hará para
    todos los clientes que compraron en el periodo de la data de ventas.
    
    El analisis lo que hace es sumar el arpu mes a mes y sacar el promedio mensual de arpu, por cada cliente.
    
    Argumentos:
    data_ventas: data de ventas que entrega la función data_ventas_query.
    lista_customer_id (opcional): lista de clientes que se quieran analizar particularmente.
    
    Resultados:
    dict_cuartil: diccionario donde la key es el customer_id y el value es el cuartil al que pertenece
    """
    
    data_ventas["month"] = data_ventas["submit_date"].dt.month
    
    if lista_customer_id == None:
        df_agrupado_por_arpu = data_ventas.groupby(by=['customer_id', 'month']).agg({'gmv_usd':np.sum}).unstack().mean(axis=1)
        dict_cuartil = pd.qcut(x=df_agrupado_por_arpu.squeeze(), q=4, labels=["Q4", "Q3", "Q2", "Q1"]).to_dict()
    elif type(lista_customer_id) == type([]):
        data_ventas_filtrada = data_ventas[data_ventas["customer_id"].isin(lista_customer_id)]
        df_agrupado_por_arpu = data_ventas_filtrada.groupby(by=['customer_id', 'month']).agg({'gmv_usd':np.sum}).unstack().mean(axis=1)
        dict_cuartil = pd.qcut(x=df_agrupado_por_arpu.squeeze(), q=4, labels=["Q4", "Q3", "Q2", "Q1"]).to_dict()
    
    data_ventas.drop(labels=["month"], axis=1, inplace=True)
    
    return dict_cuartil

def analizar_top_productos(data_ventas):
    """
    Owner: Tomas
    Está función recibe un dataframe con información de ventas (sale de la función data_ventas_query) y analiza, por sku, cuales son los 
    top productos.
    los criterios bajo el cual la función define top productos son 2: (1) penetración del sku_id (2) gmv_usd que mueve el sku_id.
    
    Argumentos:
    data_ventas: data de ventas que entrega la función data_ventas_query.
    
    Resultado:
    top_productos_df: dataframe ordenada con los top productos
    """
    def group_top_products(x):
        resultado = {}
        resultado["name"] = x["name"].unique()[0]
        resultado["category"] = x["category"].unique()[0]
        assert len(x["category"].unique()) == 1
        resultado["subcat"] = x["subcat"].unique()[0]
        assert len(x["subcat"].unique()) == 1
        resultado["penetration"] = x["customer_id"].nunique()
        resultado["gmv_usd"] = x["gmv_usd"].sum()
        assert len(x["padre_sku_id"].unique()) == 1
        resultado["padre_sku_id"] = x["padre_sku_id"].unique()[0]
        return pd.Series(resultado, index=list(resultado.keys()))
    
    top_productos_df = data_ventas.groupby(by=["sku_id"]).apply(group_top_products).reset_index()
    top_productos_df = top_productos_df.sort_values(by=["penetration", "gmv_usd"], ascending=False)
    num_customer_ids = data_ventas["customer_id"].nunique()
    top_productos_df["penetration_porcentual"] = top_productos_df["penetration"]/num_customer_ids
    return top_productos_df

def productos_correlacionados(data_ventas, feature, lista):
    """
    Owner: Tomas
    Está función permite ver que productos se relacionan, orden a orden, con la compra de una determinada categoria o subcategoria.
    
    Argumentos:
    data_ventas: data de ventas que entrega la función data_ventas_query.
    feature: "category" o "subcat" en función de lo que se quiera analizar.
    lista: lista de categorias o subcategorias, en función el feature, sobre las que se quiera sacar los productos relacionados.
    
    Resultados:
    related_products: dataframe ordenada por la relación de los productos con la categoria o subcategoria.
    """
    def ordenes_con_subcat(x, objetivo):
        return x[feature].isin(objetivo).any()
    
    boolean_dict = data_ventas.groupby(by=["order_id"]).apply(ordenes_con_subcat, objetivo=lista).to_dict()
    assert data_ventas["order_id"].nunique() == len(boolean_dict)
    
    data_ventas["order_contains_objective"] = data_ventas["order_id"].map(boolean_dict)
    sub_df = data_ventas[(data_ventas["order_contains_objective"]) & (~data_ventas[feature].isin(lista))].copy()
    
    related_products = sub_df.groupby(by=["sku_id"]).agg({"customer_id":pd.Series.nunique, "name":"last"}).sort_values(by=["customer_id"], ascending=False)
    related_products.columns = ["number_of_buyers", "name"]
    related_products["penetration"] = related_products["number_of_buyers"]/sub_df["customer_id"].nunique()

    related_products = related_products[["name", "number_of_buyers", "penetration"]].reset_index()
    return related_products

def dict_cliente_email_from_ventas(data_ventas):
    """
    Owner: Tomas
    Esta función pasa de customer_id al último email_address
    
    Argumentos:
    data_ventas: data de ventas que entrega la función data_ventas.
    
    Resultados:
    id_email: diccionario id:email. El email es el email más reciente.
    """
    sub_df = data_ventas[["submit_date", "customer_id", "email_address"]].sort_values(by=["submit_date"]).drop_duplicates(subset=["customer_id"], keep="last")
    id_email = dict(zip(sub_df["customer_id"], sub_df["email_address"]))
    return id_email

def query_para_retention_analysis(ciudad, tipo_cambio, desde, hasta, tipo_negocio):
    """
    Owner: Tomas
    Esta función es una abstracción de la función data_ventas_query agregando el registro de campañas aplicadas por lineas de orden. No se recomienda usar está función para reemplazar la primera pues hay algunas pequeñas inconsistencia en la data (En caso de arreglar la inconsistencia se recomienda modificar data_ventas_query y agregar las columnas de ajustes por campañas).
    
    Argumentos:
    ciudad: codigo de ciudad de una de las ciudades en las que está Frubana ("BOG", "CMX", etc.)
    tipo_cambio: Tipo de cambio determinado por la compañía para pasar de moneda local a dolares el valor de las transacciones.
    desde: fecha desde la cual se traerá la data, el formato debe ser "%Y-%m-%d"
    hasta: fFecha hasta la cual se traerá la data, el formato debe ser "%Y-%m-%d"
    
    Resultados:
    dataframe: dataframe con la data de ventas solicitada
    """
    
    query = """
    SELECT 
        bo.submit_date,
        ffg.close_date,
        bo.order_id,
        bo.customer_id,
        bo.email_address,
        boi.name,
        (boi.quantity * foi.step_unit * boi.sale_price)/{tipo_cambio} as gmv_usd,
        boi.quantity* foi.step_unit as cant,
        UPPER(fpro.unit) as unidades,
        bs.sku_id,
        bs.addl_product_id as padre_sku_id,
        s.site_identifier_value as region_code,
        CASE
            WHEN COALESCE(bcat2.name, bcat.name) ILIKE 'Proteínas%' THEN 'Proteínas'
            ELSE COALESCE(bcat2.name, bcat.name)
        END AS category,
        bcat.name as subcat,
        boip.use_sale_price,
        boip.order_item_id,
        baid.adjustment_reason
    FROM postgres_broadleaf_federate."broadleaf.blc_fulfillment_group_item"     bfgi
    INNER JOIN postgres_broadleaf_federate."broadleaf.blc_fulfillment_group"    bfg        ON bfgi.fulfillment_group_id = bfg.fulfillment_group_id
    INNER JOIN postgres_broadleaf_federate."broadleaf.fb_fulfillment_group"     ffg        ON ffg.fulfillment_group_id = bfg.fulfillment_group_id
    INNER JOIN postgres_broadleaf_federate."broadleaf.blc_order"                bo         ON bo.order_id = bfg.order_id
    INNER JOIN postgres_broadleaf_federate."broadleaf.blc_site"                 s          ON bo.site_disc = s.site_id
    INNER JOIN postgres_broadleaf_federate."broadleaf.blc_order_item"           boi        ON bfgi.order_item_id= boi.order_item_id
    INNER JOIN postgres_broadleaf_federate."broadleaf.fb_order_item"            foi        ON boi.order_item_id= foi.order_item_id
    INNER JOIN postgres_broadleaf_federate."broadleaf.fb_owner"                 fow        ON fow.owner_id = ffg.owner_id
    INNER JOIN postgres_broadleaf_federate."broadleaf.fb_order"                 fo         ON fo.order_id = bo.order_id
    INNER JOIN postgres_broadleaf_federate."broadleaf.blc_fulfillment_order"    bfo        ON bfo.fulfillment_group_id= bfg.fulfillment_group_id
    INNER JOIN postgres_broadleaf_federate."broadleaf.blc_discrete_order_item"  bdoi       ON bdoi.order_item_id = boi.order_item_id
    INNER JOIN postgres_broadleaf_federate."broadleaf.blc_sku"                  bs         ON bs.sku_id = bdoi.sku_id
    INNER JOIN postgres_broadleaf_federate."broadleaf.blc_product"              bp         ON bs.addl_product_id = bp.product_id
    INNER JOIN postgres_broadleaf_federate."broadleaf.blc_category"             bcat       ON bcat.category_id = bp.default_category_id
    INNER JOIN postgres_broadleaf_federate."broadleaf.fb_product"               fpro       ON fpro.product_id = bp.product_id
    LEFT JOIN  postgres_broadleaf_federate."broadleaf.blc_order_payment"        bop        ON bop.order_id = bo.order_id
    LEFT JOIN  postgres_broadleaf_federate."broadleaf.fb_order_type"            fot        ON fot.fb_order_type_id=fo.fb_order_type_id
    LEFT JOIN  postgres_broadleaf_federate."broadleaf.fb_customer"              fc         ON fc.customer_id = bo.customer_id
    LEFT JOIN  postgres_broadleaf_federate."broadleaf.fb_business_type"         bt         ON fc.business_type_id = bt.business_type_id
    LEFT JOIN  postgres_broadleaf_federate."broadleaf.blc_category_xref"        bcx        ON bcx.sub_category_id = bp.default_category_id AND bcx.archived='N' AND bcx.sndbx_tier is NULL
    LEFT JOIN  postgres_broadleaf_federate."broadleaf.blc_category"             bcat2      ON bcx.category_id = bcat2.category_id
    LEFT JOIN postgres_broadleaf_federate."broadleaf.blc_order_item_price_dtl"  boip       on  boi.order_item_id = boip.order_item_id
    LEFT JOIN postgres_broadleaf_federate."broadleaf.blc_order_item_dtl_adj"    baid       on  baid.order_item_price_dtl_id = boip.order_item_price_dtl_id

    WHERE 
       (bo.submit_date >= '{desde} 00:00:01' AND bo.submit_date <= '{hasta} 23:59:59')
       AND bo.order_status='SUBMITTED'
       AND fo.fb_order_status_id IN (1,6,7,8)
       AND s.site_identifier_value IN ('{ciudad}')
       AND bfo.status NOT IN ('ARCHIVED','CANCELLED')
       AND bop.archived = 'N'
       AND (fot.name IS NULL OR fot.name <> 'REFUND')
       AND fc.business_type_id IN ({tipo_negocio})
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
    order by 1

    """.format(tipo_cambio=tipo_cambio, desde=desde, hasta=hasta, ciudad=ciudad, tipo_negocio=tipo_negocio)
    
    dataframe = read_connection_data_warehouse.runQuery(query)
    dataframe[["gmv_usd", "cant"]] = dataframe[["gmv_usd", "cant"]].astype(float)
    dataframe["submit_date"] = pd.to_datetime(dataframe["submit_date"])
    dataframe["close_date"] = pd.to_datetime(dataframe["close_date"])
    return dataframe

def analisis_retention_campannas(data_ventas, level="subcat", especificar_fechas=None):
    """
    Owner: Tomás
    Esta función se encarga de analizar la retención de la campañas (se entiende que un cliente es retenido cuando (i) convirtió
    en la campaña y (ii) volvió a comprar productos dentro de la misma subcategoría o categoría en la que había convertido
    sin necesidad de alguna campaña).
    
    Argumentos:
    data_ventas: data_ventas salida de la función query_para_retention_analysis
    level (opcional): nivel al cual se quiere analizar la retención
    especificar_rango (opcional): En caso de no querer analizar la totalidad del tiempo que hay en data_ventas_query se puede usar
    un diccionario para delimitar el rango de timpo. El formato debe tener la siguiente estructura -> {"desde":"YYYY-MM-DD", "hasta":"YYYY-MM-DD"}
    
    Resultados:
    df_consolidated: dataframe donde se puede ver las campañas, numero de clientes convertidos, retenidos y el retention rate de
    la campaña
    """
    
    if especificar_fechas is not None:
        data_ventas_analisis = data_ventas[data_ventas["submit_date"].between(especificar_fechas["desde"], especificar_fechas["hasta"])].copy()
    elif especificar_fechas is None:
        data_ventas_analisis = data_ventas.copy()

    list_campain = []
    list_converted = []
    list_retained = []
    list_category = []
    list_subcat = []
    
    for i in list(data_ventas_analisis["adjustment_reason"].dropna().unique()):
        list_campain.extend([i])
        df_prueba = data_ventas_analisis[data_ventas_analisis["adjustment_reason"] == i].copy()
        clientes_converted = list(df_prueba["customer_id"].unique())
        category_converted = list(df_prueba["category"].unique())
        list_category.extend([category_converted])
        subcat_converted = df_prueba["subcat"].unique()
        list_subcat.extend([subcat_converted])
        
        if level == "subcat":
            b = data_ventas_analisis[(data_ventas_analisis["customer_id"].isin(clientes_converted)) & (data_ventas_analisis[level].isin(subcat_converted)) & (data_ventas_analisis["adjustment_reason"].isnull())]["customer_id"].dropna().nunique()
        elif level == "category":
            b = data_ventas_analisis[(data_ventas_analisis["customer_id"].isin(clientes_converted)) & (data_ventas_analisis[level].isin(category_converted)) & (data_ventas_analisis["adjustment_reason"].isnull())]["customer_id"].dropna().nunique()
        
        list_retained.extend([b])
        a = len(clientes_converted)
        list_converted.extend([a])

    df_consolidated = pd.DataFrame(data={"campaign":list_campain,
                                         "number_of_converted_users":list_converted,
                                         "number_of_retained_users":list_retained,
                                         "category":list_category,
                                         "subcat":list_subcat
                                        }
                                  )

    df_consolidated["retention_rate"] = df_consolidated["number_of_retained_users"]/df_consolidated["number_of_converted_users"]
    
    assert df_consolidated["retention_rate"].max() == 1
    
    return df_consolidated[["campaign", "number_of_converted_users", "number_of_retained_users", "retention_rate", "category", "subcat"]]

def users_birthday_javier_souza():
    """
    Owner: Tomas
    Esta función busca, en el historico de ordenes de los usuarios cuando se realizó la primera orden. Esta fecha será el birthday
    del usuario
    
    Argumentos:
    ciudad: codigo de ciudad de una de las ciudades en las que está Frubana ("BOG", "CMX", etc.)
    
    Resultados:
    ub: diccionario key-> customer_id y value-> birthday_date
    """
    
    query = """
    SELECT
        bo.customer_id AS customer_id,
        MIN(bo.submit_date) AS birthday
    FROM postgres_broadleaf_federate."broadleaf.blc_fulfillment_group_item"     bfgi
    INNER JOIN postgres_broadleaf_federate."broadleaf.blc_fulfillment_group"    bfg       ON bfgi.fulfillment_group_id = bfg.fulfillment_group_id
    INNER JOIN postgres_broadleaf_federate."broadleaf.fb_fulfillment_group"     ffg       ON ffg.fulfillment_group_id = bfg.fulfillment_group_id
    INNER JOIN postgres_broadleaf_federate."broadleaf.blc_order"                bo        ON bo.order_id = bfg.order_id
    INNER JOIN postgres_broadleaf_federate."broadleaf.blc_site"                 s         ON bo.site_disc = s.site_id
    INNER JOIN postgres_broadleaf_federate."broadleaf.blc_order_item"           boi       ON bfgi.order_item_id= boi.order_item_id
    INNER JOIN postgres_broadleaf_federate."broadleaf.fb_order_item"            foi       ON boi.order_item_id= foi.order_item_id
    INNER JOIN postgres_broadleaf_federate."broadleaf.fb_owner"                 fow       ON fow.owner_id = ffg.owner_id
    INNER JOIN postgres_broadleaf_federate."broadleaf.fb_order"                 fo        ON fo.order_id = bo.order_id
    INNER JOIN postgres_broadleaf_federate."broadleaf.blc_fulfillment_order"    bfo       ON bfo.fulfillment_group_id= bfg.fulfillment_group_id
    INNER JOIN postgres_broadleaf_federate."broadleaf.blc_discrete_order_item"  bdoi      ON bdoi.order_item_id = boi.order_item_id
    INNER JOIN postgres_broadleaf_federate."broadleaf.blc_sku"                  bs        ON bs.sku_id = bdoi.sku_id
    INNER JOIN postgres_broadleaf_federate."broadleaf.blc_product"              bp        ON bs.addl_product_id = bp.product_id
    INNER JOIN postgres_broadleaf_federate."broadleaf.blc_category"             bcat      ON bcat.category_id = bp.default_category_id
    INNER JOIN postgres_broadleaf_federate."broadleaf.fb_product"               fpro      ON fpro.product_id = bp.product_id
    LEFT JOIN  postgres_broadleaf_federate."broadleaf.blc_order_payment"        bop       ON bop.order_id = bo.order_id
    LEFT JOIN  postgres_broadleaf_federate."broadleaf.fb_order_type"            fot       ON fot.fb_order_type_id=fo.fb_order_type_id
    LEFT JOIN  postgres_broadleaf_federate."broadleaf.fb_customer"              fc        ON fc.customer_id = bo.customer_id
    LEFT JOIN  postgres_broadleaf_federate."broadleaf.fb_business_type"         bt        ON fc.business_type_id = bt.business_type_id

    WHERE bo.order_status='SUBMITTED'
        AND fo.fb_order_status_id IN (1,6,7,8)
        AND s.site_identifier_value IN ('BAQ', 'BOG', 'CMX', 'GDL', 'SPO')
        AND bfo.status NOT IN ('ARCHIVED','CANCELLED')
        AND bop.archived = 'N'
        AND (fot.name IS NULL OR fot.name <> 'REFUND')
        AND fc.business_type_id IN (1)
    GROUP BY bo.customer_id
    """

    dataframe = read_connection_data_warehouse.runQuery(query)
    ub = dict(zip(dataframe["customer_id"], dataframe["birthday"]))
    return ub

def sacar_buyers_below_avg(data_ventas, filtros, especificar_rango="No"):
    """
    Owner: Tomas
    Esta función permite sacar la lista de customers buyers de categorias, subcategorias o skus especificos.
    Estos buyers son compradores por debajo del promedio.
    
    Argumentos:
    data_ventas: data de ventas que entrega la función data_ventas_query.
    filtros: diccionario con el siguiente formato ->
    {
    "1":{"nombre":<Nombre deseado para el subconjunto>,
    "featureFiltrado":category, subcat o sku_id,
    "valueFiltrado":lista de categorias, subcat o sku_id a analizar},

    "2":{"nombre":<Nombre deseado para el subconjunto>,
    "featureFiltrado":category, subcat o sku_id,
    "valueFiltrado":lista de categorias, subcat o sku_id a analizar}
    }
    especificar_rango (opcional): En caso de no querer analizar la totalidad del tiempo que hay en data_ventas_query se puede usar
    un diccionario para delimitar el rango de timpo. El formato debe tener la siguiente estructura -> {"desde":"YYYY-MM-DD", "hasta":"YYYY-MM-DD"}
    
    Resultados:
    non_buyers_dataframe: dataframe con los buyers debajo del promedio para los n grupos seleccionados con la variable filtros durante el rango de fechas espcificado.
    """
    total_users = data_ventas["customer_id"].unique().tolist()
    if especificar_rango == "No":
        data_analisis = data_ventas.copy()
    elif type(especificar_rango) == type({}):
        data_analisis = data_ventas[(data_ventas["submit_date"] >= especificar_rango["desde"]) & (data_ventas["submit_date"] <= especificar_rango["hasta"])].copy()
    else:
        raise Exception("""
        En caso de determinar un rango de fechas personalizado se debe usar un diccionario
        con la estructura {"desde":"YYYY-MM-DD", "hasta":"YYYY-MM-DD"}""")
        
    buyers_below_avg_dict = {}
    for i in filtros.keys():
        buyers_gmv_subcat = data_ventas[data_ventas[filtros[i]["featureFiltrado"]].isin(filtros[i]["valueFiltrado"])].groupby(by=["customer_id"])["gmv_usd"].sum()
        avg_gmv_subcat = buyers_gmv_subcat.mean()
        ids_buyers_below_avg = buyers_gmv_subcat[buyers_gmv_subcat < avg_gmv_subcat].index.tolist()
        buyers_below_avg_dict[filtros[i]["nombre"]] = ids_buyers_below_avg

    buyers_below_avg_dataframe = pd.DataFrame.from_dict(buyers_below_avg_dict, orient='index').T
    return buyers_below_avg_dataframe

def sacar_buyers_below_median(data_ventas, filtros, especificar_rango="No"):
    """
    Owner: Tomas
    Esta función permite sacar la lista de customers buyers de categorias, subcategorias o skus especificos.
    Estos buyers son compradores por debajo de la mediana.
    
    Argumentos:
    data_ventas: data de ventas que entrega la función data_ventas_query.
    filtros: diccionario con el siguiente formato ->
    {
    "1":{"nombre":<Nombre deseado para el subconjunto>,
    "featureFiltrado":category, subcat o sku_id,
    "valueFiltrado":lista de categorias, subcat o sku_id a analizar},

    "2":{"nombre":<Nombre deseado para el subconjunto>,
    "featureFiltrado":category, subcat o sku_id,
    "valueFiltrado":lista de categorias, subcat o sku_id a analizar}
    }
    especificar_rango (opcional): En caso de no querer analizar la totalidad del tiempo que hay en data_ventas_query se puede usar
    un diccionario para delimitar el rango de timpo. El formato debe tener la siguiente estructura -> {"desde":"YYYY-MM-DD", "hasta":"YYYY-MM-DD"}
    
    Resultados:
    non_buyers_dataframe: dataframe con los buyers debajo del promedio para los n grupos seleccionados con la variable filtros durante el rango de fechas espcificado.
    """
    total_users = data_ventas["customer_id"].unique().tolist()
    if especificar_rango == "No":
        data_analisis = data_ventas.copy()
    elif type(especificar_rango) == type({}):
        data_analisis = data_ventas[(data_ventas["close_date"] >= especificar_rango["desde"]) & (data_ventas["close_date"] <= especificar_rango["hasta"])].copy()
    else:
        raise Exception("""
        En caso de determinar un rango de fechas personalizado se debe usar un diccionario
        con la estructura {"desde":"YYYY-MM-DD", "hasta":"YYYY-MM-DD"}""")
        
    buyers_below_avg_dict = {}
    for i in filtros.keys():
        buyers_gmv_subcat = data_ventas[data_ventas[filtros[i]["featureFiltrado"]].isin(filtros[i]["valueFiltrado"])].groupby(by=["customer_id"])["gmv_usd"].sum()
        avg_gmv_subcat = buyers_gmv_subcat.median()
        ids_buyers_below_avg = buyers_gmv_subcat[buyers_gmv_subcat < avg_gmv_subcat].index.tolist()
        buyers_below_avg_dict[filtros[i]["nombre"]] = ids_buyers_below_avg

    buyers_below_avg_dataframe = pd.DataFrame.from_dict(buyers_below_avg_dict, orient='index').T
    return buyers_below_avg_dataframe

def sacar_buyers(data_ventas, filtros, especificar_rango="No"):
    """
    Owner: Andres
    Esta función permite sacar la lista de customers buyers de categorias, subcategorias o skus especificos.
    
    
    Argumentos:
    data_ventas: data de ventas que entrega la función data_ventas_query.
    filtros: diccionario con el siguiente formato ->
    {
    "1":{"nombre":<Nombre deseado para el subconjunto>,
    "featureFiltrado":category, subcat o sku_id,
    "valueFiltrado":lista de categorias, subcat o sku_id a analizar},

    "2":{"nombre":<Nombre deseado para el subconjunto>,
    "featureFiltrado":category, subcat o sku_id,
    "valueFiltrado":lista de categorias, subcat o sku_id a analizar}
    }
    especificar_rango (opcional): En caso de no querer analizar la totalidad del tiempo que hay en data_ventas_query se puede usar
    un diccionario para delimitar el rango de timpo. El formato debe tener la siguiente estructura -> {"desde":"YYYY-MM-DD", "hasta":"YYYY-MM-DD"}
    
    Resultados:
    non_buyers_dataframe: dataframe con los buyers debajo del promedio para los n grupos seleccionados con la variable filtros durante el rango de fechas espcificado.
    """
    total_users = data_ventas["customer_id"].unique().tolist()
    if especificar_rango == "No":
        data_analisis = data_ventas.copy()
    elif type(especificar_rango) == type({}):
        data_analisis = data_ventas[(data_ventas["submit_date"] >= especificar_rango["desde"]) & (data_ventas["submit_date"] <= especificar_rango["hasta"])].copy()
    else:
        raise Exception("""
        En caso de determinar un rango de fechas personalizado se debe usar un diccionario
        con la estructura {"desde":"YYYY-MM-DD", "hasta":"YYYY-MM-DD"}""")
        
    buyers_dict = {}
    for i in filtros.keys():
        buyers_gmv_subcat = data_ventas[data_ventas[filtros[i]["featureFiltrado"]].isin(filtros[i]["valueFiltrado"])].groupby(by=["customer_id"])["gmv_usd"].sum()
        #avg_gmv_subcat = buyers_gmv_subcat.mean()
        ids_buyers = buyers_gmv_subcat.index.tolist()
        buyers_dict[filtros[i]["nombre"]] = ids_buyers

    buyers_dataframe = pd.DataFrame.from_dict(buyers_dict, orient='index').T
    return buyers_dataframe


def proceso_para_csv(data_ventas, todays_date, non_buyers_date, city,
                          list_to_filter, level, start_campaing_in_x_days=0, end_campaing_in_x_days=7, additional_words="", 
                          tipo='non_buyers', limite=0.9, forzar_skus = {}, data_ventas_skus = pd.DataFrame(), remove_keyword = 'False'):
    """Esta función permite crear de manera automatizada tanto los segmentos de clientes non-buyers como el csv
    con los productos que deberían aplicar para cada segmento.
    Argumentos:
    data_ventas: data de ventas que entrega la función data_ventas_query.
    todays_date: fecha, en formato string, del día de hoy.
    non_buyers_date: fecha, en formato string, del día a partir del cual se definirá a un cliente como non_buyer.
    city: ciudad del csv.
    list_to_filter: lista completa de categorias o de subcategorias a las que se les prenderá descuentos.
    level: flag que se usa para indicar si la lista corresponde a subcategorias o categorias.
    start_campaing_in_x_days: numero de dias que faltan para iniciar los descuentos
    end_campaing_in_x_days: numero de dias que faltan para apagar los descuentos
    additional_words: (opcional) palabras extras para identificar los descuentos
    tipo: (opcional) la funcion puede generar segmentos tanto para non_buyers como para buyers_below_median_gmv.
    forzar_skus: (opcional) forzar skus de distintas categorias/subcategorias - {'subcat':['sku_1', 'sku_2'],...}
    data_ventas_skus (opcional) = data_ventas de donde se tomarán los skus a ofrecer.

    Resultados:
    df_csv: dataframe con el formato csv requerido para cargar descuentos.
    non_buyers_df: dataframe con los customer_ids de los clientes que son non_buyers para cada campaña.
    non_buyers_email: dataframe con los customer_emails de los clientes que son non_buyers para cada campaña.
    """
    params_csv = {
        "BOG": {
            "Pollo": {
                "discount": 18,
                "max_uses_per_order": 10,
                "max_orders_per_customer": 2
            },
            "Cerdo": {
                "discount": 18,
                "max_uses_per_order": 10,
                "max_orders_per_customer": 2
            },
            "Res": {
                "discount": 18,
                "max_uses_per_order": 12,
                "max_orders_per_customer": 2
            },
            "Pescados & Mariscos Congelado": {
                "discount": 18,
                "max_uses_per_order": 10,
                "max_orders_per_customer": 2
            },
            "Embutidos": {
                "discount": 18,
                "max_uses_per_order": 10,
                "max_orders_per_customer": 2
            },
            "Verduras": {
                "discount": 20,
                "max_uses_per_order": 50,
                "max_orders_per_customer": 2
            },
            "Frutas": {
                "discount": 20,
                "max_uses_per_order": 50,
                "max_orders_per_customer": 2
            },
            "Tubérculos": {
                "discount": 16,
                "max_uses_per_order": 2,
                "max_orders_per_customer": 2,
            },            
            "Frutas Jugo": {
                "discount": 15,
                "max_uses_per_order": 25,
                "max_orders_per_customer": 2,                           
            },            
            "Aceites & Grasas": {
                "discount": 10,
                "max_uses_per_order": 2,
                "max_orders_per_customer": 2
            },
            "Arroz": {
                "discount": 7,
                "max_uses_per_order": 4,
                "max_orders_per_customer": 2
            },
            "Azúcar & Endulzantes": {
                "discount": 7,
                "max_uses_per_order": 3,
                "max_orders_per_customer": 2
            },
            "Harinas & Mezclas": {
                "discount": 10,
                "max_uses_per_order": 3,
                "max_orders_per_customer": 2
            },
            "Panadería & Repostería": {
                "discount": 15,
                "max_uses_per_order": 20,
                "max_orders_per_customer": 2
            },
            "Salsas": {
                "discount": 15,
                "max_uses_per_order": 15,
                "max_orders_per_customer": 2
            },
            "Granos": {
                "discount": 15,
                "max_uses_per_order": 30,
                "max_orders_per_customer": 2
            },
            "Huevos": {
                "discount": 10,
                "max_uses_per_order": 12,
                "max_orders_per_customer": 2
            },
            "Leches": {
                "discount": 10,
                "max_uses_per_order": 8,
                "max_orders_per_customer": 2
            },
            "Quesos": {
                "discount": 12,
                "max_uses_per_order": 10,
                "max_orders_per_customer": 2
            },
            'Derivados Lácteos': {
                "discount": 12,
                "max_uses_per_order": 10,
                "max_orders_per_customer": 2
            },
            "Quesos": {
                "discount": 12,
                "max_uses_per_order": 10,
                "max_orders_per_customer": 2
            },
            'Derivados Lácteos': {
                "discount": 12,
                "max_uses_per_order": 10,
                "max_orders_per_customer": 2
            },
            "Vasos": {
                "discount": 10,
                "max_uses_per_order": 6,
                "max_orders_per_customer": 2
            },
            "Contenedores": {
                "discount": 10,
                "max_uses_per_order": 6,
                "max_orders_per_customer": 2
            },
            "Platos & Bandejas": {
                "discount": 10,
                "max_uses_per_order": 6,
                "max_orders_per_customer": 2
            },
            'Cubiertos & Pitillos': {
                "discount": 10,
                "max_uses_per_order": 4,
                "max_orders_per_customer": 2
            },
            "Bolsas": {
                "discount": 10,
                "max_uses_per_order": 5,
                "max_orders_per_customer": 2
            },
            "Empaque & Envoltura": {
                "discount": 10,
                "max_uses_per_order": 4,
                "max_orders_per_customer": 2
            },
            "Papel & Toallas": {
                "discount": 15,
                "max_uses_per_order": 12,
                "max_orders_per_customer": 2
            },
            'Detergente, Jabón & Lavalozas': {
                "discount": 20,
                "max_uses_per_order": 5,
                "max_orders_per_customer": 2
            },
            'Cuidado & Protección Personal': {
                "discount": 20,
                "max_uses_per_order": 5,
                "max_orders_per_customer": 2
            },
            "Implementos de Aseo": {
                "discount": 20,
                "max_uses_per_order": 5,
                "max_orders_per_customer": 2
            },
            "Desinfectantes & Sanitizantes": {
                "discount": 20,
                "max_uses_per_order": 5,
                "max_orders_per_customer": 2
            },
            "Alimentos Congelados": {
                "discount": 15,
                "max_uses_per_order": 10,
                "max_orders_per_customer": 2
            },
            "Bebidas sin alcohol": {
                "discount": 15,
                "max_uses_per_order": 10,
                "max_orders_per_customer": 2
            }
        },
        
        "GDL": {
            "Pollo": {
                "discount": 18,
                "max_uses_per_order": 10,
                "max_orders_per_customer": 2
            },
            "Cerdo": {
                "discount": 18,
                "max_uses_per_order": 10,
                "max_orders_per_customer": 2
            },
            "Res": {
                "discount": 18,
                "max_uses_per_order": 8,
                "max_orders_per_customer": 2
            },
            "Pescados & Mariscos": {
                "discount": 18,
                "max_uses_per_order": 6,
                "max_orders_per_customer": 2
            },
            "Huevos": {
                "discount": 12,
                "max_uses_per_order": 3,
                "max_orders_per_customer": 2
            },
            "Leches": {
                "discount": 10,
                "max_uses_per_order": 15,
                "max_orders_per_customer": 2
            },
            "Alimentos Congelados": {
                "discount": 15,
                "max_uses_per_order": 10,
                "max_orders_per_customer": 2
            },
            "Verduras": {
                "discount": 20,
                "max_uses_per_order": 50,
                "max_orders_per_customer": 2
            },
            "Frutas": {
                "discount": 20,
                "max_uses_per_order": 50,
                "max_orders_per_customer": 2
            },
            "Tubérculos": {
                "discount": 10,
                "max_uses_per_order": 4,
                "max_orders_per_customer": 2
            },
            "Vasos, Contenedores & Platos": {
                "discount": 7,
                "max_uses_per_order": 15,
                "max_orders_per_customer": 2
            },
            "Papel & Toallas": {
                "discount": 10,
                "max_uses_per_order": 20,
                "max_orders_per_customer": 2
            },
            "Detergente, Jabón & Lavatrastes": {
                "discount": 10,
                "max_uses_per_order": 20,
                "max_orders_per_customer": 2
            }
        },
        
        "CMX": {
            "Pollo": {
                "discount": 20,
                "max_uses_per_order": 10,
                "max_orders_per_customer": 2
            },
            "Cerdo": {
                "discount": 20,
                "max_uses_per_order": 10,
                "max_orders_per_customer": 2
            },
            "Res": {
                "discount": 20,
                "max_uses_per_order": 8,
                "max_orders_per_customer": 2
            },
            "Pescados & Mariscos Congelado": {
                "discount": 20,
                "max_uses_per_order": 6,
                "max_orders_per_customer": 2
            },
            "Embutidos": {
                "discount": 20,
                "max_uses_per_order": 10,
                "max_orders_per_customer": 2
            },
            "Verduras": {
                "discount": 20,
                "max_uses_per_order": 50,
                "max_orders_per_customer": 2
            },
            "Frutas": {
                "discount": 20,
                "max_uses_per_order": 50,
                "max_orders_per_customer": 2
            },
            "Tubérculos": {
                "discount": 10,
                "max_uses_per_order": 4,
                "max_orders_per_customer": 2
            },
            "Aceites & Grasas": {
                "discount": 10,
                "max_uses_per_order": 2,
                "max_orders_per_customer": 2
            },
            "Arroz": {
                "discount": 7,
                "max_uses_per_order": 2,
                "max_orders_per_customer": 2
            },
            "Azúcar & Endulzantes": {
                "discount": 10,
                "max_uses_per_order": 2,
                "max_orders_per_customer": 2
            },
            "Harinas & Mezclas": {
                "discount": 7,
                "max_uses_per_order": 3,
                "max_orders_per_customer": 2
            },
            "Panadería & Tortilleria": {
                "discount": 12,
                "max_uses_per_order": 8,
                "max_orders_per_customer": 2
            },
            "Salsas": {
                "discount": 15,
                "max_uses_per_order": 15,
                "max_orders_per_customer": 2
            },
            "Chiles Secos": {
                "discount": 10,
                "max_uses_per_order": 25,
                "max_orders_per_customer": 2
            },
            "Granos": {
                "discount": 10,
                "max_uses_per_order": 25,
                "max_orders_per_customer": 2
            },
            "Pasta": {
                "discount": 8,
                "max_uses_per_order": 60,
                "max_orders_per_customer": 2
            },
            "Enlatados": {
                "discount": 10,
                "max_uses_per_order": 40,
                "max_orders_per_customer": 2
            },
            "Huevos": {
                "discount": 10,
                "max_uses_per_order": 3,
                "max_orders_per_customer": 2
            },
            "Leches": {
                "discount": 12,
                "max_uses_per_order": 5,
                "max_orders_per_customer": 2
            },
            "Quesos": {
                "discount": 15,
                "max_uses_per_order": 10,
                "max_orders_per_customer": 2
            },
            "Vasos, Contenedores & Platos": {
                "discount": 7,
                "max_uses_per_order": 6,
                "max_orders_per_customer": 2
            },
            "Cubiertos": {
                "discount": 10,
                "max_uses_per_order": 4,
                "max_orders_per_customer": 2
            },
            "Bolsas": {
                "discount": 10,
                "max_uses_per_order": 4,
                "max_orders_per_customer": 2
            },
            "Empaque & Envoltura": {
                "discount": 10,
                "max_uses_per_order": 4,
                "max_orders_per_customer": 2
            },
            "Papel & Toallas": {
                "discount": 20,
                "max_uses_per_order": 5,
                "max_orders_per_customer": 2
            },
            "Detergente, Jabón & Lavatrastes": {
                "discount": 15,
                "max_uses_per_order": 5,
                "max_orders_per_customer": 2
            },
            "Implementos de Aseo": {
                "discount": 20,
                "max_uses_per_order": 5,
                "max_orders_per_customer": 2
            },
            "Desinfectantes & Sanitizantes": {
                "discount": 20,
                "max_uses_per_order": 5,
                "max_orders_per_customer": 2
            },
            "Alimentos Congelados": {
                "discount": 15,
                "max_uses_per_order": 10,
                "max_orders_per_customer": 2
            }
        },
        
        "BAQ": {
            "Pollo": {
                "discount": 18,
                "max_uses_per_order": 30,
                "max_orders_per_customer": 2
            },
            "Cerdo": {
                "discount": 18,
                "max_uses_per_order": 15,
                "max_orders_per_customer": 2
            },
            "Res": {
                "discount": 18,
                "max_uses_per_order": 20,
                "max_orders_per_customer": 2
            },
            "Pescados & Mariscos Congelados": {
                "discount": 18,
                "max_uses_per_order": 10,
                "max_orders_per_customer": 2
            },
            "Embutidos": {
                "discount": 18,
                "max_uses_per_order": 10,
                "max_orders_per_customer": 2
            },
            "Verduras": {
                "discount": 20,
                "max_uses_per_order": 50,
                "max_orders_per_customer": 2
            },
            "Frutas": {
                "discount": 20,
                "max_uses_per_order": 50,
                "max_orders_per_customer": 2
            },
            "Tubérculos": {
                "discount": 8,
                "max_uses_per_order": 2,
                "max_orders_per_customer": 2
            },
            "Aceites & Grasas": {
                "discount": 10,
                "max_uses_per_order": 2,
                "max_orders_per_customer": 2
            },
            "Arroz": {
                "discount": 7,
                "max_uses_per_order": 4,
                "max_orders_per_customer": 2
            },
            "Azúcar & Endulzantes": {
                "discount": 8,
                "max_uses_per_order": 3,
                "max_orders_per_customer": 2
            },
            "Harinas & Mezclas": {
                "discount": 7,
                "max_uses_per_order": 3,
                "max_orders_per_customer": 2
            },
            "Panadería & Repostería": {
                "discount": 15,
                "max_uses_per_order": 20,
                "max_orders_per_customer": 2
            },
            "Salsas": {
                "discount": 15,
                "max_uses_per_order": 15,
                "max_orders_per_customer": 2
            },
            "Granos": {
                "discount": 15,
                "max_uses_per_order": 30,
                "max_orders_per_customer": 2
            },
            "Huevos": {
                "discount": 10,
                "max_uses_per_order": 12,
                "max_orders_per_customer": 2
            },
            "Leches": {
                "discount": 10,
                "max_uses_per_order": 8,
                "max_orders_per_customer": 2
            },
            "Quesos": {
                "discount": 10,
                "max_uses_per_order": 10,
                "max_orders_per_customer": 2
            },
            "Vasos": {
                "discount": 7,
                "max_uses_per_order": 6,
                "max_orders_per_customer": 2
            },
            "Contenedores": {
                "discount": 7,
                "max_uses_per_order": 6,
                "max_orders_per_customer": 2
            },
            "Platos & Bandejas": {
                "discount": 7,
                "max_uses_per_order": 6,
                "max_orders_per_customer": 2
            },
            "Cubiertos & Pitillos": {
                "discount": 10,
                "max_uses_per_order": 4,
                "max_orders_per_customer": 2
            },
            "Bolsas": {
                "discount": 10,
                "max_uses_per_order": 4,
                "max_orders_per_customer": 2
            },
            "Empaque & Envoltura": {
                "discount": 10,
                "max_uses_per_order": 4,
                "max_orders_per_customer": 2
            },
            "Papel & Toallas": {
                "discount": 15,
                "max_uses_per_order": 12,
                "max_orders_per_customer": 2
            },
            "Detergente, Jabón & Lavatrastes": {
                "discount": 15,
                "max_uses_per_order": 5,
                "max_orders_per_customer": 2
            },
            "Implementos de Aseo": {
                "discount": 20,
                "max_uses_per_order": 5,
                "max_orders_per_customer": 2
            },
            "Desinfectantes & Sanitizantes": {
                "discount": 20,
                "max_uses_per_order": 5,
                "max_orders_per_customer": 2
            },
            "Alimentos Congelados": {
                "discount": 15,
                "max_uses_per_order": 10,
                "max_orders_per_customer": 2
            },
            "Bebidas sin alcohol": {
                "discount": 10,
                "max_uses_per_order": 12,
                "max_orders_per_customer": 2
            },
            "Bebidas Alcohólicas": {
                "discount": 10,
                "max_uses_per_order": 12,
                "max_orders_per_customer": 2
            },
            
        },
        
        "SPO": {
            "Frutas": {
                "discount": 20,
                "max_uses_per_order": 10,
                "max_orders_per_customer": 2
            },
            "Verduras": {
                "discount": 20,
                "max_uses_per_order": 10,
                "max_orders_per_customer": 2
            },
            "Tubérculos": {
                "discount": 15,
                "max_uses_per_order": 5,
                "max_orders_per_customer": 2
            },
            "Legumes": {
                "discount": 15,
                "max_uses_per_order": 10,
                "max_orders_per_customer": 2
            },
            "Ovos": {
                "discount": 10,
                "max_uses_per_order": 5,
                "max_orders_per_customer": 2
            },
            "Leite": {
                "discount": 8,
                "max_uses_per_order": 24,
                "max_orders_per_customer": 2
            },
            "Alimentos Congelados": {
                "discount": 20,
                "max_uses_per_order": 20,
                "max_orders_per_customer": 2
            },
            "Carne Vermelha": {
                "discount": 20,
                "max_uses_per_order": 30,
                "max_orders_per_customer": 2
            },
            "Frango": {
                "discount": 18,
                "max_uses_per_order": 30,
                "max_orders_per_customer": 2
            },
            "Embutidos": {
                "discount": 18,
                "max_uses_per_order": 30,
                "max_orders_per_customer": 2
            },            
            "Queijos": {
                "discount": 15,
                "max_uses_per_order": 30,
                "max_orders_per_customer": 2
            },
            "Suínos": {
                "discount": 18,
                "max_uses_per_order": 30,
                "max_orders_per_customer": 2
            },
            "Peixes e Frutos do Mar": {
                "discount": 15,
                "max_uses_per_order": 30,
                "max_orders_per_customer": 2
            },
            "Detergente, Sabão e Lava-Louças": {
                "discount": 15,
                "max_uses_per_order": 12,
                "max_orders_per_customer": 2
            },
            "Feijão": {
                "discount": 8,
                "max_uses_per_order": 25,
                "max_orders_per_customer": 2
            },
            "Refrigerante": {
                "discount": 8,
                "max_uses_per_order": 25,
                "max_orders_per_customer": 2
            },
            "Cerveja": {
                "discount": 8,
                "max_uses_per_order": 25,
                "max_orders_per_customer": 2
            },
            "Água": {
                "discount": 8,
                "max_uses_per_order": 25,
                "max_orders_per_customer": 2
            },
            "Suco": {
                "discount": 8,
                "max_uses_per_order": 25,
                "max_orders_per_customer": 2
            },
            "Manteigas e Margarinas": {
                "discount": 10,
                "max_uses_per_order": 30,
                "max_orders_per_customer": 2
            },
            "Condimentos": {
                "discount": 10,
                "max_uses_per_order": 30,
                "max_orders_per_customer": 2
            },
            "Arroz": {
                "discount": 10,
                "max_uses_per_order": 30,
                "max_orders_per_customer": 2
            },
            "Farinhas e Misturas": {
                "discount": 8,
                "max_uses_per_order": 10,
                "max_orders_per_customer": 2
            },
            
        }
    }

    def organize_text(x, city, today_date, tipo, additional_words=""):

        def normalize_text(raw_text):
            """
            Removes common accent characters and normalize text.
            """
            raw_text = raw_text.replace(" ", "")
            raw_text = raw_text.replace("�", "")
            raw_text = raw_text.replace("/", "")
            raw_text = re.sub(r'\s*', '', raw_text)
            raw_text = re.sub(r"[àáâãäå]", 'a', raw_text)
            raw_text = re.sub(r"[èéêë]", 'e', raw_text)
            raw_text = re.sub(r"[ìíîï]", 'i', raw_text)
            raw_text = re.sub(r"[òóôõö]", 'o', raw_text)
            raw_text = re.sub(r"[ùúûü]", 'u', raw_text)
            raw_text = re.sub(r"[ýÿ]", 'y', raw_text)
            raw_text = re.sub(r"[ß]", 'ss', raw_text)
            raw_text = re.sub(r"[ñ]", 'n', raw_text)
            raw_text = raw_text.lower()
            return raw_text + "_"

        today_date_datetime = datetime.strptime(today_date, "%Y-%m-%d")
        week = datetime.strptime(today_date, "%Y-%m-%d").isocalendar()[1]
        ms = normalize_text(additional_words)
        ms = ms.capitalize()
        y = datetime.strptime(today_date, "%Y-%m-%d").strftime("%Y%m%d")
        x = x.lower()
        x = normalize_text(x)
        x = x.replace("&", "y")
        x = x.replace(",", "")
        x = x.replace("ç", "")
        x = x
        if tipo == 'non_buyers':
            final_x = y + "_" + city + "" + ms + "NB_" + x + f"W{week}"
        elif tipo == 'buyers_below_median_gmv':
            final_x = y + "_" + city + "" + ms + \
                "_Spend-BMS_" + x + f"W{week}"
        elif tipo ==  'all_buyers':
            final_x = y + "_" + city + "" + ms + \
                "_Spend-All_" + x + f"W{week}"

        final_x = final_x.replace("__", "_")
        return final_x

    if tipo not in ['non_buyers', 'buyers_below_median_gmv', 'all_buyers']:
        raise Exception(
            'El tipo seleccionado no está soportado por la función')

    from datetime import date, timedelta, datetime
    import re

    additional_words = additional_words.strip("_")
    additional_words = "_" + additional_words.upper()

    if level == "category":
        cat_allsubcat_series = data_ventas.groupby(by=[level])[
            "subcat"].unique()
        cat_allsubcat_series = cat_allsubcat_series[cat_allsubcat_series.index.isin(list_to_filter)]
        list_to_filter = [val for sublist in [
            *map(list, cat_allsubcat_series.values)] for val in sublist]
        print(list_to_filter)
        
#//------------------------------------------------------------------------------
#//Adición para usar una data de ventas diferente a la que se usa para non-buyers         
    if data_ventas_skus.empty:
        top_products_df = analizar_top_productos(data_ventas=data_ventas)
    else:
        top_products_df = analizar_top_productos(data_ventas=data_ventas_skus)
        print('Uso de data externa')
    
    hijo_padre_dict = dict(zip(top_products_df["sku_id"].values,
                               top_products_df["padre_sku_id"].values))

    top_products_df[["sku_id_str", "padre_sku_id_str"]
                    ] = top_products_df[["sku_id", "padre_sku_id"]].astype(str)
    padre_hijo_dict = top_products_df.groupby(by=["padre_sku_id_str"])[
        "sku_id_str"].apply('#'.join).to_dict()
    # Deleting columns created
    top_products_df = top_products_df.drop(
        labels=["sku_id_str", "padre_sku_id_str"], axis=1)
    
    #//Si se forzaron skus de una subcategoria que no está en la lista, se adiciona
    if len(forzar_skus) != 0:
        for subcat in forzar_skus:  
            if subcat not in list_to_filter:
                list_to_filter.append(subcat)

    top_products_df = top_products_df[top_products_df["subcat"].isin(
        list_to_filter)].copy()

    
    def crear_listas_top_skus(x, hijo_padre_relation, padre_hijo_relation):
        threshold_top_products = x["penetration_porcentual"].quantile(limite)
        list_of_top_skus = x[x["penetration_porcentual"]
                             >= threshold_top_products]["sku_id"].values
        list_of_top_skus_padres = [
            *map(hijo_padre_relation.get, list_of_top_skus)]
        list_of_top_skus_padres = [*map(str, list_of_top_skus_padres)]
        list_of_top_skus_all_hijos = [
            *map(padre_hijo_relation.get, list_of_top_skus_padres)]
        return "#".join(list_of_top_skus_all_hijos)

    sku_to_use_series = top_products_df.groupby(by=["subcat"]).apply(
        crear_listas_top_skus, hijo_padre_dict, padre_hijo_dict)
    
#//-----------------------------------------------------------------------------
#//Adición para forzar skus dentro de una categoría/subcategoría 
    if len(forzar_skus) != 0:
        for subcat in forzar_skus:            
            try:
                mod_str = sku_to_use_series[subcat]
            except:
                print('ERROR WITH: ', subcat)
                print('Subcategory not recognized & removed from list')
                list_to_filter.remove(subcat)
                continue
            for sku in forzar_skus[subcat]:
                if sku not in mod_str:
                    mod_str = mod_str+'#'+sku                    
            sku_to_use_series[subcat] = mod_str
            print(subcat, 'skus added succesfully')
            #//Si se forzaron skus de una subcategoria que no está en la lista, se adiciona
            if subcat not in list_to_filter:
                list_to_filter = list_to_filter.append(subcat)
    
    dict_skus_for_non_buyers = dict(
        (x, sku_to_use_series[x]) for x in list_to_filter)

    
    
    todays_date_plus_x_days = (datetime.strptime(
        todays_date, "%Y-%m-%d") + timedelta(days=start_campaing_in_x_days)).strftime("%Y-%m-%d")

    csv_file = {}
    start_date = (datetime.strptime(todays_date, "%Y-%m-%d") +
                  timedelta(days=start_campaing_in_x_days)).strftime("%d-%m-%Y")
    end_date = (datetime.strptime(todays_date, "%Y-%m-%d") +
                timedelta(days=end_campaing_in_x_days)).strftime("%d-%m-%Y")

    campaing_names = [organize_text(x, city=city, today_date=todays_date_plus_x_days, tipo=tipo,
                                    additional_words=additional_words) for x in dict_skus_for_non_buyers.keys()]
    campaing_skus = list(dict_skus_for_non_buyers.values())

    city_params = params_csv.get(
        city) if params_csv.get(city) is not None else {}

    if city_params != {}:
        offer_discount = [city_params.get(x).get('discount') if city_params.get(x) else -1 for x in dict_skus_for_non_buyers.keys()]
        offer_max_uses = [city_params.get(x).get('max_uses_per_order') if city_params.get(x) else -1 for x in dict_skus_for_non_buyers.keys()]
        offer_max_order = [city_params.get(x).get('max_orders_per_customer') if city_params.get(x) else -1 for x in dict_skus_for_non_buyers.keys()]
    else:
        offer_discount = [-1] * len(campaing_names)
        offer_max_uses = [-1] * len(campaing_names)
        offer_max_order = [-1] * len(campaing_names)


    csv_file["offer_name"] = campaing_names
    csv_file["offer_description"] = None
    # TODO: List with diccionary by city and by subcat
    csv_file["discount"] = offer_discount
    csv_file["automatically_consider_offer"] = "true"
    csv_file["start_date"] = start_date
    csv_file["end_date"] = end_date
    # TODO: List of max uses per order by city and by subcat
    csv_file["max_uses_per_order"] = offer_max_uses
    # TODO: Max uses per order by week duration of the offer
    csv_file["max_uses_per_customer"] = offer_max_order
    csv_file["customer_segment_id"] = None
    csv_file["sku_id"] = campaing_skus
    df_csv = pd.DataFrame(csv_file)

    params_non_buyers = {}
    for n, (name, to_filter) in enumerate(zip(campaing_names, list_to_filter)):
        params_non_buyers[n] = {
            "nombre": name,
            "featureFiltrado": "subcat",
            "valueFiltrado": [to_filter]
        }

    if tipo == 'non_buyers':
        non_buyers_df = sacar_non_buyers(data_ventas=data_ventas, filtros=params_non_buyers,
                                         especificar_rango={"desde": non_buyers_date, "hasta": todays_date})

        customer_id_and_email = dict_cliente_email_from_ventas(
            data_ventas=data_ventas)
        non_buyers_email = non_buyers_df.stack().map(customer_id_and_email).unstack()

        return df_csv, non_buyers_df, non_buyers_email

    elif tipo == 'buyers_below_median_gmv':
        buyers_df = sacar_buyers_below_median(data_ventas=data_ventas, filtros=params_non_buyers,
                                           especificar_rango={"desde": non_buyers_date, "hasta": todays_date})

        customer_id_and_email = dict_cliente_email_from_ventas(
            data_ventas=data_ventas)
        buyers_email = buyers_df.stack().map(customer_id_and_email).unstack()

        return df_csv, buyers_df, buyers_email

    elif tipo == 'all_buyers':
        buyers_df = sacar_buyers(data_ventas=data_ventas, filtros=params_non_buyers,
                                           especificar_rango={"desde": non_buyers_date, "hasta": todays_date})

        customer_id_and_email = dict_cliente_email_from_ventas(
            data_ventas=data_ventas)
        buyers_email = buyers_df.stack().map(customer_id_and_email).unstack()

        return df_csv, buyers_df, buyers_email

def run_customQuery(query):
    """
    Owner: Andrés Solano 
    Esta función corre el query que entre como parametro en el data warehouse
    
    Argumentos:
    query: el query para correr en el DW
    
    Resultados:
    data_discount: dataframe con los order_items_id con descuento.
    """
    dataframe = read_connection_data_warehouse.runQuery(query)
    return dataframe

def solicitar_query_as_df(api_key, query_id):
    """
    Owner: Andrés Solano 
    Esta función corre un query creado en redash
    
    Argumentos:
    api_key: api key (propia de cada usuario en redash)
    query_id: (id del query en redash - sale en la URL)
    
    Resultados:
    data_discount: dataframe con los order_items_id con descuento.
    """
    import http.client
    import json
    import time
    import pandas as pd
    print('connecting to redash...')
    h1 = http.client.HTTPSConnection('redash.federate.frubana.com')
    h1.request('POST', '/api/queries/{query_id}/results'.format(query_id=query_id), body=json.dumps({'max_age': 0}), headers={'Authorization':api_key})
    jobs_response = h1.getresponse()
    jobs_bytes = jobs_response.read()
    status = json.loads(jobs_bytes)['job']['status']
    id_jobs = json.loads(jobs_bytes)['job']['id']
    time.sleep(5)
    while status in [1, 2]:
        h1.request('GET', f'/api/jobs/{id_jobs}', body=None, headers={'Authorization':api_key})
        jobs_in_progess = h1.getresponse()
        jobs_bytes_in_progess = jobs_in_progess.read()
        status = json.loads(jobs_bytes_in_progess)['job']['status']
        print('query_executing...')
        time.sleep(20)
    print('importing data...')
    h1.request('GET', '/api/queries/{query_id}/results'.format(query_id=query_id), body=None, headers={'Authorization':api_key})
    query_response = h1.getresponse()
    print('reading data...')
    a = query_response.read()
    df = pd.DataFrame(json.loads(a)['query_result']['data']['rows'])
    print('Done')