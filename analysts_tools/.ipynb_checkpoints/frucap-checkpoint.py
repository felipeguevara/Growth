import numpy as np
import pandas as pd
import requests
import json
import time
import sys

# route='/srv/scratch/'
# sys.path.append(route)
# route='/srv/scratch/analyst_community/'
# sys.path.append(route)
from analystcommunity import read_connection_data_warehouse
###################################################################################################################################################################################
######################################################################################################################################################################################################################################################################################################################################################################
###################################################################################################################################################################################
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
###################################################################################################################################################################################
###################################################################################################################################################################################
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

###################################################################################################################################################################################
###################################################################################################################################################################################
#This method import the query results, with the last cached data (i.e. last execution)
#The url param is (generally) the complete url of a fixed query to obtain its data in json format
def import_redash_query(url):
    response = requests.get(url)
    #Print status code of the request
    print(response.status_code)
    data = pd.DataFrame.from_dict(response.json()['query_result']['data']['rows'])
    return data
###################################################################################################################################################################################
###################################################################################################################################################################################
#This method performs an execution of the query, and then obtain the query data results
#The redash_url param is the base url, the query_id param indicates the specific query
#The api_key corresponds to the used API Key (personal,not public)
#If the query requests additional params, params should be added
def get_fresh_query_result(redash_url,query_id,api_key,params,pause):
    #Each query execution generates a job, from which we extract its status
    def poll_job(s,redash_url,job,pause):
    # TODO: add timeout
        while job['status'] not in (3, 4):
            response = s.get('{}/api/jobs/{}'.format(redash_url, job['id']))
            job = response.json()['job']
            time.sleep(pause)

        if job['status'] == 3:
            return job['query_result_id']

        return None

    s=requests.Session()
    s.headers.update({'Authorization': 'Key {}'.format(api_key)})
    payload=dict(max_age=0, parameters=params)
    response=s.post('{}/api/queries/{}/results'.format(redash_url,query_id),data=json.dumps(payload))

    if response.status_code!=200:
        raise Exception('Refresh failed.')

    result_id = poll_job(s,redash_url,response.json()['job'],pause)

    if result_id:
        response = s.get('{}/api/queries/{}/results/{}.json'.format(redash_url,query_id,result_id))
        if response.status_code!=200:
            raise Exception('Failed getting results.')
    else:
        raise Exception('Query execution failed.')

    print(response.status_code)
    return pd.DataFrame.from_dict(response.json()['query_result']['data']['rows'])

###################################################################################################################################################################################
###################################################################################################################################################################################
def data_trans_comp_time_previous_query_vboletos(TIME):
    """
    Owner: Gabo Moreno
    Está función trae la data de ventas, para segmentos de restaurantes, para una ciudad en un rango de fecha determinado.
    
    Argumentos:
    Time: número de días que se requiere agrupar
 
    Resultados:
    dataframe: dataframe con la data de ventas solicitada
    """
    
    query = """
    With orden_boleto as(select   distinct bc.customer_id 
                                ,bo.order_id    as order_boleto 
                                ,bo.order_number
                                ,fbs.name as segmento  
                                ,s.site_identifier_value
                                ,Extract(Week  from bo.date_created) as bweek
                                ,Extract(year  from bo.date_created) as byear
                                ,bo.date_created::DATE as dia_boleto
                                        
                                FROM postgres_broadleaf_federate."broadleaf.blc_order"  bo 
                                inner join lnd_proc_dev.frucap_boletos                                   fru  on fru.order_code=bo.order_number
                                inner join postgres_broadleaf_federate."broadleaf.blc_fulfillment_group" bfg  ON bfg.order_id = bo.order_id
                                inner join postgres_broadleaf_federate."broadleaf.blc_fulfillment_order" bfo  ON bfo.fulfillment_group_id = bfg.fulfillment_group_id
                                inner join postgres_broadleaf_federate."broadleaf.fb_fulfillment_group"  ffg  ON ffg.fulfillment_group_id = bfg.fulfillment_group_id
                                inner join postgres_broadleaf_federate."broadleaf.fb_order"              fo   ON fo.order_id = bo.order_id
                                inner join postgres_broadleaf_federate."broadleaf.blc_order_payment"     bop  ON bop.order_id = bo.order_id
                                inner join postgres_broadleaf_federate."broadleaf.blc_site"              s    ON s.site_id = bo.site_disc
                                inner join postgres_broadleaf_federate."broadleaf.blc_customer"          bc   ON bc.customer_id = bo.customer_id
                                left join  postgres_broadleaf_federate."broadleaf.fb_order_type"         fot  ON fot.fb_order_type_id=fo.fb_order_type_id
                                left join  postgres_broadleaf_federate."broadleaf.fb_customer"           fc   ON fc.customer_id = bo.customer_id
                                left join  postgres_broadleaf_federate."broadleaf.fb_business_type"      fbs  ON fc.business_type_id = fbs.business_type_id 
                                
                               -- WHERE fo.fb_order_status_id IN (1,6,7,8)
                                 --   AND bo.order_status = 'SUBMITTED'
                                 --   AND bfo.status NOT IN ('ARCHIVED','CANCELLED')
                                 --   AND bop.archived = 'N'
                                 --   AND (fot.name IS NULL OR fot.name <> 'REFUND')
                                    )
 -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 ,ids as (select distinct customer_id from orden_boleto)
 

,conversion as (select distinct bsku.sku_id
                ,coalesce(conversion_value_y, 1) conversion_value_y
                ,coalesce(conversion_value_x, 1) conversion_value_x
                ,coalesce(conversion_type_y, fp.unit) conversion_type_y
                ,coalesce(conversion_type_x, fp.unit) conversion_type_x
                ,fp.unit
                ,bcat.name          as subcategory_name
                ,bcat.category_id   as sub_category_id
                ,bcat2.category_id  as category_id_main
                ,bcat2.name         as category_name
                from  postgres_broadleaf_federate."broadleaf.blc_sku"                 bsku 
                inner join postgres_broadleaf_federate."broadleaf.blc_sku"            s3    ON bsku.addl_product_id = s3.default_product_id
	            inner join postgres_broadleaf_federate."broadleaf.fb_sku"             s4    ON bsku.sku_id = s4.sku_id
                left join  postgres_broadleaf_federate."broadleaf.blc_product"        bp    ON bsku.addl_product_id = bp.product_id
                left join  postgres_broadleaf_federate."broadleaf.blc_category_xref"  bcx   ON bcx.sub_category_id = bp.default_category_id and bcx.archived='N' and bcx.sndbx_tier is null --Relaciones categorias 
                left join  postgres_broadleaf_federate."broadleaf.blc_category"       bcat  ON bcat.category_id = bp.default_category_id --Nombre subcategoria
                left join  postgres_broadleaf_federate."broadleaf.blc_category"       bcat2 ON bcx.category_id = bcat2.category_id 
                inner join postgres_broadleaf_federate."broadleaf.fb_product"         fp    ON bp.product_id = fp.product_id
                )

,info_cliente as (select distinct order_boleto
                ,dia_boleto
                ,bo.customer_id 
                ,bo.order_id
                ,bo.date_created as date_order
                
                ,CASE   WHEN s.site_identifier_value in ('CMX','GDL','MTY','PBC') then (bo.order_subtotal+coalesce(fo.total_tax_iva,0)+coalesce(bo.total_shipping,0))/19.19
                        WHEN s.site_identifier_value in ('SPO','BHZ','CWB') then (bo.order_subtotal+coalesce(fo.total_tax_iva,0)+coalesce(bo.total_shipping,0))/3.88
                        ELSE (bo.order_subtotal+coalesce(fo.total_tax_iva,0)+coalesce(bo.total_shipping,0))/3000 end as gmv

                ,count(distinct boi.order_item_id)    as numero_lineas_orden 


                from postgres_broadleaf_federate."broadleaf.blc_fulfillment_group_item" bfgi
                INNER JOIN postgres_broadleaf_federate."broadleaf.blc_fulfillment_group" bfg  ON bfgi.fulfillment_group_id = bfg.fulfillment_group_id
                INNER JOIN postgres_broadleaf_federate."broadleaf.fb_fulfillment_group"  ffg  ON ffg.fulfillment_group_id = bfg.fulfillment_group_id
                INNER JOIN postgres_broadleaf_federate."broadleaf.blc_fulfillment_order" bfo  ON bfo.fulfillment_group_id= bfg.fulfillment_group_id
                INNER JOIN postgres_broadleaf_federate."broadleaf.blc_order"             bo   ON bo.order_id = bfg.order_id
                inner  join ids                                                          ids  on ids.customer_id=bo.customer_id
                left   join orden_boleto                                                 fru  on fru.customer_id=bo.customer_id
                INNER JOIN postgres_broadleaf_federate."broadleaf.blc_order_item"        boi  ON bfgi.order_item_id= boi.order_item_id
                inner join postgres_broadleaf_federate."broadleaf.fb_order"              fo   ON fo.order_id = bo.order_id
                inner join postgres_broadleaf_federate."broadleaf.blc_order_payment"     bop  ON bop.order_id = bo.order_id
                inner join postgres_broadleaf_federate."broadleaf.blc_site"              s    ON s.site_id = bo.site_disc
                inner join postgres_broadleaf_federate."broadleaf.blc_customer"          bc   ON bc.customer_id = bo.customer_id
                left join  postgres_broadleaf_federate."broadleaf.fb_order_type"         fot  ON fot.fb_order_type_id=fo.fb_order_type_id

                where  bo.date_created < dia_boleto + interval '1 days'
                  and  bo.date_created >= (dia_boleto - interval '{TIME} days') 
                  and  fo.fb_order_status_id IN (1,6,7,8)
                  AND  bo.order_status = 'SUBMITTED'
                  AND  bfo.status NOT IN ('ARCHIVED','CANCELLED')
                  AND  bop.archived = 'N'
                  AND  (fot.name IS NULL OR fot.name <> 'REFUND')
                  group by 1,2,3,4,5,6
                ) 

, ordenes as(select distinct order_boleto
                    ,dia_boleto
                    ,order_id
                    ,date_order
                    from info_cliente)

,Ordenesxitem as (select distinct boi.order_item_id
                        ,order_boleto
                        ,dia_boleto
                        ,bo.order_id
                        ,date_order
                        ,bo.customer_id --1

                        ,case when s.site_identifier_value in ('CMX','GDL','MTY','PBC')
                                   then nvl((baid.adjustment_value * boip.quantity * foi.step_unit)/19.19, 0)
                                 when  s.site_identifier_value in ('SPO','BHZ','CWB')
                                       then nvl((baid.adjustment_value * boip.quantity * foi.step_unit)/3.88, 0)
                                 else nvl((baid.adjustment_value * boip.quantity * foi.step_unit)/3000, 0)
                                 end as discount_applied

                        ,case when s.site_identifier_value in ('CMX','GDL','MTY','PBC')
                                   then ((boi.quantity * foi.step_unit * boi.sale_price)/19.19)
                             when  s.site_identifier_value in ('SPO','BHZ','CWB')
                                   then ((boi.quantity * foi.step_unit * boi.sale_price)/3.88)
                             else ((boi.quantity * foi.step_unit * boi.sale_price)/3000) 
                             end as gmv_item

                        ,case when s.site_identifier_value in ('CMX','GDL','MTY','PBC')
                                 then  nvl((foi.total_price-foi.total_price_adj)/19.19, 0)
                                 when  s.site_identifier_value in ('SPO','BHZ','CWB')
                                 then  nvl((foi.total_price-foi.total_price_adj)/3.88, 0)
                                 else  nvl((foi.total_price-foi.total_price_adj)/3000, 0)
                                 end   as total_discount_order

                        ,boi.quantity * foi.step_unit as real_quantity
                        ,case when con.unit = 'UNID' then conversion_value_y*(boi.quantity * foi.step_unit) else (boi.quantity * foi.step_unit) end as volumen_kg
                       
                        --,boi.name
                        --,bdoi.sku_id
                        --,bs.addl_product_id as padre_sku_id,

                        ,con.subcategory_name
                        ,con.sub_category_id
                        ,con.category_name
                        ,con.category_id_main

                         from postgres_broadleaf_federate."broadleaf.blc_fulfillment_group_item" bfgi
                            INNER JOIN postgres_broadleaf_federate."broadleaf.blc_fulfillment_group" bfg  ON bfgi.fulfillment_group_id = bfg.fulfillment_group_id
                            INNER JOIN postgres_broadleaf_federate."broadleaf.fb_fulfillment_group"  ffg  ON ffg.fulfillment_group_id = bfg.fulfillment_group_id
                            INNER JOIN postgres_broadleaf_federate."broadleaf.blc_fulfillment_order" bfo  ON bfo.fulfillment_group_id= bfg.fulfillment_group_id
                            INNER JOIN postgres_broadleaf_federate."broadleaf.blc_order"             bo   ON bo.order_id = bfg.order_id
                            inner join ids                                                           ids  on ids.customer_id=bo.customer_id
                            inner join ordenes                                                            on ordenes.order_id=bo.order_id
                            INNER JOIN postgres_broadleaf_federate."broadleaf.blc_order_item"        boi  ON bfgi.order_item_id= boi.order_item_id
                            INNER JOIN postgres_broadleaf_federate."broadleaf.blc_site"              s    ON bo.site_disc = s.site_id
                            INNER JOIN postgres_broadleaf_federate."broadleaf.blc_discrete_order_item"  bdoi       ON bdoi.order_item_id = boi.order_item_id
                            INNER JOIN postgres_broadleaf_federate."broadleaf.fb_order_item"         foi  ON boi.order_item_id= foi.order_item_id
                            left join  conversion                                                    con  ON con.sku_id=bdoi.sku_id
                            inner join postgres_broadleaf_federate."broadleaf.fb_order"              fo   ON fo.order_id = bo.order_id
                            inner join postgres_broadleaf_federate."broadleaf.blc_order_payment"     bop  ON bop.order_id = bo.order_id
                            left join  postgres_broadleaf_federate."broadleaf.fb_order_type"         fot  ON fot.fb_order_type_id=fo.fb_order_type_id
                            LEFT JOIN postgres_broadleaf_federate."broadleaf.blc_order_item_price_dtl"  boip       ON  boi.order_item_id = boip.order_item_id
                            LEFT JOIN postgres_broadleaf_federate."broadleaf.blc_order_item_dtl_adj"    baid       ON  baid.order_item_price_dtl_id = boip.order_item_price_dtl_id
                           ) 
                       
,agrupacion_skus as (select distinct  OI.customer_id 
                    ,order_boleto
                    ,dia_boleto   

                    ,sum(discount_applied) as total_discount
                    ,sum(gmv_item) as gmv_subtotal_item
                    ,sum(volumen_kg) as size_order_kg_noreal
                    
                    ,count(distinct order_item_id)    as numero_lineas_time
                    ,count(distinct sub_category_id)  as numero_subcategorias_time 
                    ,count(distinct category_id_main) as numero_categoria_time

                    ,case when count(distinct order_item_id)    = 0 then 0 else sum(gmv_item)/count(distinct order_item_id)    end as gmv_linea
                    ,case when count(distinct order_item_id)    = 0 then 0 else sum(gmv_item)/count(order_item_id)             end as gmv_promedio_productos

                    ,case when count(distinct category_id_main) = 0 then 0 else sum(gmv_item)/count(distinct category_id_main) end as gmv_categoria
                    ,case when count(distinct sub_category_id)  = 0 then 0 else sum(gmv_item)/count(distinct sub_category_id)  end as gmv_subcategoria
         
                    --size_order_kg
                    ,sum(volumen_kg)-sum(case when (category_name ilike '%Aseo%' or  category_name ilike '%Limpe%' or subcategory_name ilike'%Aseo%' or subcategory_name ilike'%Limpeza%' 
                                                    or subcategory_name ilike'%Higiene%' or category_name ilike '%Desechables%' or category_name ilike '%Descartáveis%'
                                                    or subcategory_name ilike'%Desechables%' or subcategory_name ilike'%Utensilios%')
                                                    then volumen_kg else 0 end) 
                                                    as size_order_kg_real
                                                    
                    --gmv_kg                     
                    ,case when sum(volumen_kg)-sum(case when (category_name ilike '%Aseo%' or  category_name ilike '%Limpe%' or subcategory_name ilike'%Aseo%' or subcategory_name ilike'%Limpeza%' 
                                                    or subcategory_name ilike'%Higiene%' or category_name ilike '%Desechables%' or category_name ilike '%Descartáveis%'
                                                    or subcategory_name ilike'%Desechables%' or subcategory_name ilike'%Utensilios%')
                                                    then volumen_kg else 0 end) = 0 then 0 
                                else sum(gmv_item)/sum(volumen_kg)-sum(case when (category_name ilike '%Aseo%' or  category_name ilike '%Limpe%' or subcategory_name ilike'%Aseo%' or subcategory_name ilike'%Limpeza%' 
                                                                                    or subcategory_name ilike'%Higiene%' or category_name ilike '%Desechables%' or category_name ilike '%Descartáveis%'
                                                                                    or subcategory_name ilike'%Desechables%' or subcategory_name ilike'%Utensilios%')
                                then volumen_kg else 0 end) end 
                                as gmv_kg
                                
                    --gmv_linea_kg                   
                    ,case when count(distinct order_item_id) = 0 or (sum(volumen_kg)-sum (case when (category_name ilike '%Aseo%' or  category_name ilike '%Limpe%' or subcategory_name ilike'%Aseo%' or subcategory_name ilike'%Limpeza%' 
                                                                                                        or subcategory_name ilike'%Higiene%' or category_name ilike '%Desechables%' or category_name ilike '%Descartáveis%'
                                                                                                        or subcategory_name ilike'%Desechables%' or subcategory_name ilike'%Utensilios%')
                                                                                            then volumen_kg else 0 end)) = 0
                                then 0 else
                                        (sum(gmv_item)/count(distinct order_item_id))/(sum(volumen_kg)-sum (case when (category_name ilike '%Aseo%' or  category_name ilike '%Limpe%' or subcategory_name ilike'%Aseo%' or subcategory_name ilike'%Limpeza%' 
                                                                                                                        or subcategory_name ilike'%Higiene%' or category_name ilike '%Desechables%' or category_name ilike '%Descartáveis%'
                                                                                                                        or subcategory_name ilike'%Desechables%' or subcategory_name ilike'%Utensilios%')
                                                                                                            then volumen_kg else 0 end)) 
                                end as gmv_linea_kg
                                

                    --kg_linea     
                    ,case when count(distinct order_item_id) =0 then 0 else
                                (sum(volumen_kg)-sum (case when (category_name ilike '%Aseo%' or  category_name ilike '%Limpe%' or subcategory_name ilike'%Aseo%' or subcategory_name ilike'%Limpeza%' 
                                                                or subcategory_name ilike'%Higiene%' or category_name ilike '%Desechables%' or category_name ilike '%Descartáveis%'
                                                                or subcategory_name ilike'%Desechables%' or subcategory_name ilike'%Utensilios%')
                                                                then volumen_kg else 0 end))/count(distinct order_item_id) 
                                end as kg_linea
                    
                    --kg_subcat
                    ,case when count(distinct sub_category_id) =0 then 0 else
                                (sum(volumen_kg)-sum(case when (category_name ilike '%Aseo%' or  category_name ilike '%Limpe%' or subcategory_name ilike'%Aseo%' or subcategory_name ilike'%Limpeza%' 
                                                                or subcategory_name ilike'%Higiene%' or category_name ilike '%Desechables%' or category_name ilike '%Descartáveis%'
                                                                or subcategory_name ilike'%Desechables%' or subcategory_name ilike'%Utensilios%')
                                                                then volumen_kg else 0 end))/count(distinct sub_category_id) 
                                end as kg_subcat
                    
                    --kg_cat                         
                    ,case when count(distinct category_id_main) =0 then 0 else
                                 (sum(volumen_kg)-sum(case when (category_name ilike '%Aseo%' or  category_name ilike '%Limpe%' or subcategory_name ilike'%Aseo%' or subcategory_name ilike'%Limpeza%' 
                                                                 or subcategory_name ilike'%Higiene%' or category_name ilike '%Desechables%' or category_name ilike '%Descartáveis%'
                                                                 or subcategory_name ilike'%Desechables%' or subcategory_name ilike'%Utensilios%')
                                                                then volumen_kg else 0 end))/count(distinct category_id_main)
                    end as kg_cat
               
               
                --ABARROTES
                ,sum (case when (category_name ilike '%Abarrot%' or  category_name ilike '%Merce%' or subcategory_name ilike '%Abarrotes%' 
                      or subcategory_name ilike '%Aceites%' or subcategory_name ilike '%Mercearia%') 
                      then volumen_kg else 0 end)
                      as vol_Abarrotes
                      
                ,sum (case when (category_name ilike '%Abarrot%' or  category_name ilike  '%Merce%' or subcategory_name ilike '%Abarrotes%' 
                      or subcategory_name ilike '%Aceites%' or subcategory_name ilike '%Mercearia%') 
                      then gmv_item   else 0 end) 
                      as gmv_Abarrotes
                 
                ,sum (case when (category_name ilike '%Abarrot%' or  category_name ilike '%Merce%' or subcategory_name ilike '%Abarrotes%' 
                      or subcategory_name ilike '%Aceites%' or subcategory_name ilike '%Mercearia%') 
                      then 1 else 0 end)                       
                      as buy_Abarrotes
                
                
                --ASEO y LIMPIEZA
                ,sum (case when (category_name ilike '%Aseo%' or  category_name ilike '%Limpe%'
                     or subcategory_name ilike '%Aseo%' or subcategory_name ilike '%Limpeza%' or subcategory_name ilike '%Higiene%')
                     then volumen_kg else 0 end)         
                     as vol_Aseo
                            
                ,sum (case when (category_name ilike '%Aseo%' or  category_name ilike '%Limpe%'
                                or subcategory_name ilike '%Aseo%' or subcategory_name ilike '%Limpeza%' or subcategory_name ilike '%Higiene%')
                                then gmv_item  else 0 end) 
                                as gmv_Aseo
                                
                ,sum (case when (category_name ilike '%Aseo%' or  category_name ilike '%Limpe%'
                                or subcategory_name ilike '%Aseo%' or subcategory_name ilike '%Limpeza%' or subcategory_name ilike '%Higiene%')
                                then 1 else 0 end)           
                                as buy_Aseo
                
                --BEBIDAS
                ,sum (case when (category_name ilike '%Bebidas%' or subcategory_name ilike '%Bebidas%' or subcategory_name ilike '%Cerve%') 
                     then volumen_kg else 0 end)       
                     as vol_bebida
                     
                ,sum (case when (category_name ilike '%Bebidas%' or subcategory_name ilike '%Bebidas%' or subcategory_name ilike '%Cerve%')  
                     then gmv_item   else 0 end)        
                     as gmv_bebida
                    
                ,sum (case when (category_name ilike '%Bebidas%' or subcategory_name ilike '%Bebidas%' or subcategory_name ilike '%Cerve%')  
                     then 1  else 0 end)        
                     as buy_bebida
                    
                --CONGELADOS    
                
                ,sum (case when (category_name ilike '%Congelados%' or subcategory_name  ilike '%Congelados%') 
                     then volumen_kg else 0 end) 
                     as vol_congelados

                ,sum (case when (category_name ilike '%Congelados%' or subcategory_name ilike '%Congelados%') 
                     then gmv_item   else 0 end)        
                     as gmv_congelados

                ,sum (case when (category_name ilike '%Congelados%' or subcategory_name ilike '%Congelados%') 
                     then 1 else 0 end)        
                     as buy_congelados
                
                --DESECHABLES Y UTENSILIOS
            
                ,sum (case when (category_name ilike '%Desechables%' or category_name ilike '%Descartáveis%'
                     or subcategory_name ilike '%Desechables%' or subcategory_name ilike '%Utensilios%')
                     then volumen_kg else 0 end)       
                     as vol_desechables
                
                ,sum (case when (category_name ilike '%Desechables%' or category_name ilike '%Descartáveis%'
                     or subcategory_name ilike '%Desechables%' or subcategory_name ilike '%Utensilios%') 
                     then gmv_item   else 0 end)        
                     as gmv_desechables
                     
                ,sum (case when (category_name ilike '%Desechables%' or category_name ilike '%Descartáveis%'
                     or subcategory_name ilike '%Desechables%' or subcategory_name ilike '%Utensilios%')  
                     then 1 else 0 end)        
                     as buy_desechables
            
                -- FRUTAS Y VERDURAS
                ,sum (case when (category_name ilike '%Frutas%' or category_name ilike '%Verduras%' 
                      or subcategory_name ilike'%Frutas%' or subcategory_name ilike'%Legumbres%' 
                      or subcategory_name ilike'%Tuberculos%' or subcategory_name ilike'%Verduras%' )
                      then volumen_kg else 0 end)        
                      as vol_fruver
                      
                ,sum (case when (category_name ilike '%Frutas%' or category_name ilike '%Verduras%' 
                      or subcategory_name ilike'%Frutas%' or subcategory_name ilike'%Legumbres%' 
                      or subcategory_name ilike'%Tuberculos%' or subcategory_name ilike'%Verduras%' )  
                      then gmv_item else 0 end)       
                      as gmv_fruver
                     
                ,sum (case when (category_name ilike '%Frutas%' or category_name ilike '%Verduras%' 
                     or subcategory_name ilike'%Frutas%' or subcategory_name ilike'%Legumbres%' 
                     or subcategory_name ilike'%Tuberculos%' or subcategory_name ilike'%Verduras%') 
                     then 1 else 0 end)
                     as buy_fruver
                
                --HUEVOS
                ,sum (case when (category_name ilike '%Ovos%' or category_name ilike '%Huevos%'
                      or subcategory_name ilike'%Lacteos y Huevos%' or subcategory_name ilike'%Lácteos & Huevos%') 
                     then volumen_kg else 0 end)        
                     as vol_Lacteos_huevos
                     
                ,sum (case when (category_name ilike '%Ovos%' or category_name ilike '%Huevos%'
                      or subcategory_name ilike'%Lacteos y Huevos%' or subcategory_name ilike'%Lácteos & Huevos%') 
                      then gmv_item   else 0 end)        
                      as gmv_Lacteos_huevos
                
                ,sum (case when (category_name ilike '%Ovos%' or category_name ilike '%Huevos%'
                      or subcategory_name ilike'%Lacteos y Huevos%' or subcategory_name ilike'%Lácteos & Huevos%') 
                     then 1 else 0 end)        
                     as buy_Lacteos_huevos
                
                --PROTEINAS

                ,sum (case when (category_name ilike '%Proteínas%' or subcategory_name ilike '%Proteínas%'
                     or subcategory_name ilike'%Pescado%')
                     then volumen_kg else 0 end)       
                     as vol_proteinas
                            
                ,sum (case when (category_name ilike '%Proteínas%' or subcategory_name ilike '%Proteínas%'
                     or subcategory_name ilike'%Pescado%')
                     then gmv_item   else 0 end)        
                     as gmv_proteinas
                
                ,sum (case when (category_name ilike '%Proteínas%' or subcategory_name ilike '%Proteínas%'
                     or subcategory_name ilike'%Pescado%')
                     then 1  else 0 end)        
                     as buy_proteinas
                
                
                ----subcategorias---
                ,sum (case when (subcategory_name ilike '%Aceite%' or subcategory_name ilike '%Azeite%') then volumen_kg else 0 end) as vol_sub_3_Aceites_Grasas
                ,sum (case when (subcategory_name ilike '%Aceite%' or subcategory_name ilike '%Azeite%') then gmv_item   else 0 end) as gmv_sub_3_Aceites_Grasas
                ,sum (case when (subcategory_name ilike '%Aceite%' or subcategory_name ilike '%Azeite%') then 1   else 0 end)        as buy_sub_3_Aceites_Grasas
                
                ,sum (case when (subcategory_name ilike '%Arro%')  then volumen_kg else 0 end) as vol_sub_5_Arroz
                ,sum (case when (subcategory_name ilike '%Arro%')  then gmv_item   else 0 end) as gmv_sub_5_Arroz
                ,sum (case when (subcategory_name ilike '%Arro%')  then 1          else 0 end) as buy_sub_5_Arroz
                
                ,sum (case when (subcategory_name ilike '%Azúca%' or subcategory_name ilike '%Açúca%') then volumen_kg  else 0 end) as vol_sub_6_Azucar_Endulzantes
                ,sum (case when (subcategory_name ilike '%Azúca%' or subcategory_name ilike '%Açúca%') then gmv_item    else 0 end) as gmv_sub_6_Azucar_Endulzantes
                ,sum (case when (subcategory_name ilike '%Azúca%' or subcategory_name ilike '%Açúca%') then 1           else 0 end) as buy_sub_6_Azucar_Endulzantes
                
                ,sum (case when (subcategory_name ilike '%Café%') then volumen_kg else 0 end) as vol_sub_8_Cafe_Chocolate_Infusiones
                ,sum (case when (subcategory_name ilike '%Café%') then gmv_item   else 0 end) as gmv_sub_8_Cafe_Chocolate_Infusiones
                ,sum (case when (subcategory_name ilike '%Café%') then 1          else 0 end) as buy_sub_8_Cafe_Chocolate_Infusiones
                
                ,sum (case when (subcategory_name ilike '%Cerdo%' or subcategory_name ilike '%Suíno%') then volumen_kg else 0 end) as vol_sub_9_Cerdo
                ,sum (case when (subcategory_name ilike '%Cerdo%' or subcategory_name ilike '%Suíno%') then gmv_item   else 0 end) as gmv_sub_9_Cerdo
                ,sum (case when (subcategory_name ilike '%Cerdo%' or subcategory_name ilike '%Suíno%') then 1          else 0 end) as buy_sub_9_Cerdo
                
                ,sum (case when (subcategory_name ilike '%Chiles%') then volumen_kg else 0 end) as vol_sub_11_Chiles
                ,sum (case when (subcategory_name ilike '%Chiles%') then gmv_item   else 0 end) as gmv_sub_11_Chiles
                ,sum (case when (subcategory_name ilike '%Chiles%') then 1          else 0 end) as buy_sub_11_Chiles
                
                ,sum (case when (subcategory_name ilike '%Lácteos%' or subcategory_name ilike '%Cremas%' or subcategory_name ilike '%Lácteos & Huevos%') then volumen_kg else 0 end) as vol_sub_15_Derivados_Lacteos
                ,sum (case when (subcategory_name ilike '%Lácteos%' or subcategory_name ilike '%Cremas%' or subcategory_name ilike '%Lácteos & Huevos%') then gmv_item   else 0 end) as gmv_sub_15_Derivados_Lacteos
                ,sum (case when (subcategory_name ilike '%Lácteos%' or subcategory_name ilike '%Cremas%' or subcategory_name ilike '%Lácteos & Huevos%') then 1          else 0 end) as buy_sub_15_Derivados_Lacteos
               
                ,sum (case when (subcategory_name ilike 'Embutidos') then volumen_kg else 0 end) as vol_sub_20_Embutidos
                ,sum (case when (subcategory_name ilike 'Embutidos') then gmv_item   else 0 end) as gmv_sub_20_Embutidos
                ,sum (case when (subcategory_name ilike 'Embutidos') then 1          else 0 end) as buy_sub_20_Embutidos
                
                ,sum (case when (subcategory_name ilike '%Enlatado%' or subcategory_name ilike '%Fruto%') then volumen_kg else 0 end) as vol_sub_22_Enlatados
                ,sum (case when (subcategory_name ilike '%Enlatado%' or subcategory_name ilike '%Fruto%') then gmv_item   else 0 end) as gmv_sub_22_Enlatados
                ,sum (case when (subcategory_name ilike '%Enlatado%' or subcategory_name ilike '%Fruto%') then 1          else 0 end) as buy_sub_22_Enlatados
                
                ,sum (case when (subcategory_name ilike '%Especi%' or subcategory_name ilike '%Tempero%') then volumen_kg else 0 end) as vol_sub_23_Especias
                ,sum (case when (subcategory_name ilike '%Especi%' or subcategory_name ilike '%Tempero%') then gmv_item   else 0 end) as gmv_sub_23_Especias
                ,sum (case when (subcategory_name ilike '%Especi%' or subcategory_name ilike '%Tempero%') then 1          else 0 end) as buy_sub_23_Especias
                
                ,sum (case when (subcategory_name ilike '%Fruta%' ) then volumen_kg else 0 end) as vol_sub_24_Frutas
                ,sum (case when (subcategory_name ilike '%Fruta%' ) then gmv_item   else 0 end) as gmv_sub_24_Frutas
                ,sum (case when (subcategory_name ilike '%Fruta%' ) then 1          else 0 end) as buy_sub_24_Frutas
                
                ,sum (case when (subcategory_name ilike '%Grano%' or subcategory_name ilike '%Grão%' or subcategory_name ilike '%Feij%') then volumen_kg else 0 end) as vol_sub_26_Granos
                ,sum (case when (subcategory_name ilike '%Grano%' or subcategory_name ilike '%Grão%' or subcategory_name ilike '%Feij%') then gmv_item   else 0 end) as gmv_sub_26_Granos
                ,sum (case when (subcategory_name ilike '%Grano%' or subcategory_name ilike '%Grão%' or subcategory_name ilike '%Feij%') then 1          else 0 end) as buy_sub_26_Granos
                
                ,sum (case when (subcategory_name ilike '%Harina%' or subcategory_name ilike '%Farinha%') then volumen_kg else 0 end) as vol_sub_27_Harinas_Mezclas
                ,sum (case when (subcategory_name ilike '%Harina%' or subcategory_name ilike '%Farinha%') then gmv_item   else 0 end) as gmv_sub_27_Harinas_Mezclas
                ,sum (case when (subcategory_name ilike '%Harina%' or subcategory_name ilike '%Farinha%') then 1          else 0 end) as buy_sub_27_Harinas_Mezclas
                
                ,sum (case when (subcategory_name ilike '%Huevo%' or subcategory_name ilike '%Ovos%') then volumen_kg else 0 end) as vol_sub_30_Huevos
                ,sum (case when (subcategory_name ilike '%Huevo%' or subcategory_name ilike '%Ovos%') then gmv_item   else 0 end) as gmv_sub_30_Huevos
                ,sum (case when (subcategory_name ilike '%Huevo%' or subcategory_name ilike '%Ovos%') then 1          else 0 end) as buy_sub_30_Huevos
                
                ,sum (case when (subcategory_name ilike '%Leche%' or subcategory_name ilike '%Lact%' or subcategory_name ilike '%Leit%') then volumen_kg else 0 end) as vol_sub_32_Leches
                ,sum (case when (subcategory_name ilike '%Leche%' or subcategory_name ilike '%Lact% 'or subcategory_name ilike '%Leit%')  then gmv_item   else 0 end) as gmv_sub_32_Leches
                ,sum (case when (subcategory_name ilike '%Leche%' or subcategory_name ilike '%Lact%' or subcategory_name ilike '%Leit%')  then 1          else 0 end) as buy_sub_32_Leches
                
                ,sum (case when (subcategory_name ilike '%Mante%') then volumen_kg else 0 end) as vol_sub_33_Mantequillas_Margarinas
                ,sum (case when (subcategory_name ilike '%Mante%') then gmv_item   else 0 end) as gmv_sub_33_Mantequillas_Margarinas
                ,sum (case when (subcategory_name ilike '%Mante%') then 1          else 0 end) as buy_sub_33_Mantequillas_Margarinas
                
                ,sum (case when (subcategory_name ilike '%Embu%' or subcategory_name = 'Otras Proteínas Frescas' or subcategory_name = 'Proteínas Frescas' or  (subcategory_name = 'Otros' and (category_name = 'Proteínas Congeladas' or category_name = 'Proteínas Frescas'))) then volumen_kg else 0 end) as vol_sub_34_Otras_Proteinas
                ,sum (case when (subcategory_name ilike '%Embu%' or subcategory_name = 'Otras Proteínas Frescas' or subcategory_name = 'Proteínas Frescas' or  (subcategory_name = 'Otros' and (category_name = 'Proteínas Congeladas' or category_name = 'Proteínas Frescas'))) then volumen_kg else 0 end) as gmv_sub_34_Otras_Proteinas
                ,sum (case when (subcategory_name ilike '%Embu%' or subcategory_name = 'Otras Proteínas Frescas' or subcategory_name = 'Proteínas Frescas' or  (subcategory_name = 'Otros' and (category_name = 'Proteínas Congeladas' or category_name = 'Proteínas Frescas'))) then 1          else 0 end) as buy_sub_34_Otras_Proteinas
                
                ,sum (case when (subcategory_name ilike '%Otros Abarrotes%' or subcategory_name ilike '%Condimento%') then volumen_kg else 0 end) as vol_sub_36_Otros_Abarrotes
                ,sum (case when (subcategory_name ilike '%Otros Abarrotes%' or subcategory_name ilike '%Condimento%') then gmv_item   else 0 end) as gmv_sub_36_Otros_Abarrotes
                ,sum (case when (subcategory_name ilike '%Otros Abarrotes%' or subcategory_name ilike '%Condimento%') then 1          else 0 end) as buy_sub_36_Otros_Abarrotes
                
                ,sum (case when (subcategory_name ilike '%Panader%'or subcategory_name ilike '%Confit%') then volumen_kg else 0 end) as vol_sub_37_Panaderia_Tortilleria
                ,sum (case when (subcategory_name ilike '%Panader%'or subcategory_name ilike '%Confit%') then gmv_item   else 0 end) as gmv_sub_37_Panaderia_Tortilleria
                ,sum (case when (subcategory_name ilike '%Panader%'or subcategory_name ilike '%Confit%') then 1          else 0 end) as buy_sub_37_Panaderia_Tortilleria
                
                ,sum (case when (subcategory_name ilike '%Pasta%' or subcategory_name ilike '%Massa%') then volumen_kg else 0 end) as vol_sub_39_Pasta
                ,sum (case when (subcategory_name ilike '%Pasta%' or subcategory_name ilike '%Massa%') then gmv_item   else 0 end) as gmv_sub_39_Pasta
                ,sum (case when (subcategory_name ilike '%Pasta%' or subcategory_name ilike '%Massa%') then 1          else 0 end) as buy_sub_39_Pasta
                
                ,sum (case when (subcategory_name ilike '%Pescado%' or subcategory_name = 'Peix') then volumen_kg else 0 end) as vol_sub_40_Pescados_Mariscos_Congelado
                ,sum (case when (subcategory_name ilike '%Pescado%' or subcategory_name = 'Peix') then gmv_item   else 0 end) as gmv_sub_40_Pescados_Mariscos_Congelado
                ,sum (case when (subcategory_name ilike '%Pescado%' or subcategory_name = 'Peix') then 1          else 0 end) as buy_sub_40_Pescados_Mariscos_Congelado
                
                ,sum (case when (subcategory_name ilike '%Pollo%' or subcategory_name ilike '%Frango%') then volumen_kg else 0 end) as vol_sub_41_Pollo
                ,sum (case when (subcategory_name ilike '%Pollo%' or subcategory_name ilike '%Frango%') then gmv_item   else 0 end) as gmv_sub_41_Pollo
                ,sum (case when (subcategory_name ilike '%Pollo%' or subcategory_name ilike '%Frango%') then 1          else 0 end) as buy_sub_41_Pollo
                
                ,sum (case when (subcategory_name ilike '%Que%') then volumen_kg else 0 end) as vol_sub_43_Quesos
                ,sum (case when (subcategory_name ilike '%Que%') then gmv_item   else 0 end) as gmv_sub_43_Quesos
                ,sum (case when (subcategory_name ilike '%Que%') then 1          else 0 end) as buy_sub_43_Quesos
                
                ,sum (case when (subcategory_name ilike 'Rappi') then volumen_kg else 0 end) as vol_sub_44_Rappi
                ,sum (case when (subcategory_name ilike 'Rappi') then gmv_item   else 0 end) as gmv_sub_44_Rappi
                ,sum (case when (subcategory_name ilike 'Rappi') then 1          else 0 end) as buy_sub_44_Rappi
                
                ,sum (case when (subcategory_name ilike '%Res%' or subcategory_name ilike '%Vermel%') then volumen_kg else 0 end) as vol_sub_45_Res
                ,sum (case when (subcategory_name ilike '%Res%' or subcategory_name ilike '%Vermel%') then gmv_item   else 0 end) as gmv_sub_45_Res
                ,sum (case when (subcategory_name ilike '%Res%' or subcategory_name ilike '%Vermel%') then 1          else 0 end) as buy_sub_46_Res
                
                ,sum (case when (subcategory_name ilike '%Sal%') then volumen_kg else 0 end) as vol_sub_47_Sal_Sazonadores
                ,sum (case when (subcategory_name ilike '%Sal%') then gmv_item   else 0 end) as gmv_sub_47_Sal_Sazonadores
                ,sum (case when (subcategory_name ilike '%Sal%') then 1          else 0 end) as buy_sub_47_Sal_Sazonadores
                
                ,sum (case when (subcategory_name ilike '%Salsas%' or subcategory_name ilike '%Molhos%') then volumen_kg else 0 end) as vol_sub_48_Salsas
                ,sum (case when (subcategory_name ilike '%Salsas%' or subcategory_name ilike '%Molhos%') then gmv_item   else 0 end) as gmv_sub_48_Salsas
                ,sum (case when (subcategory_name ilike '%Salsas%' or subcategory_name ilike '%Molhos%') then gmv_item   else 0 end) as buy_sub_48_Salsas
                
                ,sum (case when (subcategory_name ilike '%Tubérculos%') then volumen_kg else 0 end) as vol_sub_49_Tuberculos
                ,sum (case when (subcategory_name ilike '%Tubérculos%') then gmv_item   else 0 end) as gmv_sub_49_Tuberculos
                ,sum (case when (subcategory_name ilike '%Tubérculos%') then 1          else 0 end) as buy_sub_49_Tuberculos
                
                ,sum (case when (subcategory_name = 'Verduras' or subcategory_name ilike '%Hortaliças%' or subcategory_name ilike '%Legum%' or subcategory_name ilike '%Cogumelos%' or subcategory_name ilike '%Folhas%' or subcategory_name ilike '%Hierbas%' or subcategory_name ilike '%Hongos%' or subcategory_name ilike '%Raízes%' ) then volumen_kg else 0 end) as vol_sub_51_Verduras
                ,sum (case when (subcategory_name = 'Verduras' or subcategory_name ilike '%Hortaliças%' or subcategory_name ilike '%Legum%' or subcategory_name ilike '%Cogumelos%' or subcategory_name ilike '%Folhas%' or subcategory_name ilike '%Hierbas%' or subcategory_name ilike '%Hongos%' or subcategory_name ilike '%Raízes%') then gmv_item   else 0 end) as gmv_sub_51_Verduras
                ,sum (case when (subcategory_name = 'Verduras' or subcategory_name ilike '%Hortaliças%' or subcategory_name ilike '%Legum%' or subcategory_name ilike '%Cogumelos%' or subcategory_name ilike '%Folhas%' or subcategory_name ilike '%Hierbas%' or subcategory_name ilike '%Hongos%' or subcategory_name ilike '%Raízes%') then 1          else 0 end) as buy_sub_51_Verduras
                
                ,sum (case when (subcategory_name ilike '%Bebida%' or subcategory_name ilike '%Refres%' or subcategory_name ilike '%Polpa%' or subcategory_name ilike '%Pulpa%' or subcategory_name ilike '%Cerve%') then volumen_kg else 0 end) as vol_sub_51_Bebidas
                ,sum (case when (subcategory_name ilike '%Bebida%' or subcategory_name ilike '%Refres%' or subcategory_name ilike '%Polpa%' or subcategory_name ilike '%Pulpa%' or subcategory_name ilike '%Cerve%') then gmv_item   else 0 end) as gmv_sub_51_Bebidas
                ,sum (case when (subcategory_name ilike '%Bebida%' or subcategory_name ilike '%Refres%' or subcategory_name ilike '%Polpa%' or subcategory_name ilike '%Pulpa%' or subcategory_name ilike '%Cerve%') then 1          else 0 end) as buy_sub_51_Bebidas
                
          from  Ordenesxitem  OI 

group by order_boleto,dia_boleto,customer_id)

select agrupacion_orden.*
,agrupacion_skus.*
,total_discount/numero_ordenes as discountxo
,size_order_kg_real/numero_ordenes as sizexo
,numero_ordenes/numero_lineas_time as oxlineas
,numero_ordenes/numero_subcategorias_time as oxsubcat
,numero_ordenes/numero_categoria_time as oxcat

from (select distinct  IC.customer_id,order_boleto,dia_boleto
,sum(gmv) as gmv_total
,count(distinct order_id) as numero_ordenes
,avg(numero_lineas_orden) as promedio_numero_lineas
,sum(gmv)/avg(numero_lineas_orden) as gmv_linea_porden
,count(distinct order_id)/avg(numero_lineas_orden) as ordenesxplineas

from info_cliente IC
group by order_boleto,dia_boleto,customer_id
) as agrupacion_orden

inner join agrupacion_skus  on (agrupacion_skus.order_boleto=agrupacion_orden.order_boleto)

    """.format(TIME=TIME)
    
    dataframe = read_connection_data_warehouse.runQuery(query)
    
    return dataframe

###################################################################################################################################################################################
###################################################################################################################################################################################
###################################################################################################################################################################################
###################################################################################################################################################################################

def data_trans_order_boleto():
    """
    Owner: Gabo Moreno
    Está función trae la data de ventas, para segmentos de restaurantes, para una ciudad en un rango de fecha determinado.
    
    Argumentos:
    Time: número de días que se requiere agrupar
 
    Resultados:
    dataframe: dataframe con la data de ventas solicitada
    """
    
    query = """
    With orden_boleto as(select   distinct bc.customer_id 
                                ,bo.order_id    as order_boleto 
                                ,bo.order_number
                                ,fbs.name as segmento  
                                ,s.site_identifier_value
                                ,Extract(Week  from bo.date_created) as bweek
                                ,Extract(year  from bo.date_created) as byear
                                ,bo.date_created::DATE as dia_boleto
                                        
                                FROM postgres_broadleaf_federate."broadleaf.blc_order"  bo 
                                inner join lnd_proc_dev.frucap_boletos                                   fru  on fru.order_code=bo.order_number
                                inner join postgres_broadleaf_federate."broadleaf.blc_fulfillment_group" bfg  ON bfg.order_id = bo.order_id
                                inner join postgres_broadleaf_federate."broadleaf.blc_fulfillment_order" bfo  ON bfo.fulfillment_group_id = bfg.fulfillment_group_id
                                inner join postgres_broadleaf_federate."broadleaf.fb_fulfillment_group"  ffg  ON ffg.fulfillment_group_id = bfg.fulfillment_group_id
                                inner join postgres_broadleaf_federate."broadleaf.fb_order"              fo   ON fo.order_id = bo.order_id
                                inner join postgres_broadleaf_federate."broadleaf.blc_order_payment"     bop  ON bop.order_id = bo.order_id
                                inner join postgres_broadleaf_federate."broadleaf.blc_site"              s    ON s.site_id = bo.site_disc
                                inner join postgres_broadleaf_federate."broadleaf.blc_customer"          bc   ON bc.customer_id = bo.customer_id
                                left join  postgres_broadleaf_federate."broadleaf.fb_order_type"         fot  ON fot.fb_order_type_id=fo.fb_order_type_id
                                left join  postgres_broadleaf_federate."broadleaf.fb_customer"           fc   ON fc.customer_id = bo.customer_id
                                left join  postgres_broadleaf_federate."broadleaf.fb_business_type"      fbs  ON fc.business_type_id = fbs.business_type_id 
                                
                               -- WHERE fo.fb_order_status_id IN (1,6,7,8)
                                 --   AND bo.order_status = 'SUBMITTED'
                                 --   AND bfo.status NOT IN ('ARCHIVED','CANCELLED')
                                 --   AND bop.archived = 'N'
                                 --   AND (fot.name IS NULL OR fot.name <> 'REFUND')
                                    )

 ,ids as (select distinct customer_id from orden_boleto)
 

,conversion as (select distinct bsku.sku_id
                ,coalesce(conversion_value_y, 1) conversion_value_y
                ,coalesce(conversion_value_x, 1) conversion_value_x
                ,coalesce(conversion_type_y, fp.unit) conversion_type_y
                ,coalesce(conversion_type_x, fp.unit) conversion_type_x
                ,fp.unit
                ,bcat.name          as subcategory_name
                ,bcat.category_id   as sub_category_id
                ,bcat2.category_id  as category_id_main
                ,bcat2.name         as category_name
                from  postgres_broadleaf_federate."broadleaf.blc_sku"                 bsku 
                inner join postgres_broadleaf_federate."broadleaf.blc_sku"            s3    ON bsku.addl_product_id = s3.default_product_id
	            inner join postgres_broadleaf_federate."broadleaf.fb_sku"             s4    ON bsku.sku_id = s4.sku_id
                left join  postgres_broadleaf_federate."broadleaf.blc_product"        bp    ON bsku.addl_product_id = bp.product_id
                left join  postgres_broadleaf_federate."broadleaf.blc_category_xref"  bcx   ON bcx.sub_category_id = bp.default_category_id and bcx.archived='N' and bcx.sndbx_tier is null --Relaciones categorias 
                left join  postgres_broadleaf_federate."broadleaf.blc_category"       bcat  ON bcat.category_id = bp.default_category_id --Nombre subcategoria
                left join  postgres_broadleaf_federate."broadleaf.blc_category"       bcat2 ON bcx.category_id = bcat2.category_id 
                inner join postgres_broadleaf_federate."broadleaf.fb_product"         fp    ON bp.product_id = fp.product_id
                )

,info_cliente as (select distinct order_boleto
                ,dia_boleto
                ,bo.customer_id 
                ,bo.order_id
                ,bo.date_created as date_order
                
                ,CASE   WHEN s.site_identifier_value in ('CMX','GDL','MTY','PBC') then (bo.order_subtotal+coalesce(fo.total_tax_iva,0)+coalesce(bo.total_shipping,0))/19.19
                        WHEN s.site_identifier_value in ('SPO','BHZ','CWB') then (bo.order_subtotal+coalesce(fo.total_tax_iva,0)+coalesce(bo.total_shipping,0))/3.88
                        ELSE (bo.order_subtotal+coalesce(fo.total_tax_iva,0)+coalesce(bo.total_shipping,0))/3000 end as gmv

                ,count(distinct boi.order_item_id)    as numero_lineas_orden 

                from postgres_broadleaf_federate."broadleaf.blc_fulfillment_group_item" bfgi
                INNER JOIN postgres_broadleaf_federate."broadleaf.blc_fulfillment_group" bfg  ON bfgi.fulfillment_group_id = bfg.fulfillment_group_id
                INNER JOIN postgres_broadleaf_federate."broadleaf.fb_fulfillment_group"  ffg  ON ffg.fulfillment_group_id = bfg.fulfillment_group_id
                INNER JOIN postgres_broadleaf_federate."broadleaf.blc_fulfillment_order" bfo  ON bfo.fulfillment_group_id= bfg.fulfillment_group_id
                INNER JOIN postgres_broadleaf_federate."broadleaf.blc_order"             bo   ON bo.order_id = bfg.order_id
                inner join ids                                                           ids  on ids.customer_id=bo.customer_id
                inner join orden_boleto                                                  fru  on fru.order_boleto=bo.order_id
                INNER JOIN postgres_broadleaf_federate."broadleaf.blc_order_item"        boi  ON bfgi.order_item_id= boi.order_item_id
                inner join postgres_broadleaf_federate."broadleaf.fb_order"              fo   ON fo.order_id = bo.order_id
                inner join postgres_broadleaf_federate."broadleaf.blc_order_payment"     bop  ON bop.order_id = bo.order_id
                inner join postgres_broadleaf_federate."broadleaf.blc_site"              s    ON s.site_id = bo.site_disc
                inner join postgres_broadleaf_federate."broadleaf.blc_customer"          bc   ON bc.customer_id = bo.customer_id
                left join  postgres_broadleaf_federate."broadleaf.fb_order_type"         fot  ON fot.fb_order_type_id=fo.fb_order_type_id

               -- where   fo.fb_order_status_id IN (1,6,7,8)
                 -- AND  bo.order_status = 'SUBMITTED'
                 -- AND  bfo.status NOT IN ('ARCHIVED','CANCELLED')
                --  AND  bop.archived = 'N'
                --  AND  (fot.name IS NULL OR fot.name <> 'REFUND')
                  group by 1,2,3,4,5,6
                ) 

, ordenes as(select distinct order_boleto
                    ,dia_boleto
                    ,order_id
                    ,date_order
                    from info_cliente)

,Ordenesxitem as (select distinct boi.order_item_id
                        ,order_boleto
                        ,dia_boleto
                        ,bo.order_id
                        ,date_order
                        ,bo.customer_id --1

                        ,case when s.site_identifier_value in ('CMX','GDL','MTY','PBC')
                                   then nvl((baid.adjustment_value * boip.quantity * foi.step_unit)/19.19, 0)
                                 when  s.site_identifier_value in ('SPO','BHZ','CWB')
                                       then nvl((baid.adjustment_value * boip.quantity * foi.step_unit)/3.88, 0)
                                 else nvl((baid.adjustment_value * boip.quantity * foi.step_unit)/3000, 0)
                                 end as discount_applied

                        ,case when s.site_identifier_value in ('CMX','GDL','MTY','PBC')
                                   then ((boi.quantity * foi.step_unit * boi.sale_price)/19.19)
                             when  s.site_identifier_value in ('SPO','BHZ','CWB')
                                   then ((boi.quantity * foi.step_unit * boi.sale_price)/3.88)
                             else ((boi.quantity * foi.step_unit * boi.sale_price)/3000) 
                             end as gmv_item

                        ,case when s.site_identifier_value in ('CMX','GDL','MTY','PBC')
                                 then  nvl((foi.total_price-foi.total_price_adj)/19.19, 0)
                                 when  s.site_identifier_value in ('SPO','BHZ','CWB')
                                 then  nvl((foi.total_price-foi.total_price_adj)/3.88, 0)
                                 else  nvl((foi.total_price-foi.total_price_adj)/3000, 0)
                                 end   as total_discount_order

                        ,boi.quantity * foi.step_unit as real_quantity
                        ,case when con.unit = 'UNID' then conversion_value_y*(boi.quantity * foi.step_unit) else (boi.quantity * foi.step_unit) end as volumen_kg
                       
                        --,boi.name
                        --,bdoi.sku_id
                        --,bs.addl_product_id as padre_sku_id,

                        ,con.subcategory_name
                        ,con.sub_category_id
                        ,con.category_name
                        ,con.category_id_main

                         from postgres_broadleaf_federate."broadleaf.blc_fulfillment_group_item" bfgi
                            INNER JOIN postgres_broadleaf_federate."broadleaf.blc_fulfillment_group" bfg  ON bfgi.fulfillment_group_id = bfg.fulfillment_group_id
                            INNER JOIN postgres_broadleaf_federate."broadleaf.fb_fulfillment_group"  ffg  ON ffg.fulfillment_group_id = bfg.fulfillment_group_id
                            INNER JOIN postgres_broadleaf_federate."broadleaf.blc_fulfillment_order" bfo  ON bfo.fulfillment_group_id= bfg.fulfillment_group_id
                            INNER JOIN postgres_broadleaf_federate."broadleaf.blc_order"             bo   ON bo.order_id = bfg.order_id
                            inner join ids                                                           ids  on ids.customer_id=bo.customer_id
                            inner join ordenes                                                            on ordenes.order_boleto=bo.order_id
                            INNER JOIN postgres_broadleaf_federate."broadleaf.blc_order_item"        boi  ON bfgi.order_item_id= boi.order_item_id
                            INNER JOIN postgres_broadleaf_federate."broadleaf.blc_site"              s    ON bo.site_disc = s.site_id
                            INNER JOIN postgres_broadleaf_federate."broadleaf.blc_discrete_order_item"  bdoi       ON bdoi.order_item_id = boi.order_item_id
                            INNER JOIN postgres_broadleaf_federate."broadleaf.fb_order_item"         foi  ON boi.order_item_id= foi.order_item_id
                            left join  conversion                                                    con  ON con.sku_id=bdoi.sku_id
                            inner join postgres_broadleaf_federate."broadleaf.fb_order"              fo   ON fo.order_id = bo.order_id
                            inner join postgres_broadleaf_federate."broadleaf.blc_order_payment"     bop  ON bop.order_id = bo.order_id
                            left join  postgres_broadleaf_federate."broadleaf.fb_order_type"         fot  ON fot.fb_order_type_id=fo.fb_order_type_id
                            LEFT JOIN postgres_broadleaf_federate."broadleaf.blc_order_item_price_dtl"  boip       ON  boi.order_item_id = boip.order_item_id
                            LEFT JOIN postgres_broadleaf_federate."broadleaf.blc_order_item_dtl_adj"    baid       ON  baid.order_item_price_dtl_id = boip.order_item_price_dtl_id
                           ) 
                       
,agrupacion_skus as (select distinct  OI.customer_id 
                    ,order_boleto
                    ,dia_boleto   

                    ,sum(discount_applied) as total_discount
                    ,sum(gmv_item) as gmv_subtotal_item
                    ,sum(volumen_kg) as size_order_kg_noreal
                    
                    ,count(distinct order_item_id)    as numero_lineas_time
                    ,count(distinct sub_category_id)  as numero_subcategorias_time 
                    ,count(distinct category_id_main) as numero_categoria_time

                    ,case when count(distinct order_item_id)    = 0 then 0 else sum(gmv_item)/count(distinct order_item_id)    end as gmv_linea
                    ,case when count(distinct order_item_id)    = 0 then 0 else sum(gmv_item)/count(order_item_id)             end as gmv_promedio_productos

                    ,case when count(distinct category_id_main) = 0 then 0 else sum(gmv_item)/count(distinct category_id_main) end as gmv_categoria
                    ,case when count(distinct sub_category_id)  = 0 then 0 else sum(gmv_item)/count(distinct sub_category_id)  end as gmv_subcategoria
         
                    --size_order_kg
                    ,sum(volumen_kg)-sum(case when (category_name ilike '%Aseo%' or  category_name ilike '%Limpe%' or subcategory_name ilike'%Aseo%' or subcategory_name ilike'%Limpeza%' 
                                                    or subcategory_name ilike'%Higiene%' or category_name ilike '%Desechables%' or category_name ilike '%Descartáveis%'
                                                    or subcategory_name ilike'%Desechables%' or subcategory_name ilike'%Utensilios%')
                                                    then volumen_kg else 0 end) 
                                                    as size_order_kg_real
                                                    
                    --gmv_kg                     
                    ,case when sum(volumen_kg)-sum(case when (category_name ilike '%Aseo%' or  category_name ilike '%Limpe%' or subcategory_name ilike'%Aseo%' or subcategory_name ilike'%Limpeza%' 
                                                    or subcategory_name ilike'%Higiene%' or category_name ilike '%Desechables%' or category_name ilike '%Descartáveis%'
                                                    or subcategory_name ilike'%Desechables%' or subcategory_name ilike'%Utensilios%')
                                                    then volumen_kg else 0 end) = 0 then 0 
                                else sum(gmv_item)/sum(volumen_kg)-sum(case when (category_name ilike '%Aseo%' or  category_name ilike '%Limpe%' or subcategory_name ilike'%Aseo%' or subcategory_name ilike'%Limpeza%' 
                                                                                    or subcategory_name ilike'%Higiene%' or category_name ilike '%Desechables%' or category_name ilike '%Descartáveis%'
                                                                                    or subcategory_name ilike'%Desechables%' or subcategory_name ilike'%Utensilios%')
                                then volumen_kg else 0 end) end 
                                as gmv_kg
                                
                    --gmv_linea_kg                   
                    ,case when count(distinct order_item_id) = 0 or (sum(volumen_kg)-sum (case when (category_name ilike '%Aseo%' or  category_name ilike '%Limpe%' or subcategory_name ilike'%Aseo%' or subcategory_name ilike'%Limpeza%' 
                                                                                                        or subcategory_name ilike'%Higiene%' or category_name ilike '%Desechables%' or category_name ilike '%Descartáveis%'
                                                                                                        or subcategory_name ilike'%Desechables%' or subcategory_name ilike'%Utensilios%')
                                                                                            then volumen_kg else 0 end)) = 0
                                then 0 else
                                        (sum(gmv_item)/count(distinct order_item_id))/(sum(volumen_kg)-sum (case when (category_name ilike '%Aseo%' or  category_name ilike '%Limpe%' or subcategory_name ilike'%Aseo%' or subcategory_name ilike'%Limpeza%' 
                                                                                                                        or subcategory_name ilike'%Higiene%' or category_name ilike '%Desechables%' or category_name ilike '%Descartáveis%'
                                                                                                                        or subcategory_name ilike'%Desechables%' or subcategory_name ilike'%Utensilios%')
                                                                                                            then volumen_kg else 0 end)) 
                                end as gmv_linea_kg
                                

                    --kg_linea     
                    ,case when count(distinct order_item_id) =0 then 0 else
                                (sum(volumen_kg)-sum (case when (category_name ilike '%Aseo%' or  category_name ilike '%Limpe%' or subcategory_name ilike'%Aseo%' or subcategory_name ilike'%Limpeza%' 
                                                                or subcategory_name ilike'%Higiene%' or category_name ilike '%Desechables%' or category_name ilike '%Descartáveis%'
                                                                or subcategory_name ilike'%Desechables%' or subcategory_name ilike'%Utensilios%')
                                                                then volumen_kg else 0 end))/count(distinct order_item_id) 
                                end as kg_linea
                    
                    --kg_subcat
                    ,case when count(distinct sub_category_id) =0 then 0 else
                                (sum(volumen_kg)-sum(case when (category_name ilike '%Aseo%' or  category_name ilike '%Limpe%' or subcategory_name ilike'%Aseo%' or subcategory_name ilike'%Limpeza%' 
                                                                or subcategory_name ilike'%Higiene%' or category_name ilike '%Desechables%' or category_name ilike '%Descartáveis%'
                                                                or subcategory_name ilike'%Desechables%' or subcategory_name ilike'%Utensilios%')
                                                                then volumen_kg else 0 end))/count(distinct sub_category_id) 
                                end as kg_subcat
                    
                    --kg_cat                         
                    ,case when count(distinct category_id_main) =0 then 0 else
                                 (sum(volumen_kg)-sum(case when (category_name ilike '%Aseo%' or  category_name ilike '%Limpe%' or subcategory_name ilike'%Aseo%' or subcategory_name ilike'%Limpeza%' 
                                                                 or subcategory_name ilike'%Higiene%' or category_name ilike '%Desechables%' or category_name ilike '%Descartáveis%'
                                                                 or subcategory_name ilike'%Desechables%' or subcategory_name ilike'%Utensilios%')
                                                                then volumen_kg else 0 end))/count(distinct category_id_main)
                    end as kg_cat
               
               
                --ABARROTES
                ,sum (case when (category_name ilike '%Abarrot%' or  category_name ilike '%Merce%' or subcategory_name ilike '%Abarrotes%' 
                      or subcategory_name ilike '%Aceites%' or subcategory_name ilike '%Mercearia%') 
                      then volumen_kg else 0 end)
                      as vol_Abarrotes
                      
                ,sum (case when (category_name ilike '%Abarrot%' or  category_name ilike  '%Merce%' or subcategory_name ilike '%Abarrotes%' 
                      or subcategory_name ilike '%Aceites%' or subcategory_name ilike '%Mercearia%') 
                      then gmv_item   else 0 end) 
                      as gmv_Abarrotes
                 
                ,sum (case when (category_name ilike '%Abarrot%' or  category_name ilike '%Merce%' or subcategory_name ilike '%Abarrotes%' 
                      or subcategory_name ilike '%Aceites%' or subcategory_name ilike '%Mercearia%') 
                      then 1 else 0 end)                       
                      as buy_Abarrotes
                
                
                --ASEO y LIMPIEZA
                ,sum (case when (category_name ilike '%Aseo%' or  category_name ilike '%Limpe%'
                     or subcategory_name ilike '%Aseo%' or subcategory_name ilike '%Limpeza%' or subcategory_name ilike '%Higiene%')
                     then volumen_kg else 0 end)         
                     as vol_Aseo
                            
                ,sum (case when (category_name ilike '%Aseo%' or  category_name ilike '%Limpe%'
                                or subcategory_name ilike '%Aseo%' or subcategory_name ilike '%Limpeza%' or subcategory_name ilike '%Higiene%')
                                then gmv_item  else 0 end) 
                                as gmv_Aseo
                                
                ,sum (case when (category_name ilike '%Aseo%' or  category_name ilike '%Limpe%'
                                or subcategory_name ilike '%Aseo%' or subcategory_name ilike '%Limpeza%' or subcategory_name ilike '%Higiene%')
                                then 1 else 0 end)           
                                as buy_Aseo
                
                --BEBIDAS
                ,sum (case when (category_name ilike '%Bebidas%' or subcategory_name ilike '%Bebidas%' or subcategory_name ilike '%Cerve%') 
                     then volumen_kg else 0 end)       
                     as vol_bebida
                     
                ,sum (case when (category_name ilike '%Bebidas%' or subcategory_name ilike '%Bebidas%' or subcategory_name ilike '%Cerve%')  
                     then gmv_item   else 0 end)        
                     as gmv_bebida
                    
                ,sum (case when (category_name ilike '%Bebidas%' or subcategory_name ilike '%Bebidas%' or subcategory_name ilike '%Cerve%')  
                     then 1  else 0 end)        
                     as buy_bebida
                    
                --CONGELADOS    
                
                ,sum (case when (category_name ilike '%Congelados%' or subcategory_name  ilike '%Congelados%') 
                     then volumen_kg else 0 end) 
                     as vol_congelados

                ,sum (case when (category_name ilike '%Congelados%' or subcategory_name ilike '%Congelados%') 
                     then gmv_item   else 0 end)        
                     as gmv_congelados

                ,sum (case when (category_name ilike '%Congelados%' or subcategory_name ilike '%Congelados%') 
                     then 1 else 0 end)        
                     as buy_congelados
                
                --DESECHABLES Y UTENSILIOS
            
                ,sum (case when (category_name ilike '%Desechables%' or category_name ilike '%Descartáveis%'
                     or subcategory_name ilike '%Desechables%' or subcategory_name ilike '%Utensilios%')
                     then volumen_kg else 0 end)       
                     as vol_desechables
                
                ,sum (case when (category_name ilike '%Desechables%' or category_name ilike '%Descartáveis%'
                     or subcategory_name ilike '%Desechables%' or subcategory_name ilike '%Utensilios%') 
                     then gmv_item   else 0 end)        
                     as gmv_desechables
                     
                ,sum (case when (category_name ilike '%Desechables%' or category_name ilike '%Descartáveis%'
                     or subcategory_name ilike '%Desechables%' or subcategory_name ilike '%Utensilios%')  
                     then 1 else 0 end)        
                     as buy_desechables
            
                -- FRUTAS Y VERDURAS
                ,sum (case when (category_name ilike '%Frutas%' or category_name ilike '%Verduras%' 
                      or subcategory_name ilike'%Frutas%' or subcategory_name ilike'%Legumbres%' 
                      or subcategory_name ilike'%Tuberculos%' or subcategory_name ilike'%Verduras%' )
                      then volumen_kg else 0 end)        
                      as vol_fruver
                      
                ,sum (case when (category_name ilike '%Frutas%' or category_name ilike '%Verduras%' 
                      or subcategory_name ilike'%Frutas%' or subcategory_name ilike'%Legumbres%' 
                      or subcategory_name ilike'%Tuberculos%' or subcategory_name ilike'%Verduras%' )  
                      then gmv_item else 0 end)       
                      as gmv_fruver
                     
                ,sum (case when (category_name ilike '%Frutas%' or category_name ilike '%Verduras%' 
                     or subcategory_name ilike'%Frutas%' or subcategory_name ilike'%Legumbres%' 
                     or subcategory_name ilike'%Tuberculos%' or subcategory_name ilike'%Verduras%') 
                     then 1 else 0 end)
                     as buy_fruver
                
                --HUEVOS
                ,sum (case when (category_name ilike '%Ovos%' or category_name ilike '%Huevos%'
                      or subcategory_name ilike'%Lacteos y Huevos%' or subcategory_name ilike'%Lácteos & Huevos%') 
                     then volumen_kg else 0 end)        
                     as vol_Lacteos_huevos
                     
                ,sum (case when (category_name ilike '%Ovos%' or category_name ilike '%Huevos%'
                      or subcategory_name ilike'%Lacteos y Huevos%' or subcategory_name ilike'%Lácteos & Huevos%') 
                      then gmv_item   else 0 end)        
                      as gmv_Lacteos_huevos
                
                ,sum (case when (category_name ilike '%Ovos%' or category_name ilike '%Huevos%'
                      or subcategory_name ilike'%Lacteos y Huevos%' or subcategory_name ilike'%Lácteos & Huevos%') 
                     then 1 else 0 end)        
                     as buy_Lacteos_huevos
                
                --PROTEINAS

                ,sum (case when (category_name ilike '%Proteínas%' or subcategory_name ilike '%Proteínas%'
                     or subcategory_name ilike'%Pescado%')
                     then volumen_kg else 0 end)       
                     as vol_proteinas
                            
                ,sum (case when (category_name ilike '%Proteínas%' or subcategory_name ilike '%Proteínas%'
                     or subcategory_name ilike'%Pescado%')
                     then gmv_item   else 0 end)        
                     as gmv_proteinas
                
                ,sum (case when (category_name ilike '%Proteínas%' or subcategory_name ilike '%Proteínas%'
                     or subcategory_name ilike'%Pescado%')
                     then 1  else 0 end)        
                     as buy_proteinas
                
                
                ----subcategorias---
                ,sum (case when (subcategory_name ilike '%Aceite%' or subcategory_name ilike '%Azeite%') then volumen_kg else 0 end) as vol_sub_3_Aceites_Grasas
                ,sum (case when (subcategory_name ilike '%Aceite%' or subcategory_name ilike '%Azeite%') then gmv_item   else 0 end) as gmv_sub_3_Aceites_Grasas
                ,sum (case when (subcategory_name ilike '%Aceite%' or subcategory_name ilike '%Azeite%') then 1   else 0 end)        as buy_sub_3_Aceites_Grasas
                
                ,sum (case when (subcategory_name ilike '%Arro%')  then volumen_kg else 0 end) as vol_sub_5_Arroz
                ,sum (case when (subcategory_name ilike '%Arro%')  then gmv_item   else 0 end) as gmv_sub_5_Arroz
                ,sum (case when (subcategory_name ilike '%Arro%')  then 1          else 0 end) as buy_sub_5_Arroz
                
                ,sum (case when (subcategory_name ilike '%Azúca%' or subcategory_name ilike '%Açúca%') then volumen_kg  else 0 end) as vol_sub_6_Azucar_Endulzantes
                ,sum (case when (subcategory_name ilike '%Azúca%' or subcategory_name ilike '%Açúca%') then gmv_item    else 0 end) as gmv_sub_6_Azucar_Endulzantes
                ,sum (case when (subcategory_name ilike '%Azúca%' or subcategory_name ilike '%Açúca%') then 1           else 0 end) as buy_sub_6_Azucar_Endulzantes
                
                ,sum (case when (subcategory_name ilike '%Café%') then volumen_kg else 0 end) as vol_sub_8_Cafe_Chocolate_Infusiones
                ,sum (case when (subcategory_name ilike '%Café%') then gmv_item   else 0 end) as gmv_sub_8_Cafe_Chocolate_Infusiones
                ,sum (case when (subcategory_name ilike '%Café%') then 1          else 0 end) as buy_sub_8_Cafe_Chocolate_Infusiones
                
                ,sum (case when (subcategory_name ilike '%Cerdo%' or subcategory_name ilike '%Suíno%') then volumen_kg else 0 end) as vol_sub_9_Cerdo
                ,sum (case when (subcategory_name ilike '%Cerdo%' or subcategory_name ilike '%Suíno%') then gmv_item   else 0 end) as gmv_sub_9_Cerdo
                ,sum (case when (subcategory_name ilike '%Cerdo%' or subcategory_name ilike '%Suíno%') then 1          else 0 end) as buy_sub_9_Cerdo
                
                ,sum (case when (subcategory_name ilike '%Chiles%') then volumen_kg else 0 end) as vol_sub_11_Chiles
                ,sum (case when (subcategory_name ilike '%Chiles%') then gmv_item   else 0 end) as gmv_sub_11_Chiles
                ,sum (case when (subcategory_name ilike '%Chiles%') then 1          else 0 end) as buy_sub_11_Chiles
                
                ,sum (case when (subcategory_name ilike '%Lácteos%' or subcategory_name ilike '%Cremas%' or subcategory_name ilike '%Lácteos & Huevos%') then volumen_kg else 0 end) as vol_sub_15_Derivados_Lacteos
                ,sum (case when (subcategory_name ilike '%Lácteos%' or subcategory_name ilike '%Cremas%' or subcategory_name ilike '%Lácteos & Huevos%') then gmv_item   else 0 end) as gmv_sub_15_Derivados_Lacteos
                ,sum (case when (subcategory_name ilike '%Lácteos%' or subcategory_name ilike '%Cremas%' or subcategory_name ilike '%Lácteos & Huevos%') then 1          else 0 end) as buy_sub_15_Derivados_Lacteos
               
                ,sum (case when (subcategory_name ilike 'Embutidos') then volumen_kg else 0 end) as vol_sub_20_Embutidos
                ,sum (case when (subcategory_name ilike 'Embutidos') then gmv_item   else 0 end) as gmv_sub_20_Embutidos
                ,sum (case when (subcategory_name ilike 'Embutidos') then 1          else 0 end) as buy_sub_20_Embutidos
                
                ,sum (case when (subcategory_name ilike '%Enlatado%' or subcategory_name ilike '%Fruto%') then volumen_kg else 0 end) as vol_sub_22_Enlatados
                ,sum (case when (subcategory_name ilike '%Enlatado%' or subcategory_name ilike '%Fruto%') then gmv_item   else 0 end) as gmv_sub_22_Enlatados
                ,sum (case when (subcategory_name ilike '%Enlatado%' or subcategory_name ilike '%Fruto%') then 1          else 0 end) as buy_sub_22_Enlatados
                
                ,sum (case when (subcategory_name ilike '%Especi%' or subcategory_name ilike '%Tempero%') then volumen_kg else 0 end) as vol_sub_23_Especias
                ,sum (case when (subcategory_name ilike '%Especi%' or subcategory_name ilike '%Tempero%') then gmv_item   else 0 end) as gmv_sub_23_Especias
                ,sum (case when (subcategory_name ilike '%Especi%' or subcategory_name ilike '%Tempero%') then 1          else 0 end) as buy_sub_23_Especias
                
                ,sum (case when (subcategory_name ilike '%Fruta%' ) then volumen_kg else 0 end) as vol_sub_24_Frutas
                ,sum (case when (subcategory_name ilike '%Fruta%' ) then gmv_item   else 0 end) as gmv_sub_24_Frutas
                ,sum (case when (subcategory_name ilike '%Fruta%' ) then 1          else 0 end) as buy_sub_24_Frutas
                
                ,sum (case when (subcategory_name ilike '%Grano%' or subcategory_name ilike '%Grão%' or subcategory_name ilike '%Feij%') then volumen_kg else 0 end) as vol_sub_26_Granos
                ,sum (case when (subcategory_name ilike '%Grano%' or subcategory_name ilike '%Grão%' or subcategory_name ilike '%Feij%') then gmv_item   else 0 end) as gmv_sub_26_Granos
                ,sum (case when (subcategory_name ilike '%Grano%' or subcategory_name ilike '%Grão%' or subcategory_name ilike '%Feij%') then 1          else 0 end) as buy_sub_26_Granos
                
                ,sum (case when (subcategory_name ilike '%Harina%' or subcategory_name ilike '%Farinha%') then volumen_kg else 0 end) as vol_sub_27_Harinas_Mezclas
                ,sum (case when (subcategory_name ilike '%Harina%' or subcategory_name ilike '%Farinha%') then gmv_item   else 0 end) as gmv_sub_27_Harinas_Mezclas
                ,sum (case when (subcategory_name ilike '%Harina%' or subcategory_name ilike '%Farinha%') then 1          else 0 end) as buy_sub_27_Harinas_Mezclas
                
                ,sum (case when (subcategory_name ilike '%Huevo%' or subcategory_name ilike '%Ovos%') then volumen_kg else 0 end) as vol_sub_30_Huevos
                ,sum (case when (subcategory_name ilike '%Huevo%' or subcategory_name ilike '%Ovos%') then gmv_item   else 0 end) as gmv_sub_30_Huevos
                ,sum (case when (subcategory_name ilike '%Huevo%' or subcategory_name ilike '%Ovos%') then 1          else 0 end) as buy_sub_30_Huevos
                
                ,sum (case when (subcategory_name ilike '%Leche%' or subcategory_name ilike '%Lact%' or subcategory_name ilike '%Leit%') then volumen_kg else 0 end) as vol_sub_32_Leches
                ,sum (case when (subcategory_name ilike '%Leche%' or subcategory_name ilike '%Lact% 'or subcategory_name ilike '%Leit%')  then gmv_item   else 0 end) as gmv_sub_32_Leches
                ,sum (case when (subcategory_name ilike '%Leche%' or subcategory_name ilike '%Lact%' or subcategory_name ilike '%Leit%')  then 1          else 0 end) as buy_sub_32_Leches
                
                ,sum (case when (subcategory_name ilike '%Mante%') then volumen_kg else 0 end) as vol_sub_33_Mantequillas_Margarinas
                ,sum (case when (subcategory_name ilike '%Mante%') then gmv_item   else 0 end) as gmv_sub_33_Mantequillas_Margarinas
                ,sum (case when (subcategory_name ilike '%Mante%') then 1          else 0 end) as buy_sub_33_Mantequillas_Margarinas
                
                ,sum (case when (subcategory_name ilike '%Embu%' or subcategory_name = 'Otras Proteínas Frescas' or subcategory_name = 'Proteínas Frescas' or  (subcategory_name = 'Otros' and (category_name = 'Proteínas Congeladas' or category_name = 'Proteínas Frescas'))) then volumen_kg else 0 end) as vol_sub_34_Otras_Proteinas
                ,sum (case when (subcategory_name ilike '%Embu%' or subcategory_name = 'Otras Proteínas Frescas' or subcategory_name = 'Proteínas Frescas' or  (subcategory_name = 'Otros' and (category_name = 'Proteínas Congeladas' or category_name = 'Proteínas Frescas'))) then volumen_kg else 0 end) as gmv_sub_34_Otras_Proteinas
                ,sum (case when (subcategory_name ilike '%Embu%' or subcategory_name = 'Otras Proteínas Frescas' or subcategory_name = 'Proteínas Frescas' or  (subcategory_name = 'Otros' and (category_name = 'Proteínas Congeladas' or category_name = 'Proteínas Frescas'))) then 1          else 0 end) as buy_sub_34_Otras_Proteinas
                
                ,sum (case when (subcategory_name ilike '%Otros Abarrotes%' or subcategory_name ilike '%Condimento%') then volumen_kg else 0 end) as vol_sub_36_Otros_Abarrotes
                ,sum (case when (subcategory_name ilike '%Otros Abarrotes%' or subcategory_name ilike '%Condimento%') then gmv_item   else 0 end) as gmv_sub_36_Otros_Abarrotes
                ,sum (case when (subcategory_name ilike '%Otros Abarrotes%' or subcategory_name ilike '%Condimento%') then 1          else 0 end) as buy_sub_36_Otros_Abarrotes
                
                ,sum (case when (subcategory_name ilike '%Panader%'or subcategory_name ilike '%Confit%') then volumen_kg else 0 end) as vol_sub_37_Panaderia_Tortilleria
                ,sum (case when (subcategory_name ilike '%Panader%'or subcategory_name ilike '%Confit%') then gmv_item   else 0 end) as gmv_sub_37_Panaderia_Tortilleria
                ,sum (case when (subcategory_name ilike '%Panader%'or subcategory_name ilike '%Confit%') then 1          else 0 end) as buy_sub_37_Panaderia_Tortilleria
                
                ,sum (case when (subcategory_name ilike '%Pasta%' or subcategory_name ilike '%Massa%') then volumen_kg else 0 end) as vol_sub_39_Pasta
                ,sum (case when (subcategory_name ilike '%Pasta%' or subcategory_name ilike '%Massa%') then gmv_item   else 0 end) as gmv_sub_39_Pasta
                ,sum (case when (subcategory_name ilike '%Pasta%' or subcategory_name ilike '%Massa%') then 1          else 0 end) as buy_sub_39_Pasta
                
                ,sum (case when (subcategory_name ilike '%Pescado%' or subcategory_name = 'Peix') then volumen_kg else 0 end) as vol_sub_40_Pescados_Mariscos_Congelado
                ,sum (case when (subcategory_name ilike '%Pescado%' or subcategory_name = 'Peix') then gmv_item   else 0 end) as gmv_sub_40_Pescados_Mariscos_Congelado
                ,sum (case when (subcategory_name ilike '%Pescado%' or subcategory_name = 'Peix') then 1          else 0 end) as buy_sub_40_Pescados_Mariscos_Congelado
                
                ,sum (case when (subcategory_name ilike '%Pollo%' or subcategory_name ilike '%Frango%') then volumen_kg else 0 end) as vol_sub_41_Pollo
                ,sum (case when (subcategory_name ilike '%Pollo%' or subcategory_name ilike '%Frango%') then gmv_item   else 0 end) as gmv_sub_41_Pollo
                ,sum (case when (subcategory_name ilike '%Pollo%' or subcategory_name ilike '%Frango%') then 1          else 0 end) as buy_sub_41_Pollo
                
                ,sum (case when (subcategory_name ilike '%Que%') then volumen_kg else 0 end) as vol_sub_43_Quesos
                ,sum (case when (subcategory_name ilike '%Que%') then gmv_item   else 0 end) as gmv_sub_43_Quesos
                ,sum (case when (subcategory_name ilike '%Que%') then 1          else 0 end) as buy_sub_43_Quesos
                
                ,sum (case when (subcategory_name ilike 'Rappi') then volumen_kg else 0 end) as vol_sub_44_Rappi
                ,sum (case when (subcategory_name ilike 'Rappi') then gmv_item   else 0 end) as gmv_sub_44_Rappi
                ,sum (case when (subcategory_name ilike 'Rappi') then 1          else 0 end) as buy_sub_44_Rappi
                
                ,sum (case when (subcategory_name ilike '%Res%' or subcategory_name ilike '%Vermel%') then volumen_kg else 0 end) as vol_sub_45_Res
                ,sum (case when (subcategory_name ilike '%Res%' or subcategory_name ilike '%Vermel%') then gmv_item   else 0 end) as gmv_sub_45_Res
                ,sum (case when (subcategory_name ilike '%Res%' or subcategory_name ilike '%Vermel%') then 1          else 0 end) as buy_sub_46_Res
                
                ,sum (case when (subcategory_name ilike '%Sal%') then volumen_kg else 0 end) as vol_sub_47_Sal_Sazonadores
                ,sum (case when (subcategory_name ilike '%Sal%') then gmv_item   else 0 end) as gmv_sub_47_Sal_Sazonadores
                ,sum (case when (subcategory_name ilike '%Sal%') then 1          else 0 end) as buy_sub_47_Sal_Sazonadores
                
                ,sum (case when (subcategory_name ilike '%Salsas%' or subcategory_name ilike '%Molhos%') then volumen_kg else 0 end) as vol_sub_48_Salsas
                ,sum (case when (subcategory_name ilike '%Salsas%' or subcategory_name ilike '%Molhos%') then gmv_item   else 0 end) as gmv_sub_48_Salsas
                ,sum (case when (subcategory_name ilike '%Salsas%' or subcategory_name ilike '%Molhos%') then gmv_item   else 0 end) as buy_sub_48_Salsas
                
                ,sum (case when (subcategory_name ilike '%Tubérculos%') then volumen_kg else 0 end) as vol_sub_49_Tuberculos
                ,sum (case when (subcategory_name ilike '%Tubérculos%') then gmv_item   else 0 end) as gmv_sub_49_Tuberculos
                ,sum (case when (subcategory_name ilike '%Tubérculos%') then 1          else 0 end) as buy_sub_49_Tuberculos
                
                ,sum (case when (subcategory_name = 'Verduras' or subcategory_name ilike '%Hortaliças%' or subcategory_name ilike '%Legum%' or subcategory_name ilike '%Cogumelos%' or subcategory_name ilike '%Folhas%' or subcategory_name ilike '%Hierbas%' or subcategory_name ilike '%Hongos%' or subcategory_name ilike '%Raízes%' ) then volumen_kg else 0 end) as vol_sub_51_Verduras
                ,sum (case when (subcategory_name = 'Verduras' or subcategory_name ilike '%Hortaliças%' or subcategory_name ilike '%Legum%' or subcategory_name ilike '%Cogumelos%' or subcategory_name ilike '%Folhas%' or subcategory_name ilike '%Hierbas%' or subcategory_name ilike '%Hongos%' or subcategory_name ilike '%Raízes%') then gmv_item   else 0 end) as gmv_sub_51_Verduras
                ,sum (case when (subcategory_name = 'Verduras' or subcategory_name ilike '%Hortaliças%' or subcategory_name ilike '%Legum%' or subcategory_name ilike '%Cogumelos%' or subcategory_name ilike '%Folhas%' or subcategory_name ilike '%Hierbas%' or subcategory_name ilike '%Hongos%' or subcategory_name ilike '%Raízes%') then 1          else 0 end) as buy_sub_51_Verduras
                
                ,sum (case when (subcategory_name ilike '%Bebida%' or subcategory_name ilike '%Refres%' or subcategory_name ilike '%Polpa%' or subcategory_name ilike '%Pulpa%' or subcategory_name ilike '%Cerve%') then volumen_kg else 0 end) as vol_sub_51_Bebidas
                ,sum (case when (subcategory_name ilike '%Bebida%' or subcategory_name ilike '%Refres%' or subcategory_name ilike '%Polpa%' or subcategory_name ilike '%Pulpa%' or subcategory_name ilike '%Cerve%') then gmv_item   else 0 end) as gmv_sub_51_Bebidas
                ,sum (case when (subcategory_name ilike '%Bebida%' or subcategory_name ilike '%Refres%' or subcategory_name ilike '%Polpa%' or subcategory_name ilike '%Pulpa%' or subcategory_name ilike '%Cerve%') then 1          else 0 end) as buy_sub_51_Bebidas
                
          from  Ordenesxitem  OI 

group by order_boleto,dia_boleto,customer_id)

select agrupacion_orden.*
,agrupacion_skus.*

from (select distinct  IC.customer_id,order_boleto,dia_boleto
,sum(gmv) as gmv_total
,count(distinct order_id) as numero_ordenes
,sum(gmv)/avg(numero_lineas_orden) as gmv_linea_real

from info_cliente IC
group by order_boleto,dia_boleto,customer_id
) as agrupacion_orden

inner join agrupacion_skus  on (agrupacion_skus.order_boleto=agrupacion_orden.order_boleto)

    """
    
    dataframe = read_connection_data_warehouse.runQuery(query)
    
    return dataframe


###################################################################################################################################################################################
###################################################################################################################################################################################
###################################################################################################################################################################################
###################################################################################################################################################################################

def data_indep_time_previous_week_boleto(time):
    
    time_2=time
    print(time_2)
    query ="""
With orden_boleto as(select   distinct bc.customer_id 
                                ,bo.order_id    as order_boleto 
                                ,bo.order_number
                                ,fbs.name as segmento  
                                ,s.site_identifier_value
                                ,Extract(Week  from bo.date_created) as bweek
                                ,Extract(year  from bo.date_created) as byear
                                ,bo.date_created::DATE as dia_boleto

                                FROM postgres_broadleaf_federate."broadleaf.blc_order"  bo 
                                inner join lnd_proc_dev.frucap_boletos                                   fru  on fru.order_code=bo.order_number
                                inner join postgres_broadleaf_federate."broadleaf.blc_fulfillment_group" bfg  ON bfg.order_id = bo.order_id
                                inner join postgres_broadleaf_federate."broadleaf.blc_fulfillment_order" bfo  ON bfo.fulfillment_group_id = bfg.fulfillment_group_id
                                inner join postgres_broadleaf_federate."broadleaf.fb_fulfillment_group"  ffg  ON ffg.fulfillment_group_id = bfg.fulfillment_group_id
                                inner join postgres_broadleaf_federate."broadleaf.fb_order"              fo   ON fo.order_id = bo.order_id
                                inner join postgres_broadleaf_federate."broadleaf.blc_order_payment"     bop  ON bop.order_id = bo.order_id
                                inner join postgres_broadleaf_federate."broadleaf.blc_site"              s    ON s.site_id = bo.site_disc
                                inner join postgres_broadleaf_federate."broadleaf.blc_customer"          bc   ON bc.customer_id = bo.customer_id
                                left join  postgres_broadleaf_federate."broadleaf.fb_order_type"         fot  ON fot.fb_order_type_id=fo.fb_order_type_id
                                left join  postgres_broadleaf_federate."broadleaf.fb_customer"           fc   ON fc.customer_id = bo.customer_id
                                left join  postgres_broadleaf_federate."broadleaf.fb_business_type"      fbs  ON fc.business_type_id = fbs.business_type_id 

                               -- WHERE fo.fb_order_status_id IN (1,6,7,8)
                                 --   AND bo.order_status = 'SUBMITTED'
                                 --   AND bfo.status NOT IN ('ARCHIVED','CANCELLED')
                                 --   AND bop.archived = 'N'
                                 --   AND (fot.name IS NULL OR fot.name <> 'REFUND')
                                    )

 ,ids as (select distinct customer_id from orden_boleto)


SELECT uid
    ,order_boleto
    ,count(distinct order_id) as num_orders_last6w
    ,sum(case when order_coincides_otp_request=0 and order_through_csr=0 then 1 else 0 end) as num_indep_orders_last6w
    ,(sum(case when order_coincides_otp_request=0 and order_through_csr=0 then 1 else 0 end)*1.0)/(count(distinct order_id)*1.0) as pct_indep_orders_6w


    FROM
            (SELECT cast(a.uid as bigint) as uid
            ,a.order_id
            ,a.dia_boleto
            ,a.order_boleto
            ,a.order_number
            ,a.submit_date
            ,a.visita_farmer_completedon
            ,a.order_coincides_with_farmer_visit
            ,a.date_otp_requested
            ,a.order_coincides_otp_request
            ,b.log_date_created
            ,b.order_through_csr
            ,case when order_coincides_with_farmer_visit=0 and order_coincides_otp_request=0 and b.order_through_csr=0 then 1 else 0 end as fully_independent_order
            FROM
                (SELECT distinct orders.uid
                        ,orders.order_id
                        ,orders.order_number
                        ,orders.submit_date
                        ,orders.dia_boleto
                        ,orders.order_boleto
                        ,visitas_farmers.completed_on as visita_farmer_completedon
                        ,case when visitas_farmers.uid is null then 0 else 1 end as order_coincides_with_farmer_visit
                        ,otp_requests.date_requested as date_otp_requested
                        ,case when otp_requests.uid is null then 0 else 1 end as order_coincides_otp_request

                        FROM (SELECT distinct bo.customer_id::bigint as uid
                                    ,bo.order_id
                                    ,bo.order_number
                                    ,DATE(bo.submit_date  AT TIME ZONE 'UTC' at time zone 'america/bogota') as submit_date
                                    ,bc.email_address as email
                                    ,ffg.close_date
                                    ,dia_boleto
                                    ,order_boleto
                                FROM postgres_broadleaf_federate."broadleaf.blc_order" bo
                                INNER JOIN postgres_broadleaf_federate."broadleaf.blc_fulfillment_group" bfg ON bfg.order_id = bo.order_id
                                INNER JOIN postgres_broadleaf_federate."broadleaf.blc_fulfillment_order" bfo ON bfo.fulfillment_group_id = bfg.fulfillment_group_id
                                INNER JOIN postgres_broadleaf_federate."broadleaf.fb_fulfillment_group"  ffg ON ffg.fulfillment_group_id = bfg.fulfillment_group_id
                                INNER JOIN postgres_broadleaf_federate."broadleaf.fb_order"              fo  ON fo.order_id = bo.order_id
                                INNER JOIN postgres_broadleaf_federate."broadleaf.blc_order_payment"     bop ON bop.order_id = bo.order_id
                                INNER JOIN postgres_broadleaf_federate."broadleaf.blc_site"              s   ON s.site_id = bo.site_disc
                                INNER JOIN postgres_broadleaf_federate."broadleaf.blc_customer"          bc  ON bc.customer_id = bo.customer_id
                                LEFT  JOIN postgres_broadleaf_federate."broadleaf.fb_order_type"         fot ON fot.fb_order_type_id=fo.fb_order_type_id
                                Inner join ids                                                               on ids.customer_id=bo.customer_id
                                Inner join orden_boleto                                                      on orden_boleto.customer_id=bo.customer_id

                                WHERE fo.fb_order_status_id IN (1,6,7,8) 
                                  AND bo.order_status = 'SUBMITTED' 
                                  AND bfo.status NOT IN ('ARCHIVED','CANCELLED') 
                                  AND bop.archived = 'N' 
                                  AND (fot.name IS NULL OR fot.name <> 'REFUND')
                                GROUP BY 1,2,3,4,5,6,7,8) as orders

                LEFT JOIN --encuestas farmer
                (SELECT  base.mx_id_frida::int  as uid,
                    date(to_timestamp(task.completedon,'YYYY-MM-DD HH24:MI:SS') at time zone 'america/bogota') as completed_on

                        FROM leadsquared.usertask_base task
                        INNER JOIN leadsquared.prospect_base base on task.relatedentityid=base.prospectid
                        WHERE (task.name like 'Encuesta%' OR task.name like 'Enquete%' OR task.name like 'Reforzar%') AND
                        completed_on>='2021-03-10' GROUP BY 1,2) as visitas_farmers
                        ON orders.uid=visitas_farmers.uid and orders.submit_date=visitas_farmers.completed_on

                LEFT JOIN --- 
                (SELECT a.profile_identity::varchar as uid
                        ,b.email::varchar as email
                        ,a.session_date as date_requested 
                        FROM
                        (SELECT profile_identity
                        ,date(event_date) as session_date
                        ,sum(case when (eventname='FRB Signin/up OTP Submit' AND frsubmitresult='Successful') then 1 else 0 end) as num_successful_otp_access
                        ,sum(case when eventname='FRF Logout Submit' then 1 else 0 end) as num_daily_logouts 
                        FROM dpr_clevertap.events WHERE date(event_date)>='2020-11-01' 
                        GROUP BY 1,2 
                        HAVING num_successful_otp_access>0 AND num_daily_logouts>0) as a

                        LEFT JOIN (SELECT profile_identity
                                    , email 
                                    FROM dpr_clevertap.profile) AS b ON a.profile_identity=b.profile_identity) as otp_requests
                        ON (orders.uid=otp_requests.uid or orders.email=otp_requests.email) AND orders.submit_date=otp_requests.date_requested
                        WHERE orders.submit_date>='2020-11-01') as a 

                LEFT JOIN
                (SELECT orders.uid
                ,orders.order_id
                ,orders.order_number
                ,orders.submit_date /*orders.close_date,*/
                ,csr_orders.log_date_created
                ,case when csr_orders.order_id_csr is null then 0 else 1 end as order_through_csr
                FROM (SELECT distinct bo.customer_id::bigint as uid
                            ,bo.order_id,
                            substring(bo.order_number,0,case when charindex('-',RIGHT(bo.order_number,LEN(bo.order_number)-1))=0 then LEN(bo.order_number)+1
                                                             else charindex('-',RIGHT(bo.order_number,LEN(bo.order_number)-1))+1 END) as order_number,
                            bo.submit_date AT TIME ZONE 'UTC' at time zone 'america/bogota' as submit_date,/*bc.email_address,ffg.close_date,*/
                            s.site_identifier_value as order_city
                        FROM postgres_broadleaf_federate."broadleaf.blc_order" bo
                        INNER JOIN postgres_broadleaf_federate."broadleaf.blc_fulfillment_group" bfg ON bfg.order_id = bo.order_id
                        INNER JOIN postgres_broadleaf_federate."broadleaf.blc_fulfillment_order" bfo ON bfo.fulfillment_group_id = bfg.fulfillment_group_id
                        INNER JOIN postgres_broadleaf_federate."broadleaf.fb_fulfillment_group"  ffg ON ffg.fulfillment_group_id = bfg.fulfillment_group_id
                        INNER JOIN postgres_broadleaf_federate."broadleaf.fb_order"               fo ON fo.order_id = bo.order_id
                        INNER JOIN postgres_broadleaf_federate."broadleaf.blc_order_payment"     bop ON bop.order_id = bo.order_id
                        INNER JOIN postgres_broadleaf_federate."broadleaf.blc_site"                s ON s.site_id = bo.site_disc
                        INNER JOIN postgres_broadleaf_federate."broadleaf.blc_customer"           bc ON bc.customer_id = bo.customer_id
                        LEFT JOIN  postgres_broadleaf_federate."broadleaf.fb_order_type"         fot ON fot.fb_order_type_id=fo.fb_order_type_id
                        Inner join ids                                                               on ids.customer_id=bo.customer_id
                        --WHERE fo.fb_order_status_id IN (1,6,7,8)
                        where bo.order_status = 'SUBMITTED') as orders 

                        LEFT JOIN
                                (select log.order_id as order_id_csr
                                        ,log.referred_by as log_referred_by
                                        ,log.type as log_type
                                        ,log.date_created AT TIME ZONE 'UTC' at time zone 'america/bogota' as log_date_created
                                        ,log.admin_user_id
                                        ,substring(bo.order_number,0,case when charindex('-',RIGHT(bo.order_number,LEN(bo.order_number)-1))=0 then LEN(bo.order_number)+1
                                                                         else charindex('-',RIGHT(bo.order_number,LEN(bo.order_number)-1))+1 END) as order_number

                                FROM      postgres_broadleaf_federate."broadleaf.fb_csr_order_log_event" log
                                LEFT JOIN postgres_broadleaf_federate."broadleaf.blc_order" bo on bo.order_id=log.order_id
                                   Inner join ids                                                               on ids.customer_id=bo.customer_id
                                where log.type in ('ORDER_SUBMIT') 
                                and log_date_created>='2020-11-01') as csr_orders on orders.order_number=csr_orders.order_number

                        WHERE orders.submit_date>='2020-11-01') as b on a.uid=b.uid and a.order_number=b.order_number) 

                        WHERE submit_date>=DATE(DATE_TRUNC('WEEK', dia_boleto - INTERVAL '{time} WEEK'))
                        and submit_date<DATE(DATE_TRUNC('WEEK', dia_boleto + INTERVAL '1 WEEK')) 
                        GROUP BY 1,2
    
            """.format(time=time)    
    
    dataframe = read_connection_data_warehouse.runQuery(query)
    return dataframe

###################################################################################################################################################################################
###################################################################################################################################################################################
###################################################################################################################################################################################
###################################################################################################################################################################################

def data_indep_boleto():
    
    query ="""
With orden_boleto as(select   distinct bc.customer_id 
                                ,bo.order_id    as order_boleto 
                                ,bo.order_number
                                ,fbs.name as segmento  
                                ,s.site_identifier_value
                                ,Extract(Week  from bo.date_created) as bweek
                                ,Extract(year  from bo.date_created) as byear
                                ,bo.date_created::DATE as dia_boleto

                                FROM postgres_broadleaf_federate."broadleaf.blc_order"  bo 
                                inner join lnd_proc_dev.frucap_boletos                                   fru  on fru.order_code=bo.order_number
                                inner join postgres_broadleaf_federate."broadleaf.blc_fulfillment_group" bfg  ON bfg.order_id = bo.order_id
                                inner join postgres_broadleaf_federate."broadleaf.blc_fulfillment_order" bfo  ON bfo.fulfillment_group_id = bfg.fulfillment_group_id
                                inner join postgres_broadleaf_federate."broadleaf.fb_fulfillment_group"  ffg  ON ffg.fulfillment_group_id = bfg.fulfillment_group_id
                                inner join postgres_broadleaf_federate."broadleaf.fb_order"              fo   ON fo.order_id = bo.order_id
                                inner join postgres_broadleaf_federate."broadleaf.blc_order_payment"     bop  ON bop.order_id = bo.order_id
                                inner join postgres_broadleaf_federate."broadleaf.blc_site"              s    ON s.site_id = bo.site_disc
                                inner join postgres_broadleaf_federate."broadleaf.blc_customer"          bc   ON bc.customer_id = bo.customer_id
                                left join  postgres_broadleaf_federate."broadleaf.fb_order_type"         fot  ON fot.fb_order_type_id=fo.fb_order_type_id
                                left join  postgres_broadleaf_federate."broadleaf.fb_customer"           fc   ON fc.customer_id = bo.customer_id
                                left join  postgres_broadleaf_federate."broadleaf.fb_business_type"      fbs  ON fc.business_type_id = fbs.business_type_id 

                               -- WHERE fo.fb_order_status_id IN (1,6,7,8)
                                 --   AND bo.order_status = 'SUBMITTED'
                                 --   AND bfo.status NOT IN ('ARCHIVED','CANCELLED')
                                 --   AND bop.archived = 'N'
                                 --   AND (fot.name IS NULL OR fot.name <> 'REFUND')
                                    )

 ,ids as (select distinct customer_id from orden_boleto)


SELECT cast(a.uid as bigint) as uid
            ,a.order_id
            ,a.dia_boleto
            ,a.order_boleto
            ,a.order_number
            ,a.submit_date
            ,a.date_otp_requested
            ,a.order_coincides_otp_request
            ,b.log_date_created
            ,b.order_through_csr
            ,case when order_coincides_otp_request=0 and b.order_through_csr=0 then 1 else 0 end as fully_independent_order
            FROM
                (SELECT distinct orders.uid
                        ,orders.order_id
                        ,orders.order_number
                        ,orders.submit_date
                        ,orders.dia_boleto
                        ,orders.order_boleto
                        ,otp_requests.date_requested as date_otp_requested
                        ,case when otp_requests.uid is null then 0 else 1 end as order_coincides_otp_request

                        FROM (SELECT distinct bo.customer_id::bigint as uid
                                    ,bo.order_id
                                    ,bo.order_number
                                    ,DATE(bo.submit_date  AT TIME ZONE 'UTC' at time zone 'america/bogota') as submit_date
                                    ,bc.email_address as email
                                    ,ffg.close_date
                                    ,dia_boleto
                                    ,order_boleto
                                FROM postgres_broadleaf_federate."broadleaf.blc_order" bo
                                INNER JOIN postgres_broadleaf_federate."broadleaf.blc_fulfillment_group" bfg ON bfg.order_id = bo.order_id
                                INNER JOIN postgres_broadleaf_federate."broadleaf.blc_fulfillment_order" bfo ON bfo.fulfillment_group_id = bfg.fulfillment_group_id
                                INNER JOIN postgres_broadleaf_federate."broadleaf.fb_fulfillment_group"  ffg ON ffg.fulfillment_group_id = bfg.fulfillment_group_id
                                INNER JOIN postgres_broadleaf_federate."broadleaf.fb_order"              fo  ON fo.order_id = bo.order_id
                                INNER JOIN postgres_broadleaf_federate."broadleaf.blc_order_payment"     bop ON bop.order_id = bo.order_id
                                INNER JOIN postgres_broadleaf_federate."broadleaf.blc_site"              s   ON s.site_id = bo.site_disc
                                INNER JOIN postgres_broadleaf_federate."broadleaf.blc_customer"          bc  ON bc.customer_id = bo.customer_id
                                LEFT  JOIN postgres_broadleaf_federate."broadleaf.fb_order_type"         fot ON fot.fb_order_type_id=fo.fb_order_type_id
                                Inner join ids                                                               on ids.customer_id=bo.customer_id
                                Inner join orden_boleto                                                      on orden_boleto.order_boleto=bo.order_id

                              --  WHERE fo.fb_order_status_id IN (1,6,7,8) 
                               --   AND bo.order_status = 'SUBMITTED' 
                               --   AND bfo.status NOT IN ('ARCHIVED','CANCELLED') 
                               --   AND bop.archived = 'N' 
                               --   AND (fot.name IS NULL OR fot.name <> 'REFUND')
                                GROUP BY 1,2,3,4,5,6,7,8) as orders

                LEFT JOIN --encuestas farmer
                (SELECT  base.mx_id_frida::int  as uid,
                    date(to_timestamp(task.completedon,'YYYY-MM-DD HH24:MI:SS') at time zone 'america/bogota') as completed_on

                        FROM leadsquared.usertask_base task
                        INNER JOIN leadsquared.prospect_base base on task.relatedentityid=base.prospectid
                        WHERE (task.name like 'Encuesta%' OR task.name like 'Enquete%' OR task.name like 'Reforzar%') AND
                        completed_on>='2021-03-10' GROUP BY 1,2) as visitas_farmers
                        ON orders.uid=visitas_farmers.uid and orders.submit_date=visitas_farmers.completed_on

                LEFT JOIN --- 
                (SELECT a.profile_identity::varchar as uid
                        ,b.email::varchar as email
                        ,a.session_date as date_requested 
                        FROM
                        (SELECT profile_identity
                        ,date(event_date) as session_date
                        ,sum(case when (eventname='FRB Signin/up OTP Submit' AND frsubmitresult='Successful') then 1 else 0 end) as num_successful_otp_access
                        ,sum(case when eventname='FRF Logout Submit' then 1 else 0 end) as num_daily_logouts 
                        FROM dpr_clevertap.events WHERE date(event_date)>='2020-11-01' 
                        GROUP BY 1,2 
                        HAVING num_successful_otp_access>0 AND num_daily_logouts>0) as a

                        LEFT JOIN (SELECT profile_identity
                                    , email 
                                    FROM dpr_clevertap.profile) AS b ON a.profile_identity=b.profile_identity) as otp_requests
                        ON (orders.uid=otp_requests.uid or orders.email=otp_requests.email) AND orders.submit_date=otp_requests.date_requested
                        WHERE orders.submit_date>='2020-11-01') as a 

                LEFT JOIN
                (SELECT orders.uid
                ,orders.order_id
                ,orders.order_number
                ,orders.submit_date /*orders.close_date,*/
                ,csr_orders.log_date_created
                ,case when csr_orders.order_id_csr is null then 0 else 1 end as order_through_csr
                FROM (SELECT distinct bo.customer_id::bigint as uid
                            ,bo.order_id,
                            substring(bo.order_number,0,case when charindex('-',RIGHT(bo.order_number,LEN(bo.order_number)-1))=0 then LEN(bo.order_number)+1
                                                             else charindex('-',RIGHT(bo.order_number,LEN(bo.order_number)-1))+1 END) as order_number,
                            bo.submit_date AT TIME ZONE 'UTC' at time zone 'america/bogota' as submit_date,/*bc.email_address,ffg.close_date,*/
                            s.site_identifier_value as order_city
                        FROM postgres_broadleaf_federate."broadleaf.blc_order" bo
                        INNER JOIN postgres_broadleaf_federate."broadleaf.blc_fulfillment_group" bfg ON bfg.order_id = bo.order_id
                        INNER JOIN postgres_broadleaf_federate."broadleaf.blc_fulfillment_order" bfo ON bfo.fulfillment_group_id = bfg.fulfillment_group_id
                        INNER JOIN postgres_broadleaf_federate."broadleaf.fb_fulfillment_group"  ffg ON ffg.fulfillment_group_id = bfg.fulfillment_group_id
                        INNER JOIN postgres_broadleaf_federate."broadleaf.fb_order"               fo ON fo.order_id = bo.order_id
                        INNER JOIN postgres_broadleaf_federate."broadleaf.blc_order_payment"     bop ON bop.order_id = bo.order_id
                        INNER JOIN postgres_broadleaf_federate."broadleaf.blc_site"                s ON s.site_id = bo.site_disc
                        INNER JOIN postgres_broadleaf_federate."broadleaf.blc_customer"           bc ON bc.customer_id = bo.customer_id
                        LEFT JOIN  postgres_broadleaf_federate."broadleaf.fb_order_type"         fot ON fot.fb_order_type_id=fo.fb_order_type_id
                        Inner join ids                                                               on ids.customer_id=bo.customer_id
                        Inner join orden_boleto                                                      on orden_boleto.order_boleto=bo.order_id
                        --WHERE fo.fb_order_status_id IN (1,6,7,8)
                        --where bo.order_status = 'SUBMITTED'
                        ) as orders 

                        LEFT JOIN
                                (select log.order_id as order_id_csr
                                        ,log.referred_by as log_referred_by
                                        ,log.type as log_type
                                        ,log.date_created AT TIME ZONE 'UTC' at time zone 'america/bogota' as log_date_created
                                        ,log.admin_user_id
                                        ,substring(bo.order_number,0,case when charindex('-',RIGHT(bo.order_number,LEN(bo.order_number)-1))=0 then LEN(bo.order_number)+1
                                                                         else charindex('-',RIGHT(bo.order_number,LEN(bo.order_number)-1))+1 END) as order_number

                                FROM      postgres_broadleaf_federate."broadleaf.fb_csr_order_log_event" log
                                LEFT JOIN postgres_broadleaf_federate."broadleaf.blc_order" bo on bo.order_id=log.order_id
                                   Inner join ids                                                               on ids.customer_id=bo.customer_id
                                where log.type in ('ORDER_SUBMIT') 
                                and log_date_created>='2020-11-01') as csr_orders on orders.order_number=csr_orders.order_number

                        WHERE orders.submit_date>='2020-11-01') as b on a.uid=b.uid and a.order_number=b.order_number  
            """ 
    
    dataframe = read_connection_data_warehouse.runQuery(query)
    return dataframe


###################################################################################################################################################################################
###################################################################################################################################################################################
###################################################################################################################################################################################
###################################################################################################################################################################################


def data_trans_comp_rachas_week_previous_query_vboletos(TIME):
    """
    Owner: Gabo Moreno
    Está función trae la data de ventas, para segmentos de restaurantes, para una ciudad en un rango de fecha determinado.
    
    Argumentos:
    Time: número de días que se requiere agrupar
 
    Resultados:
    dataframe: dataframe con la data de ventas solicitada
    """
    
    query = """
    With orden_boleto as(select   distinct bc.customer_id 
                                ,bo.order_id    as order_boleto 
                                ,bo.order_number
                                ,fbs.name as segmento  
                                ,s.site_identifier_value
                                ,Extract(Week  from bo.date_created) as bweek
                                ,Extract(year  from bo.date_created) as byear
                                ,bo.date_created::DATE as dia_boleto
                                        
                                FROM postgres_broadleaf_federate."broadleaf.blc_order"  bo 
                                inner join lnd_proc_dev.frucap_boletos                                   fru  on fru.order_code=bo.order_number
                                inner join postgres_broadleaf_federate."broadleaf.blc_fulfillment_group" bfg  ON bfg.order_id = bo.order_id
                                inner join postgres_broadleaf_federate."broadleaf.blc_fulfillment_order" bfo  ON bfo.fulfillment_group_id = bfg.fulfillment_group_id
                                inner join postgres_broadleaf_federate."broadleaf.fb_fulfillment_group"  ffg  ON ffg.fulfillment_group_id = bfg.fulfillment_group_id
                                inner join postgres_broadleaf_federate."broadleaf.fb_order"              fo   ON fo.order_id = bo.order_id
                                inner join postgres_broadleaf_federate."broadleaf.blc_order_payment"     bop  ON bop.order_id = bo.order_id
                                inner join postgres_broadleaf_federate."broadleaf.blc_site"              s    ON s.site_id = bo.site_disc
                                inner join postgres_broadleaf_federate."broadleaf.blc_customer"          bc   ON bc.customer_id = bo.customer_id
                                left join  postgres_broadleaf_federate."broadleaf.fb_order_type"         fot  ON fot.fb_order_type_id=fo.fb_order_type_id
                                left join  postgres_broadleaf_federate."broadleaf.fb_customer"           fc   ON fc.customer_id = bo.customer_id
                                left join  postgres_broadleaf_federate."broadleaf.fb_business_type"      fbs  ON fc.business_type_id = fbs.business_type_id 
                                
                               -- WHERE fo.fb_order_status_id IN (1,6,7,8)
                                 --   AND bo.order_status = 'SUBMITTED'
                                 --   AND bfo.status NOT IN ('ARCHIVED','CANCELLED')
                                 --   AND bop.archived = 'N'
                                 --   AND (fot.name IS NULL OR fot.name <> 'REFUND')
                                 
                                    )
 -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 ,ids as (select distinct customer_id from orden_boleto)
 

,conversion as (select distinct bsku.sku_id
                ,coalesce(conversion_value_y, 1) conversion_value_y
                ,coalesce(conversion_value_x, 1) conversion_value_x
                ,coalesce(conversion_type_y, fp.unit) conversion_type_y
                ,coalesce(conversion_type_x, fp.unit) conversion_type_x
                ,fp.unit
                ,bcat.name          as subcategory_name
                ,bcat.category_id   as sub_category_id
                ,bcat2.category_id  as category_id_main
                ,bcat2.name         as category_name
                from  postgres_broadleaf_federate."broadleaf.blc_sku"                 bsku 
                inner join postgres_broadleaf_federate."broadleaf.blc_sku"            s3    ON bsku.addl_product_id = s3.default_product_id
	            inner join postgres_broadleaf_federate."broadleaf.fb_sku"             s4    ON bsku.sku_id = s4.sku_id
                left join  postgres_broadleaf_federate."broadleaf.blc_product"        bp    ON bsku.addl_product_id = bp.product_id
                left join  postgres_broadleaf_federate."broadleaf.blc_category_xref"  bcx   ON bcx.sub_category_id = bp.default_category_id and bcx.archived='N' and bcx.sndbx_tier is null --Relaciones categorias 
                left join  postgres_broadleaf_federate."broadleaf.blc_category"       bcat  ON bcat.category_id = bp.default_category_id --Nombre subcategoria
                left join  postgres_broadleaf_federate."broadleaf.blc_category"       bcat2 ON bcx.category_id = bcat2.category_id 
                inner join postgres_broadleaf_federate."broadleaf.fb_product"         fp    ON bp.product_id = fp.product_id
                )

,info_cliente as (select distinct order_boleto
                ,dia_boleto
                ,bo.customer_id 
                ,bo.order_id
                ,bo.date_created as date_order
                ,Extract(Week  from bo.date_created) as week
                ,Extract(year  from bo.date_created) as year
                ,s.site_identifier_value
                
                ,CASE   WHEN s.site_identifier_value in ('CMX','GDL','MTY','PBC') then (bo.order_subtotal+coalesce(fo.total_tax_iva,0)+coalesce(bo.total_shipping,0))/19.19
                        WHEN s.site_identifier_value in ('SPO','BHZ','CWB') then (bo.order_subtotal+coalesce(fo.total_tax_iva,0)+coalesce(bo.total_shipping,0))/3.88
                        ELSE (bo.order_subtotal+coalesce(fo.total_tax_iva,0)+coalesce(bo.total_shipping,0))/3000 end as gmv

                ,count(distinct boi.order_item_id)    as numero_lineas_orden 


                from postgres_broadleaf_federate."broadleaf.blc_fulfillment_group_item" bfgi
                INNER JOIN postgres_broadleaf_federate."broadleaf.blc_fulfillment_group" bfg  ON bfgi.fulfillment_group_id = bfg.fulfillment_group_id
                INNER JOIN postgres_broadleaf_federate."broadleaf.fb_fulfillment_group"  ffg  ON ffg.fulfillment_group_id = bfg.fulfillment_group_id
                INNER JOIN postgres_broadleaf_federate."broadleaf.blc_fulfillment_order" bfo  ON bfo.fulfillment_group_id= bfg.fulfillment_group_id
                INNER JOIN postgres_broadleaf_federate."broadleaf.blc_order"             bo   ON bo.order_id = bfg.order_id
                inner  join ids                                                          ids  on ids.customer_id=bo.customer_id
                left   join orden_boleto                                                 fru  on fru.customer_id=bo.customer_id
                INNER JOIN postgres_broadleaf_federate."broadleaf.blc_order_item"        boi  ON bfgi.order_item_id= boi.order_item_id
                inner join postgres_broadleaf_federate."broadleaf.fb_order"              fo   ON fo.order_id = bo.order_id
                inner join postgres_broadleaf_federate."broadleaf.blc_order_payment"     bop  ON bop.order_id = bo.order_id
                inner join postgres_broadleaf_federate."broadleaf.blc_site"              s    ON s.site_id = bo.site_disc
                inner join postgres_broadleaf_federate."broadleaf.blc_customer"          bc   ON bc.customer_id = bo.customer_id
                left join  postgres_broadleaf_federate."broadleaf.fb_order_type"         fot  ON fot.fb_order_type_id=fo.fb_order_type_id

                where  bo.date_created < dia_boleto + interval '1 days'
                  and  bo.date_created >= (dia_boleto - interval '{TIME} weeks') 
                  and  fo.fb_order_status_id IN (1,6,7,8)
                  AND  bo.order_status = 'SUBMITTED'
                  AND  bfo.status NOT IN ('ARCHIVED','CANCELLED')
                  AND  bop.archived = 'N'
                  AND  (fot.name IS NULL OR fot.name <> 'REFUND')
                  group by 1,2,3,4,5,6,7,8,9
                ) 

, ordenes as(select distinct order_boleto
                    ,dia_boleto
                    ,order_id
                    ,date_order
                    ,week
                    ,year
                    from info_cliente)

,Ordenesxitem as (select distinct boi.order_item_id
                        ,order_boleto
                        ,dia_boleto
                        ,bo.order_id
                        ,date_order
                        ,bo.customer_id 
                        ,week
                        ,year
                        ,case when s.site_identifier_value in ('CMX','GDL','MTY','PBC')
                                   then nvl((baid.adjustment_value * boip.quantity * foi.step_unit)/19.19, 0)
                                 when  s.site_identifier_value in ('SPO','BHZ','CWB')
                                       then nvl((baid.adjustment_value * boip.quantity * foi.step_unit)/3.88, 0)
                                 else nvl((baid.adjustment_value * boip.quantity * foi.step_unit)/3000, 0)
                                 end as discount_applied

                        ,case when s.site_identifier_value in ('CMX','GDL','MTY','PBC')
                                   then ((boi.quantity * foi.step_unit * boi.sale_price)/19.19)
                             when  s.site_identifier_value in ('SPO','BHZ','CWB')
                                   then ((boi.quantity * foi.step_unit * boi.sale_price)/3.88)
                             else ((boi.quantity * foi.step_unit * boi.sale_price)/3000) 
                             end as gmv_item

                        ,case when s.site_identifier_value in ('CMX','GDL','MTY','PBC')
                                 then  nvl((foi.total_price-foi.total_price_adj)/19.19, 0)
                                 when  s.site_identifier_value in ('SPO','BHZ','CWB')
                                 then  nvl((foi.total_price-foi.total_price_adj)/3.88, 0)
                                 else  nvl((foi.total_price-foi.total_price_adj)/3000, 0)
                                 end   as total_discount_order

                        ,boi.quantity * foi.step_unit as real_quantity
                        ,case when con.unit = 'UNID' then conversion_value_y*(boi.quantity * foi.step_unit) else (boi.quantity * foi.step_unit) end as volumen_kg
                       
                        --,boi.name
                        --,bdoi.sku_id
                        --,bs.addl_product_id as padre_sku_id,

                        ,con.subcategory_name
                        ,con.sub_category_id
                        ,con.category_name
                        ,con.category_id_main

                         from postgres_broadleaf_federate."broadleaf.blc_fulfillment_group_item" bfgi
                            INNER JOIN postgres_broadleaf_federate."broadleaf.blc_fulfillment_group" bfg  ON bfgi.fulfillment_group_id = bfg.fulfillment_group_id
                            INNER JOIN postgres_broadleaf_federate."broadleaf.fb_fulfillment_group"  ffg  ON ffg.fulfillment_group_id = bfg.fulfillment_group_id
                            INNER JOIN postgres_broadleaf_federate."broadleaf.blc_fulfillment_order" bfo  ON bfo.fulfillment_group_id= bfg.fulfillment_group_id
                            INNER JOIN postgres_broadleaf_federate."broadleaf.blc_order"             bo   ON bo.order_id = bfg.order_id
                            inner join ids                                                           ids  on ids.customer_id=bo.customer_id
                            inner join ordenes                                                            on ordenes.order_id=bo.order_id
                            INNER JOIN postgres_broadleaf_federate."broadleaf.blc_order_item"        boi  ON bfgi.order_item_id= boi.order_item_id
                            INNER JOIN postgres_broadleaf_federate."broadleaf.blc_site"              s    ON bo.site_disc = s.site_id
                            INNER JOIN postgres_broadleaf_federate."broadleaf.blc_discrete_order_item"  bdoi       ON bdoi.order_item_id = boi.order_item_id
                            INNER JOIN postgres_broadleaf_federate."broadleaf.fb_order_item"         foi  ON boi.order_item_id= foi.order_item_id
                            left join  conversion                                                    con  ON con.sku_id=bdoi.sku_id
                            inner join postgres_broadleaf_federate."broadleaf.fb_order"              fo   ON fo.order_id = bo.order_id
                            inner join postgres_broadleaf_federate."broadleaf.blc_order_payment"     bop  ON bop.order_id = bo.order_id
                            left join  postgres_broadleaf_federate."broadleaf.fb_order_type"         fot  ON fot.fb_order_type_id=fo.fb_order_type_id
                            LEFT JOIN postgres_broadleaf_federate."broadleaf.blc_order_item_price_dtl"  boip       ON  boi.order_item_id = boip.order_item_id
                            LEFT JOIN postgres_broadleaf_federate."broadleaf.blc_order_item_dtl_adj"    baid       ON  baid.order_item_price_dtl_id = boip.order_item_price_dtl_id
                           ) 
                       
,agrupacion_skus as (select distinct  OI.customer_id 
                    ,order_boleto
                    ,dia_boleto
                    ,week
                    ,year

                    ,sum(discount_applied) as total_discount
                    ,sum(gmv_item) as gmv_subtotal_item
                    ,sum(volumen_kg) as size_order_kg_noreal
                    
                    ,count(distinct order_item_id)    as numero_lineas_time
                    ,count(distinct sub_category_id)  as numero_subcategorias_time 
                    ,count(distinct category_id_main) as numero_categoria_time

                    ,case when count(distinct order_item_id)    = 0 then 0 else sum(gmv_item)/count(distinct order_item_id)    end as gmv_linea
                    ,case when count(distinct order_item_id)    = 0 then 0 else sum(gmv_item)/count(order_item_id)             end as gmv_promedio_productos

                    ,case when count(distinct category_id_main) = 0 then 0 else sum(gmv_item)/count(distinct category_id_main) end as gmv_categoria
                    ,case when count(distinct sub_category_id)  = 0 then 0 else sum(gmv_item)/count(distinct sub_category_id)  end as gmv_subcategoria
         
                    --size_order_kg
                    ,sum(volumen_kg)-sum(case when (category_name ilike '%Aseo%' or  category_name ilike '%Limpe%' or subcategory_name ilike'%Aseo%' or subcategory_name ilike'%Limpeza%' 
                                                    or subcategory_name ilike'%Higiene%' or category_name ilike '%Desechables%' or category_name ilike '%Descartáveis%'
                                                    or subcategory_name ilike'%Desechables%' or subcategory_name ilike'%Utensilios%')
                                                    then volumen_kg else 0 end) 
                                                    as size_order_kg_real
                                                    
                    --gmv_kg                     
                    ,case when sum(volumen_kg)-sum(case when (category_name ilike '%Aseo%' or  category_name ilike '%Limpe%' or subcategory_name ilike'%Aseo%' or subcategory_name ilike'%Limpeza%' 
                                                    or subcategory_name ilike'%Higiene%' or category_name ilike '%Desechables%' or category_name ilike '%Descartáveis%'
                                                    or subcategory_name ilike'%Desechables%' or subcategory_name ilike'%Utensilios%')
                                                    then volumen_kg else 0 end) = 0 then 0 
                                else sum(gmv_item)/sum(volumen_kg)-sum(case when (category_name ilike '%Aseo%' or  category_name ilike '%Limpe%' or subcategory_name ilike'%Aseo%' or subcategory_name ilike'%Limpeza%' 
                                                                                    or subcategory_name ilike'%Higiene%' or category_name ilike '%Desechables%' or category_name ilike '%Descartáveis%'
                                                                                    or subcategory_name ilike'%Desechables%' or subcategory_name ilike'%Utensilios%')
                                then volumen_kg else 0 end) end 
                                as gmv_kg
                                
                    --gmv_linea_kg                   
                    ,case when count(distinct order_item_id) = 0 or (sum(volumen_kg)-sum (case when (category_name ilike '%Aseo%' or  category_name ilike '%Limpe%' or subcategory_name ilike'%Aseo%' or subcategory_name ilike'%Limpeza%' 
                                                                                                        or subcategory_name ilike'%Higiene%' or category_name ilike '%Desechables%' or category_name ilike '%Descartáveis%'
                                                                                                        or subcategory_name ilike'%Desechables%' or subcategory_name ilike'%Utensilios%')
                                                                                            then volumen_kg else 0 end)) = 0
                                then 0 else
                                        (sum(gmv_item)/count(distinct order_item_id))/(sum(volumen_kg)-sum (case when (category_name ilike '%Aseo%' or  category_name ilike '%Limpe%' or subcategory_name ilike'%Aseo%' or subcategory_name ilike'%Limpeza%' 
                                                                                                                        or subcategory_name ilike'%Higiene%' or category_name ilike '%Desechables%' or category_name ilike '%Descartáveis%'
                                                                                                                        or subcategory_name ilike'%Desechables%' or subcategory_name ilike'%Utensilios%')
                                                                                                            then volumen_kg else 0 end)) 
                                end as gmv_linea_kg
                                

                    --kg_linea     
                    ,case when count(distinct order_item_id) =0 then 0 else
                                (sum(volumen_kg)-sum (case when (category_name ilike '%Aseo%' or  category_name ilike '%Limpe%' or subcategory_name ilike'%Aseo%' or subcategory_name ilike'%Limpeza%' 
                                                                or subcategory_name ilike'%Higiene%' or category_name ilike '%Desechables%' or category_name ilike '%Descartáveis%'
                                                                or subcategory_name ilike'%Desechables%' or subcategory_name ilike'%Utensilios%')
                                                                then volumen_kg else 0 end))/count(distinct order_item_id) 
                                end as kg_linea
                    
                    --kg_subcat
                    ,case when count(distinct sub_category_id) =0 then 0 else
                                (sum(volumen_kg)-sum(case when (category_name ilike '%Aseo%' or  category_name ilike '%Limpe%' or subcategory_name ilike'%Aseo%' or subcategory_name ilike'%Limpeza%' 
                                                                or subcategory_name ilike'%Higiene%' or category_name ilike '%Desechables%' or category_name ilike '%Descartáveis%'
                                                                or subcategory_name ilike'%Desechables%' or subcategory_name ilike'%Utensilios%')
                                                                then volumen_kg else 0 end))/count(distinct sub_category_id) 
                                end as kg_subcat
                    
                    --kg_cat                         
                    ,case when count(distinct category_id_main) =0 then 0 else
                                 (sum(volumen_kg)-sum(case when (category_name ilike '%Aseo%' or  category_name ilike '%Limpe%' or subcategory_name ilike'%Aseo%' or subcategory_name ilike'%Limpeza%' 
                                                                 or subcategory_name ilike'%Higiene%' or category_name ilike '%Desechables%' or category_name ilike '%Descartáveis%'
                                                                 or subcategory_name ilike'%Desechables%' or subcategory_name ilike'%Utensilios%')
                                                                then volumen_kg else 0 end))/count(distinct category_id_main)
                    end as kg_cat
               
               
                --ABARROTES
                ,sum (case when (category_name ilike '%Abarrot%' or  category_name ilike '%Merce%' or subcategory_name ilike '%Abarrotes%' 
                      or subcategory_name ilike '%Aceites%' or subcategory_name ilike '%Mercearia%') 
                      then volumen_kg else 0 end)
                      as vol_Abarrotes
                      
                ,sum (case when (category_name ilike '%Abarrot%' or  category_name ilike  '%Merce%' or subcategory_name ilike '%Abarrotes%' 
                      or subcategory_name ilike '%Aceites%' or subcategory_name ilike '%Mercearia%') 
                      then gmv_item   else 0 end) 
                      as gmv_Abarrotes
                 
                ,sum (case when (category_name ilike '%Abarrot%' or  category_name ilike '%Merce%' or subcategory_name ilike '%Abarrotes%' 
                      or subcategory_name ilike '%Aceites%' or subcategory_name ilike '%Mercearia%') 
                      then 1 else 0 end)                       
                      as buy_Abarrotes
                
                
                --ASEO y LIMPIEZA
                ,sum (case when (category_name ilike '%Aseo%' or  category_name ilike '%Limpe%'
                     or subcategory_name ilike '%Aseo%' or subcategory_name ilike '%Limpeza%' or subcategory_name ilike '%Higiene%')
                     then volumen_kg else 0 end)         
                     as vol_Aseo
                            
                ,sum (case when (category_name ilike '%Aseo%' or  category_name ilike '%Limpe%'
                                or subcategory_name ilike '%Aseo%' or subcategory_name ilike '%Limpeza%' or subcategory_name ilike '%Higiene%')
                                then gmv_item  else 0 end) 
                                as gmv_Aseo
                                
                ,sum (case when (category_name ilike '%Aseo%' or  category_name ilike '%Limpe%'
                                or subcategory_name ilike '%Aseo%' or subcategory_name ilike '%Limpeza%' or subcategory_name ilike '%Higiene%')
                                then 1 else 0 end)           
                                as buy_Aseo
                
                --BEBIDAS
                ,sum (case when (category_name ilike '%Bebidas%' or subcategory_name ilike '%Bebidas%' or subcategory_name ilike '%Cerve%') 
                     then volumen_kg else 0 end)       
                     as vol_bebida
                     
                ,sum (case when (category_name ilike '%Bebidas%' or subcategory_name ilike '%Bebidas%' or subcategory_name ilike '%Cerve%')  
                     then gmv_item   else 0 end)        
                     as gmv_bebida
                    
                ,sum (case when (category_name ilike '%Bebidas%' or subcategory_name ilike '%Bebidas%' or subcategory_name ilike '%Cerve%')  
                     then 1  else 0 end)        
                     as buy_bebida
                    
                --CONGELADOS    
                
                ,sum (case when (category_name ilike '%Congelados%' or subcategory_name  ilike '%Congelados%') 
                     then volumen_kg else 0 end) 
                     as vol_congelados

                ,sum (case when (category_name ilike '%Congelados%' or subcategory_name ilike '%Congelados%') 
                     then gmv_item   else 0 end)        
                     as gmv_congelados

                ,sum (case when (category_name ilike '%Congelados%' or subcategory_name ilike '%Congelados%') 
                     then 1 else 0 end)        
                     as buy_congelados
                
                --DESECHABLES Y UTENSILIOS
            
                ,sum (case when (category_name ilike '%Desechables%' or category_name ilike '%Descartáveis%'
                     or subcategory_name ilike '%Desechables%' or subcategory_name ilike '%Utensilios%')
                     then volumen_kg else 0 end)       
                     as vol_desechables
                
                ,sum (case when (category_name ilike '%Desechables%' or category_name ilike '%Descartáveis%'
                     or subcategory_name ilike '%Desechables%' or subcategory_name ilike '%Utensilios%') 
                     then gmv_item   else 0 end)        
                     as gmv_desechables
                     
                ,sum (case when (category_name ilike '%Desechables%' or category_name ilike '%Descartáveis%'
                     or subcategory_name ilike '%Desechables%' or subcategory_name ilike '%Utensilios%')  
                     then 1 else 0 end)        
                     as buy_desechables
            
                -- FRUTAS Y VERDURAS
                ,sum (case when (category_name ilike '%Frutas%' or category_name ilike '%Verduras%' 
                      or subcategory_name ilike'%Frutas%' or subcategory_name ilike'%Legumbres%' 
                      or subcategory_name ilike'%Tuberculos%' or subcategory_name ilike'%Verduras%' )
                      then volumen_kg else 0 end)        
                      as vol_fruver
                      
                ,sum (case when (category_name ilike '%Frutas%' or category_name ilike '%Verduras%' 
                      or subcategory_name ilike'%Frutas%' or subcategory_name ilike'%Legumbres%' 
                      or subcategory_name ilike'%Tuberculos%' or subcategory_name ilike'%Verduras%' )  
                      then gmv_item else 0 end)       
                      as gmv_fruver
                     
                ,sum (case when (category_name ilike '%Frutas%' or category_name ilike '%Verduras%' 
                     or subcategory_name ilike'%Frutas%' or subcategory_name ilike'%Legumbres%' 
                     or subcategory_name ilike'%Tuberculos%' or subcategory_name ilike'%Verduras%') 
                     then 1 else 0 end)
                     as buy_fruver
                
                --HUEVOS
                ,sum (case when (category_name ilike '%Ovos%' or category_name ilike '%Huevos%'
                      or subcategory_name ilike'%Lacteos y Huevos%' or subcategory_name ilike'%Lácteos & Huevos%') 
                     then volumen_kg else 0 end)        
                     as vol_Lacteos_huevos
                     
                ,sum (case when (category_name ilike '%Ovos%' or category_name ilike '%Huevos%'
                      or subcategory_name ilike'%Lacteos y Huevos%' or subcategory_name ilike'%Lácteos & Huevos%') 
                      then gmv_item   else 0 end)        
                      as gmv_Lacteos_huevos
                
                ,sum (case when (category_name ilike '%Ovos%' or category_name ilike '%Huevos%'
                      or subcategory_name ilike'%Lacteos y Huevos%' or subcategory_name ilike'%Lácteos & Huevos%') 
                     then 1 else 0 end)        
                     as buy_Lacteos_huevos
                
                --PROTEINAS

                ,sum (case when (category_name ilike '%Proteínas%' or subcategory_name ilike '%Proteínas%'
                     or subcategory_name ilike'%Pescado%')
                     then volumen_kg else 0 end)       
                     as vol_proteinas
                            
                ,sum (case when (category_name ilike '%Proteínas%' or subcategory_name ilike '%Proteínas%'
                     or subcategory_name ilike'%Pescado%')
                     then gmv_item   else 0 end)        
                     as gmv_proteinas
                
                ,sum (case when (category_name ilike '%Proteínas%' or subcategory_name ilike '%Proteínas%'
                     or subcategory_name ilike'%Pescado%')
                     then 1  else 0 end)        
                     as buy_proteinas
                
                
                ----subcategorias---
                ,sum (case when (subcategory_name ilike '%Aceite%' or subcategory_name ilike '%Azeite%') then volumen_kg else 0 end) as vol_sub_3_Aceites_Grasas
                ,sum (case when (subcategory_name ilike '%Aceite%' or subcategory_name ilike '%Azeite%') then gmv_item   else 0 end) as gmv_sub_3_Aceites_Grasas
                ,sum (case when (subcategory_name ilike '%Aceite%' or subcategory_name ilike '%Azeite%') then 1   else 0 end)        as buy_sub_3_Aceites_Grasas
                
                ,sum (case when (subcategory_name ilike '%Arro%')  then volumen_kg else 0 end) as vol_sub_5_Arroz
                ,sum (case when (subcategory_name ilike '%Arro%')  then gmv_item   else 0 end) as gmv_sub_5_Arroz
                ,sum (case when (subcategory_name ilike '%Arro%')  then 1          else 0 end) as buy_sub_5_Arroz
                
                ,sum (case when (subcategory_name ilike '%Azúca%' or subcategory_name ilike '%Açúca%') then volumen_kg  else 0 end) as vol_sub_6_Azucar_Endulzantes
                ,sum (case when (subcategory_name ilike '%Azúca%' or subcategory_name ilike '%Açúca%') then gmv_item    else 0 end) as gmv_sub_6_Azucar_Endulzantes
                ,sum (case when (subcategory_name ilike '%Azúca%' or subcategory_name ilike '%Açúca%') then 1           else 0 end) as buy_sub_6_Azucar_Endulzantes
                
                ,sum (case when (subcategory_name ilike '%Café%') then volumen_kg else 0 end) as vol_sub_8_Cafe_Chocolate_Infusiones
                ,sum (case when (subcategory_name ilike '%Café%') then gmv_item   else 0 end) as gmv_sub_8_Cafe_Chocolate_Infusiones
                ,sum (case when (subcategory_name ilike '%Café%') then 1          else 0 end) as buy_sub_8_Cafe_Chocolate_Infusiones
                
                ,sum (case when (subcategory_name ilike '%Cerdo%' or subcategory_name ilike '%Suíno%') then volumen_kg else 0 end) as vol_sub_9_Cerdo
                ,sum (case when (subcategory_name ilike '%Cerdo%' or subcategory_name ilike '%Suíno%') then gmv_item   else 0 end) as gmv_sub_9_Cerdo
                ,sum (case when (subcategory_name ilike '%Cerdo%' or subcategory_name ilike '%Suíno%') then 1          else 0 end) as buy_sub_9_Cerdo
                
                ,sum (case when (subcategory_name ilike '%Chiles%') then volumen_kg else 0 end) as vol_sub_11_Chiles
                ,sum (case when (subcategory_name ilike '%Chiles%') then gmv_item   else 0 end) as gmv_sub_11_Chiles
                ,sum (case when (subcategory_name ilike '%Chiles%') then 1          else 0 end) as buy_sub_11_Chiles
                
                ,sum (case when (subcategory_name ilike '%Lácteos%' or subcategory_name ilike '%Cremas%' or subcategory_name ilike '%Lácteos & Huevos%') then volumen_kg else 0 end) as vol_sub_15_Derivados_Lacteos
                ,sum (case when (subcategory_name ilike '%Lácteos%' or subcategory_name ilike '%Cremas%' or subcategory_name ilike '%Lácteos & Huevos%') then gmv_item   else 0 end) as gmv_sub_15_Derivados_Lacteos
                ,sum (case when (subcategory_name ilike '%Lácteos%' or subcategory_name ilike '%Cremas%' or subcategory_name ilike '%Lácteos & Huevos%') then 1          else 0 end) as buy_sub_15_Derivados_Lacteos
               
                ,sum (case when (subcategory_name ilike 'Embutidos') then volumen_kg else 0 end) as vol_sub_20_Embutidos
                ,sum (case when (subcategory_name ilike 'Embutidos') then gmv_item   else 0 end) as gmv_sub_20_Embutidos
                ,sum (case when (subcategory_name ilike 'Embutidos') then 1          else 0 end) as buy_sub_20_Embutidos
                
                ,sum (case when (subcategory_name ilike '%Enlatado%' or subcategory_name ilike '%Fruto%') then volumen_kg else 0 end) as vol_sub_22_Enlatados
                ,sum (case when (subcategory_name ilike '%Enlatado%' or subcategory_name ilike '%Fruto%') then gmv_item   else 0 end) as gmv_sub_22_Enlatados
                ,sum (case when (subcategory_name ilike '%Enlatado%' or subcategory_name ilike '%Fruto%') then 1          else 0 end) as buy_sub_22_Enlatados
                
                ,sum (case when (subcategory_name ilike '%Especi%' or subcategory_name ilike '%Tempero%') then volumen_kg else 0 end) as vol_sub_23_Especias
                ,sum (case when (subcategory_name ilike '%Especi%' or subcategory_name ilike '%Tempero%') then gmv_item   else 0 end) as gmv_sub_23_Especias
                ,sum (case when (subcategory_name ilike '%Especi%' or subcategory_name ilike '%Tempero%') then 1          else 0 end) as buy_sub_23_Especias
                
                ,sum (case when (subcategory_name ilike '%Fruta%' ) then volumen_kg else 0 end) as vol_sub_24_Frutas
                ,sum (case when (subcategory_name ilike '%Fruta%' ) then gmv_item   else 0 end) as gmv_sub_24_Frutas
                ,sum (case when (subcategory_name ilike '%Fruta%' ) then 1          else 0 end) as buy_sub_24_Frutas
                
                ,sum (case when (subcategory_name ilike '%Grano%' or subcategory_name ilike '%Grão%' or subcategory_name ilike '%Feij%') then volumen_kg else 0 end) as vol_sub_26_Granos
                ,sum (case when (subcategory_name ilike '%Grano%' or subcategory_name ilike '%Grão%' or subcategory_name ilike '%Feij%') then gmv_item   else 0 end) as gmv_sub_26_Granos
                ,sum (case when (subcategory_name ilike '%Grano%' or subcategory_name ilike '%Grão%' or subcategory_name ilike '%Feij%') then 1          else 0 end) as buy_sub_26_Granos
                
                ,sum (case when (subcategory_name ilike '%Harina%' or subcategory_name ilike '%Farinha%') then volumen_kg else 0 end) as vol_sub_27_Harinas_Mezclas
                ,sum (case when (subcategory_name ilike '%Harina%' or subcategory_name ilike '%Farinha%') then gmv_item   else 0 end) as gmv_sub_27_Harinas_Mezclas
                ,sum (case when (subcategory_name ilike '%Harina%' or subcategory_name ilike '%Farinha%') then 1          else 0 end) as buy_sub_27_Harinas_Mezclas
                
                ,sum (case when (subcategory_name ilike '%Huevo%' or subcategory_name ilike '%Ovos%') then volumen_kg else 0 end) as vol_sub_30_Huevos
                ,sum (case when (subcategory_name ilike '%Huevo%' or subcategory_name ilike '%Ovos%') then gmv_item   else 0 end) as gmv_sub_30_Huevos
                ,sum (case when (subcategory_name ilike '%Huevo%' or subcategory_name ilike '%Ovos%') then 1          else 0 end) as buy_sub_30_Huevos
                
                ,sum (case when (subcategory_name ilike '%Leche%' or subcategory_name ilike '%Lact%' or subcategory_name ilike '%Leit%') then volumen_kg else 0 end) as vol_sub_32_Leches
                ,sum (case when (subcategory_name ilike '%Leche%' or subcategory_name ilike '%Lact% 'or subcategory_name ilike '%Leit%')  then gmv_item   else 0 end) as gmv_sub_32_Leches
                ,sum (case when (subcategory_name ilike '%Leche%' or subcategory_name ilike '%Lact%' or subcategory_name ilike '%Leit%')  then 1          else 0 end) as buy_sub_32_Leches
                
                ,sum (case when (subcategory_name ilike '%Mante%') then volumen_kg else 0 end) as vol_sub_33_Mantequillas_Margarinas
                ,sum (case when (subcategory_name ilike '%Mante%') then gmv_item   else 0 end) as gmv_sub_33_Mantequillas_Margarinas
                ,sum (case when (subcategory_name ilike '%Mante%') then 1          else 0 end) as buy_sub_33_Mantequillas_Margarinas
                
                ,sum (case when (subcategory_name ilike '%Embu%' or subcategory_name = 'Otras Proteínas Frescas' or subcategory_name = 'Proteínas Frescas' or  (subcategory_name = 'Otros' and (category_name = 'Proteínas Congeladas' or category_name = 'Proteínas Frescas'))) then volumen_kg else 0 end) as vol_sub_34_Otras_Proteinas
                ,sum (case when (subcategory_name ilike '%Embu%' or subcategory_name = 'Otras Proteínas Frescas' or subcategory_name = 'Proteínas Frescas' or  (subcategory_name = 'Otros' and (category_name = 'Proteínas Congeladas' or category_name = 'Proteínas Frescas'))) then volumen_kg else 0 end) as gmv_sub_34_Otras_Proteinas
                ,sum (case when (subcategory_name ilike '%Embu%' or subcategory_name = 'Otras Proteínas Frescas' or subcategory_name = 'Proteínas Frescas' or  (subcategory_name = 'Otros' and (category_name = 'Proteínas Congeladas' or category_name = 'Proteínas Frescas'))) then 1          else 0 end) as buy_sub_34_Otras_Proteinas
                
                ,sum (case when (subcategory_name ilike '%Otros Abarrotes%' or subcategory_name ilike '%Condimento%') then volumen_kg else 0 end) as vol_sub_36_Otros_Abarrotes
                ,sum (case when (subcategory_name ilike '%Otros Abarrotes%' or subcategory_name ilike '%Condimento%') then gmv_item   else 0 end) as gmv_sub_36_Otros_Abarrotes
                ,sum (case when (subcategory_name ilike '%Otros Abarrotes%' or subcategory_name ilike '%Condimento%') then 1          else 0 end) as buy_sub_36_Otros_Abarrotes
                
                ,sum (case when (subcategory_name ilike '%Panader%'or subcategory_name ilike '%Confit%') then volumen_kg else 0 end) as vol_sub_37_Panaderia_Tortilleria
                ,sum (case when (subcategory_name ilike '%Panader%'or subcategory_name ilike '%Confit%') then gmv_item   else 0 end) as gmv_sub_37_Panaderia_Tortilleria
                ,sum (case when (subcategory_name ilike '%Panader%'or subcategory_name ilike '%Confit%') then 1          else 0 end) as buy_sub_37_Panaderia_Tortilleria
                
                ,sum (case when (subcategory_name ilike '%Pasta%' or subcategory_name ilike '%Massa%') then volumen_kg else 0 end) as vol_sub_39_Pasta
                ,sum (case when (subcategory_name ilike '%Pasta%' or subcategory_name ilike '%Massa%') then gmv_item   else 0 end) as gmv_sub_39_Pasta
                ,sum (case when (subcategory_name ilike '%Pasta%' or subcategory_name ilike '%Massa%') then 1          else 0 end) as buy_sub_39_Pasta
                
                ,sum (case when (subcategory_name ilike '%Pescado%' or subcategory_name = 'Peix') then volumen_kg else 0 end) as vol_sub_40_Pescados_Mariscos_Congelado
                ,sum (case when (subcategory_name ilike '%Pescado%' or subcategory_name = 'Peix') then gmv_item   else 0 end) as gmv_sub_40_Pescados_Mariscos_Congelado
                ,sum (case when (subcategory_name ilike '%Pescado%' or subcategory_name = 'Peix') then 1          else 0 end) as buy_sub_40_Pescados_Mariscos_Congelado
                
                ,sum (case when (subcategory_name ilike '%Pollo%' or subcategory_name ilike '%Frango%') then volumen_kg else 0 end) as vol_sub_41_Pollo
                ,sum (case when (subcategory_name ilike '%Pollo%' or subcategory_name ilike '%Frango%') then gmv_item   else 0 end) as gmv_sub_41_Pollo
                ,sum (case when (subcategory_name ilike '%Pollo%' or subcategory_name ilike '%Frango%') then 1          else 0 end) as buy_sub_41_Pollo
                
                ,sum (case when (subcategory_name ilike '%Que%') then volumen_kg else 0 end) as vol_sub_43_Quesos
                ,sum (case when (subcategory_name ilike '%Que%') then gmv_item   else 0 end) as gmv_sub_43_Quesos
                ,sum (case when (subcategory_name ilike '%Que%') then 1          else 0 end) as buy_sub_43_Quesos
                
                ,sum (case when (subcategory_name ilike 'Rappi') then volumen_kg else 0 end) as vol_sub_44_Rappi
                ,sum (case when (subcategory_name ilike 'Rappi') then gmv_item   else 0 end) as gmv_sub_44_Rappi
                ,sum (case when (subcategory_name ilike 'Rappi') then 1          else 0 end) as buy_sub_44_Rappi
                
                ,sum (case when (subcategory_name ilike '%Res%' or subcategory_name ilike '%Vermel%') then volumen_kg else 0 end) as vol_sub_45_Res
                ,sum (case when (subcategory_name ilike '%Res%' or subcategory_name ilike '%Vermel%') then gmv_item   else 0 end) as gmv_sub_45_Res
                ,sum (case when (subcategory_name ilike '%Res%' or subcategory_name ilike '%Vermel%') then 1          else 0 end) as buy_sub_46_Res
                
                ,sum (case when (subcategory_name ilike '%Sal%') then volumen_kg else 0 end) as vol_sub_47_Sal_Sazonadores
                ,sum (case when (subcategory_name ilike '%Sal%') then gmv_item   else 0 end) as gmv_sub_47_Sal_Sazonadores
                ,sum (case when (subcategory_name ilike '%Sal%') then 1          else 0 end) as buy_sub_47_Sal_Sazonadores
                
                ,sum (case when (subcategory_name ilike '%Salsas%' or subcategory_name ilike '%Molhos%') then volumen_kg else 0 end) as vol_sub_48_Salsas
                ,sum (case when (subcategory_name ilike '%Salsas%' or subcategory_name ilike '%Molhos%') then gmv_item   else 0 end) as gmv_sub_48_Salsas
                ,sum (case when (subcategory_name ilike '%Salsas%' or subcategory_name ilike '%Molhos%') then gmv_item   else 0 end) as buy_sub_48_Salsas
                
                ,sum (case when (subcategory_name ilike '%Tubérculos%') then volumen_kg else 0 end) as vol_sub_49_Tuberculos
                ,sum (case when (subcategory_name ilike '%Tubérculos%') then gmv_item   else 0 end) as gmv_sub_49_Tuberculos
                ,sum (case when (subcategory_name ilike '%Tubérculos%') then 1          else 0 end) as buy_sub_49_Tuberculos
                
                ,sum (case when (subcategory_name = 'Verduras' or subcategory_name ilike '%Hortaliças%' or subcategory_name ilike '%Legum%' or subcategory_name ilike '%Cogumelos%' or subcategory_name ilike '%Folhas%' or subcategory_name ilike '%Hierbas%' or subcategory_name ilike '%Hongos%' or subcategory_name ilike '%Raízes%' ) then volumen_kg else 0 end) as vol_sub_51_Verduras
                ,sum (case when (subcategory_name = 'Verduras' or subcategory_name ilike '%Hortaliças%' or subcategory_name ilike '%Legum%' or subcategory_name ilike '%Cogumelos%' or subcategory_name ilike '%Folhas%' or subcategory_name ilike '%Hierbas%' or subcategory_name ilike '%Hongos%' or subcategory_name ilike '%Raízes%') then gmv_item   else 0 end) as gmv_sub_51_Verduras
                ,sum (case when (subcategory_name = 'Verduras' or subcategory_name ilike '%Hortaliças%' or subcategory_name ilike '%Legum%' or subcategory_name ilike '%Cogumelos%' or subcategory_name ilike '%Folhas%' or subcategory_name ilike '%Hierbas%' or subcategory_name ilike '%Hongos%' or subcategory_name ilike '%Raízes%') then 1          else 0 end) as buy_sub_51_Verduras
                
                ,sum (case when (subcategory_name ilike '%Bebida%' or subcategory_name ilike '%Refres%' or subcategory_name ilike '%Polpa%' or subcategory_name ilike '%Pulpa%' or subcategory_name ilike '%Cerve%') then volumen_kg else 0 end) as vol_sub_51_Bebidas
                ,sum (case when (subcategory_name ilike '%Bebida%' or subcategory_name ilike '%Refres%' or subcategory_name ilike '%Polpa%' or subcategory_name ilike '%Pulpa%' or subcategory_name ilike '%Cerve%') then gmv_item   else 0 end) as gmv_sub_51_Bebidas
                ,sum (case when (subcategory_name ilike '%Bebida%' or subcategory_name ilike '%Refres%' or subcategory_name ilike '%Polpa%' or subcategory_name ilike '%Pulpa%' or subcategory_name ilike '%Cerve%') then 1          else 0 end) as buy_sub_51_Bebidas
                
          from  Ordenesxitem  OI 

group by order_boleto,dia_boleto,customer_id,year,week)

select agrupacion_orden.*
,agrupacion_skus.*
,total_discount/numero_ordenes as discountxo
,size_order_kg_real/numero_ordenes as sizexo
,numero_ordenes/numero_lineas_time as oxlineas
,numero_ordenes/numero_subcategorias_time as oxsubcat
,numero_ordenes/numero_categoria_time as oxcat

from (select distinct  IC.customer_id
                        ,order_boleto
                        ,dia_boleto                    
                        ,week
                        ,year
,sum(gmv) as gmv_total
,count(distinct order_id) as numero_ordenes
,avg(numero_lineas_orden) as promedio_numero_lineas
,sum(gmv)/avg(numero_lineas_orden) as gmv_linea_porden
,count(distinct order_id)/avg(numero_lineas_orden) as ordenesxplineas

from info_cliente IC
group by order_boleto,dia_boleto,customer_id,week,year
) as agrupacion_orden

inner join agrupacion_skus  on (agrupacion_skus.order_boleto=agrupacion_orden.order_boleto 
                                and agrupacion_skus.week=agrupacion_orden.week 
                                and agrupacion_skus.year=agrupacion_orden.year)

    """.format(TIME=TIME)
    
    dataframe = read_connection_data_warehouse.runQuery(query)
    
    return dataframe

###################################################################################################################################################################################
###################################################################################################################################################################################
###################################################################################################################################################################################
###################################################################################################################################################################################


def data_comp_freq_week_previous_query_vboletos(TIME):
    
    query = """
    With orden_boleto as(select   distinct bc.customer_id 
                            ,bo.order_id    as order_boleto 
                            ,bo.order_number
                            ,fbs.name as segmento  
                            ,s.site_identifier_value
                            ,Extract(Week  from bo.date_created) as bweek
                            ,Extract(year  from bo.date_created) as byear
                            ,bo.date_created::DATE as dia_boleto
                                    
                            FROM postgres_broadleaf_federate."broadleaf.blc_order"  bo 
                            inner join lnd_proc_dev.frucap_boletos                                   fru  on fru.order_code=bo.order_number
                            inner join postgres_broadleaf_federate."broadleaf.blc_fulfillment_group" bfg  ON bfg.order_id = bo.order_id
                            inner join postgres_broadleaf_federate."broadleaf.blc_fulfillment_order" bfo  ON bfo.fulfillment_group_id = bfg.fulfillment_group_id
                            inner join postgres_broadleaf_federate."broadleaf.fb_fulfillment_group"  ffg  ON ffg.fulfillment_group_id = bfg.fulfillment_group_id
                            inner join postgres_broadleaf_federate."broadleaf.fb_order"              fo   ON fo.order_id = bo.order_id
                            inner join postgres_broadleaf_federate."broadleaf.blc_order_payment"     bop  ON bop.order_id = bo.order_id
                            inner join postgres_broadleaf_federate."broadleaf.blc_site"              s    ON s.site_id = bo.site_disc
                            inner join postgres_broadleaf_federate."broadleaf.blc_customer"          bc   ON bc.customer_id = bo.customer_id
                            left join  postgres_broadleaf_federate."broadleaf.fb_order_type"         fot  ON fot.fb_order_type_id=fo.fb_order_type_id
                            left join  postgres_broadleaf_federate."broadleaf.fb_customer"           fc   ON fc.customer_id = bo.customer_id
                            left join  postgres_broadleaf_federate."broadleaf.fb_business_type"      fbs  ON fc.business_type_id = fbs.business_type_id 
                                )

 ,ids as (select distinct customer_id from orden_boleto)

SELECT uid
    ,order_boleto
    ,order_number
    ,dia_boleto
,count(week_session) as num_sem_consesion
,count(case when num_sesiones>=7 then week_session end) as num_sem_m7s
,avg(num_dias_distintos) as avg_dias_conact_perweek
,sum(num_dias_distintos) as total_dias_conact_perweek 

,count(case when num_products>=20 then week_session end) as num_sem_m20p
,avg(num_products) as avg_prods_seen_perweek 
,sum(num_products) as total_prods_seen_perweek 
,avg(promedio_sesion) as promedio_sesion


FROM
    (SELECT uid
            ,order_boleto
            ,order_number
            ,dia_boleto
            ,week_session
            ,count(distinct sessionid) as num_sesiones
            ,count(distinct start_session) as num_dias_distintos
            ,sum(num_products_seen) as num_products
            ,sum(num_searches) as num_searches 
            ,avg(session_length) as promedio_sesion
            FROM
            (SELECT profile_identity::varchar as uid
                    ,ctsessionid as sessionid
                    ,order_boleto
                    ,order_number
                    ,dia_boleto
                    ,sessionlength/60 as session_length
                    ,min(date(event_date)) as start_session
                    ,extract(week from min(date(event_date))) as week_session
                    ,count(case when eventname='FRF Search Tracking' then frsearchterm end) as num_searches
                    ,count(case when eventname='FRF Product View' then frproductname end) as num_products_seen
                    FROM dpr_clevertap.events ce
                    inner  join ids             ids  on ids.customer_id=ce.profile_identity
                    left join orden_boleto      fru  on fru.customer_id=ce.profile_identity
                    WHERE date(event_date) < (dia_boleto + interval '1 days')
                    and date(event_date) >= (dia_boleto - interval '{TIME} weeks') 
                    AND ctsessionid is not null 
                    GROUP BY 1,2,3,4,5,6)
            GROUP BY 1,2,3,4,5) 
GROUP BY 1,2,3,4
        """.format(TIME=TIME)
    
    dataframe = read_connection_data_warehouse.runQuery(query)
    
    return dataframe


###################################################################################################################################################################################
###################################################################################################################################################################################
###################################################################################################################################################################################
###################################################################################################################################################################################


def data_comp_clicks_week_previous_query_vboletos(TIME):
    
    query = """
        With orden_boleto as(select   distinct bc.customer_id 
                            ,bo.order_id    as order_boleto 
                            ,bo.order_number
                            ,fbs.name as segmento  
                            ,s.site_identifier_value
                            ,Extract(Week  from bo.date_created) as bweek
                            ,Extract(year  from bo.date_created) as byear
                            ,bo.date_created::DATE as dia_boleto
                                    
                            FROM postgres_broadleaf_federate."broadleaf.blc_order"  bo 
                            inner join lnd_proc_dev.frucap_boletos                                   fru  on fru.order_code=bo.order_number
                            inner join postgres_broadleaf_federate."broadleaf.blc_fulfillment_group" bfg  ON bfg.order_id = bo.order_id
                            inner join postgres_broadleaf_federate."broadleaf.blc_fulfillment_order" bfo  ON bfo.fulfillment_group_id = bfg.fulfillment_group_id
                            inner join postgres_broadleaf_federate."broadleaf.fb_fulfillment_group"  ffg  ON ffg.fulfillment_group_id = bfg.fulfillment_group_id
                            inner join postgres_broadleaf_federate."broadleaf.fb_order"              fo   ON fo.order_id = bo.order_id
                            inner join postgres_broadleaf_federate."broadleaf.blc_order_payment"     bop  ON bop.order_id = bo.order_id
                            inner join postgres_broadleaf_federate."broadleaf.blc_site"              s    ON s.site_id = bo.site_disc
                            inner join postgres_broadleaf_federate."broadleaf.blc_customer"          bc   ON bc.customer_id = bo.customer_id
                            left join  postgres_broadleaf_federate."broadleaf.fb_order_type"         fot  ON fot.fb_order_type_id=fo.fb_order_type_id
                            left join  postgres_broadleaf_federate."broadleaf.fb_customer"           fc   ON fc.customer_id = bo.customer_id
                            left join  postgres_broadleaf_federate."broadleaf.fb_business_type"      fbs  ON fc.business_type_id = fbs.business_type_id 
                                )

 ,ids as (select distinct customer_id from orden_boleto)

SELECT DISTINCT  ea.profile_identity as user

   ,order_boleto
   ,order_number
    ,dia_boleto,
   count(DISTINCT case when ea.eventname ='FRB Cart Delete' then frLoggedIn end) as delete_carrito,
   count(DISTINCT case when ea.eventname ='FRF Cart Edit' then eventname end) as edito_carrito,
   count(distinct case when eventname='FRF SAC Contact' then eventname end) as contacto_SAC,
   count( case when eventname='FRF Search Tracking' then frresults end) as veces_utilizo_buscador,
    count(DISTINCT ea.eventname) as distinct_num_events,
    count(DISTINCT case when eventname='View Product Detail' then frproductname when eventname='FRF Product View' then frproductname end) as num_distinct_products_seen,
    count(eventname) as num_total_eventos
    ,sum(case when ea.frsource in ('Android','iOS') then 1 else 0 end) as num_mobile_events
    ,sum(case when ea.frsource in ('Android') then 1 else 0 end) as num_mobile_events_android
    ,sum(case when ea.frsource in ('iOS') then 1 else 0 end) as num_mobile_events_ios,
    sum(case when ea.frsource in ('Web Mobile','Web') then 1 else 0 end) as num_web_events,
    count( DISTINCT case when eventname='Click On Category' and frcategoryname in ('Abarrotes',
'Aseo e Higiene','Bebidas','Congelados','Descartáveis','Desechables','Frutas e Verduras','Frutas & Verduras','Kit Feijoada','Lácteos & Huevos','Laticínios e Ovos',
'Limpeza e Higiene','Mercearia','Otros','Proteínas','Proteínas Congeladas','Proteínas Frescas') then frcategoryname

when eventname='FRF Category View' and frcategoryname in ('Abarrotes','Aseo e Higiene','Bebidas','Congelados','Descartáveis',
'Desechables','Frutas e Verduras','Frutas & Verduras','Kit Feijoada','Lácteos & Huevos','Laticínios e Ovos','Limpeza e Higiene','Mercearia',
'Otros','Proteínas','Proteínas Congeladas','Proteínas Frescas') 

 then frcategoryname end) as num_distinct_categories_seen


FROM dpr_clevertap.events ea
inner  join ids                    on ids.customer_id=ea.profile_identity
inner  join orden_boleto      fru  on fru.customer_id=ea.profile_identity
     
WHERE  date(event_date) < (dia_boleto + interval '1 days')
and date(event_date) >= (dia_boleto - interval '{TIME} weeks') 
GROUP BY 1,2,3,4
        """.format(TIME=TIME)
    
    dataframe = read_connection_data_warehouse.runQuery(query)
    
    return dataframe

###################################################################################################################################################################################
###################################################################################################################################################################################
###################################################################################################################################################################################
###################################################################################################################################################################################


def data_comp_clicks_week_previous_query_vboletos(TIME):
    
    query = """
        With orden_boleto as(select   distinct bc.customer_id 
                            ,bo.order_id    as order_boleto 
                            ,bo.order_number
                            ,fbs.name as segmento  
                            ,s.site_identifier_value
                            ,Extract(Week  from bo.date_created) as bweek
                            ,Extract(year  from bo.date_created) as byear
                            ,bo.date_created::DATE as dia_boleto
                                    
                            FROM postgres_broadleaf_federate."broadleaf.blc_order"  bo 
                            inner join lnd_proc_dev.frucap_boletos                                   fru  on fru.order_code=bo.order_number
                            inner join postgres_broadleaf_federate."broadleaf.blc_fulfillment_group" bfg  ON bfg.order_id = bo.order_id
                            inner join postgres_broadleaf_federate."broadleaf.blc_fulfillment_order" bfo  ON bfo.fulfillment_group_id = bfg.fulfillment_group_id
                            inner join postgres_broadleaf_federate."broadleaf.fb_fulfillment_group"  ffg  ON ffg.fulfillment_group_id = bfg.fulfillment_group_id
                            inner join postgres_broadleaf_federate."broadleaf.fb_order"              fo   ON fo.order_id = bo.order_id
                            inner join postgres_broadleaf_federate."broadleaf.blc_order_payment"     bop  ON bop.order_id = bo.order_id
                            inner join postgres_broadleaf_federate."broadleaf.blc_site"              s    ON s.site_id = bo.site_disc
                            inner join postgres_broadleaf_federate."broadleaf.blc_customer"          bc   ON bc.customer_id = bo.customer_id
                            left join  postgres_broadleaf_federate."broadleaf.fb_order_type"         fot  ON fot.fb_order_type_id=fo.fb_order_type_id
                            left join  postgres_broadleaf_federate."broadleaf.fb_customer"           fc   ON fc.customer_id = bo.customer_id
                            left join  postgres_broadleaf_federate."broadleaf.fb_business_type"      fbs  ON fc.business_type_id = fbs.business_type_id 
                                )

 ,ids as (select distinct customer_id from orden_boleto)

SELECT DISTINCT  ea.profile_identity as user

   ,order_boleto
   ,order_number
    ,dia_boleto,
   count(DISTINCT case when ea.eventname ='FRB Cart Delete' then frLoggedIn end) as delete_carrito,
   count(DISTINCT case when ea.eventname ='FRF Cart Edit' then eventname end) as edito_carrito,
   count(distinct case when eventname='FRF SAC Contact' then eventname end) as contacto_SAC,
   count( case when eventname='FRF Search Tracking' then frresults end) as veces_utilizo_buscador,
    count(DISTINCT ea.eventname) as distinct_num_events,
    count(DISTINCT case when eventname='View Product Detail' then frproductname when eventname='FRF Product View' then frproductname end) as num_distinct_products_seen,
    count(eventname) as num_total_eventos
    ,sum(case when ea.frsource in ('Android','iOS') then 1 else 0 end) as num_mobile_events
    ,sum(case when ea.frsource in ('Android') then 1 else 0 end) as num_mobile_events_android
    ,sum(case when ea.frsource in ('iOS') then 1 else 0 end) as num_mobile_events_ios,
    sum(case when ea.frsource in ('Web Mobile','Web') then 1 else 0 end) as num_web_events,
    count( DISTINCT case when eventname='Click On Category' and frcategoryname in ('Abarrotes',
'Aseo e Higiene','Bebidas','Congelados','Descartáveis','Desechables','Frutas e Verduras','Frutas & Verduras','Kit Feijoada','Lácteos & Huevos','Laticínios e Ovos',
'Limpeza e Higiene','Mercearia','Otros','Proteínas','Proteínas Congeladas','Proteínas Frescas') then frcategoryname

when eventname='FRF Category View' and frcategoryname in ('Abarrotes','Aseo e Higiene','Bebidas','Congelados','Descartáveis',
'Desechables','Frutas e Verduras','Frutas & Verduras','Kit Feijoada','Lácteos & Huevos','Laticínios e Ovos','Limpeza e Higiene','Mercearia',
'Otros','Proteínas','Proteínas Congeladas','Proteínas Frescas') 

 then frcategoryname end) as num_distinct_categories_seen


FROM dpr_clevertap.events ea
inner  join ids                    on ids.customer_id=ea.profile_identity
inner  join orden_boleto      fru  on fru.customer_id=ea.profile_identity
     
WHERE  date(event_date) < (dia_boleto + interval '1 days')
and date(event_date) >= (dia_boleto - interval '{TIME} weeks') 
GROUP BY 1,2,3,4
        """.format(TIME=TIME)
    
    dataframe = read_connection_data_warehouse.runQuery(query)
    
    return dataframe