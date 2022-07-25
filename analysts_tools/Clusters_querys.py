import numpy as np
import pandas as pd

import sys
# route='/srv/scratch/'
# sys.path.append(route)
# route='/srv/scratch/analyst_community/'
# sys.path.append(route)
from analystcommunity import read_connection_data_warehouse


def run_customQuery(query):
    """
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
    
################################################################################################################################################################################################################
################################################################################################################################################################################################################

def query_info_first_order(desde,hasta):
    """
    Esta función extrae la data de la primera orden
    
    """
    query = """
With primera_orden as(select a.* 
                            ,fbs.name as segmento
                            --,fc.segmentation_stage
                            
                            from  (select  
                                 distinct bc.customer_id 
                                ,s.site_identifier_value
                                 ,min(bo.order_id) as first_order 
                                ,Extract(Week  from min(ffg.close_date)) as bweek
                                ,Extract(month from min(ffg.close_date)) as bmonth
                                ,Extract(Year  from min(ffg.close_date)) as byear
                                ,min(ffg.close_date) as primer_dia_de_fulfillment
                                ,min(bo.date_created) as date_first_order 
                                        
                                FROM postgres_broadleaf_federate."broadleaf.blc_order"  bo 
                                inner join postgres_broadleaf_federate."broadleaf.blc_fulfillment_group" bfg  ON bfg.order_id = bo.order_id
                                inner join postgres_broadleaf_federate."broadleaf.blc_fulfillment_order" bfo  ON bfo.fulfillment_group_id = bfg.fulfillment_group_id
                                inner join postgres_broadleaf_federate."broadleaf.fb_fulfillment_group"  ffg  ON ffg.fulfillment_group_id = bfg.fulfillment_group_id
                                inner join postgres_broadleaf_federate."broadleaf.fb_order"              fo   ON fo.order_id = bo.order_id
                                inner join postgres_broadleaf_federate."broadleaf.blc_order_payment"     bop  ON bop.order_id = bo.order_id
                                inner join postgres_broadleaf_federate."broadleaf.blc_site"              s    ON s.site_id = bo.site_disc
                                inner join postgres_broadleaf_federate."broadleaf.blc_customer"          bc   ON bc.customer_id = bo.customer_id
                                left join  postgres_broadleaf_federate."broadleaf.fb_order_type"         fot  ON fot.fb_order_type_id=fo.fb_order_type_id

                                WHERE fo.fb_order_status_id IN (1,6,7,8)
                                    AND bo.order_status = 'SUBMITTED'
                                    AND bfo.status NOT IN ('ARCHIVED','CANCELLED')
                                    AND bop.archived = 'N'
                                    AND (fot.name IS NULL OR fot.name <> 'REFUND')
                                group by bc.customer_id,s.site_identifier_value

                               ) as a
                                   
                                left join  postgres_broadleaf_federate."broadleaf.fb_customer"           fc   ON fc.customer_id = a.customer_id
                                left join  postgres_broadleaf_federate."broadleaf.fb_business_type"      fbs  ON fc.business_type_id = fbs.business_type_id 
                                
                                where  primer_dia_de_fulfillment >= '{desde}'
                                   and  primer_dia_de_fulfillment <= '{hasta}' 
                                   and fbs.name in ('Restaurante')
                                    ) 

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

,info_cliente as (select distinct bo.customer_id 
                ,first_order
                
                ,CASE   WHEN s.site_identifier_value in ('CMX','GDL','MTY','PBC') then (bo.order_subtotal+coalesce(fo.total_tax_iva,0)+coalesce(bo.total_shipping,0))/19.19
                        WHEN s.site_identifier_value in ('SPO','BHZ','CWB') then (bo.order_subtotal+coalesce(fo.total_tax_iva,0)+coalesce(bo.total_shipping,0))/3.88
                        ELSE (bo.order_subtotal+coalesce(fo.total_tax_iva,0)+coalesce(bo.total_shipping,0))/3000 end as gmv
                        
                from primera_orden  po
                inner join postgres_broadleaf_federate."broadleaf.blc_order"             bo   ON bo.order_id = po.first_order
                inner join postgres_broadleaf_federate."broadleaf.blc_fulfillment_group" bfg  ON bfg.order_id = bo.order_id
                inner join postgres_broadleaf_federate."broadleaf.blc_fulfillment_order" bfo  ON bfo.fulfillment_group_id = bfg.fulfillment_group_id
                inner join postgres_broadleaf_federate."broadleaf.fb_fulfillment_group"  ffg  ON ffg.fulfillment_group_id = bfg.fulfillment_group_id
                inner join postgres_broadleaf_federate."broadleaf.fb_order"              fo   ON fo.order_id = bo.order_id
                inner join postgres_broadleaf_federate."broadleaf.blc_order_payment"     bop  ON bop.order_id = bo.order_id
                inner join postgres_broadleaf_federate."broadleaf.blc_site"              s    ON s.site_id = bo.site_disc
                inner join postgres_broadleaf_federate."broadleaf.blc_customer"          bc   ON bc.customer_id = bo.customer_id
                left join  postgres_broadleaf_federate."broadleaf.fb_order_type"         fot  ON fot.fb_order_type_id=fo.fb_order_type_id

                ) 

,Ordenesxitem as (select distinct boi.order_item_id
                        ,bo.order_id
                        ,bo.customer_id
                        ,po.first_order
                        ,primer_dia_de_fulfillment
                        ,date_first_order
                        
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
                            inner join primera_orden                                                 po   ON bo.order_id = po.first_order
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
                    ,first_order
                    ,primer_dia_de_fulfillment
                    ,date_first_order

                    ,sum(discount_applied) as total_discount
                    ,sum(gmv_item) as gmv_subtotal_item
                    ,sum(volumen_kg) as size_order_kg_noreal
                    
                    ,count(distinct order_item_id)    as numero_lineas
                    ,count(distinct sub_category_id)  as numero_subcategorias
                    ,count(distinct category_id_main) as numero_categoria

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

group by customer_id,first_order
,primer_dia_de_fulfillment
,date_first_order)

select agrupacion_orden.*
,agrupacion_skus.*

from (select distinct  IC.customer_id,first_order
,sum(gmv) as gmv_total
--,count(distinct order_id) as numero_ordenes
--sum(gmv)/avg(numero_lineas_orden) as gmv_linea_real

from info_cliente IC
group by customer_id,first_order
) as agrupacion_orden

inner join agrupacion_skus  on (agrupacion_skus.first_order=agrupacion_orden.first_order)

    """.format(desde=desde,hasta=hasta)
    
    dataframe = read_connection_data_warehouse.runQuery(query)
    
    return dataframe

   
################################################################################################################################################################################################################
################################################################################################################################################################################################################

def query_info_first_order_posterior(desde,hasta,TIME,timeline):
    """
    Esta función extrae la data de la primera orden
    
    """
    query = """
With primera_orden as(select a.* 
                            ,fbs.name as segmento
                            --,fc.segmentation_stage
                            
                            from  (select  
                                 distinct bc.customer_id 
                                ,s.site_identifier_value
                                 ,min(bo.order_id) as first_order 
                                ,Extract(Week  from min(ffg.close_date)) as bweek
                                ,Extract(month from min(ffg.close_date)) as bmonth
                                ,Extract(Year  from min(ffg.close_date)) as byear
                                ,min(ffg.close_date) as primer_dia_de_fulfillment
                                ,min(bo.date_created) as date_first_order 
                                        
                                FROM postgres_broadleaf_federate."broadleaf.blc_order"  bo 
                                inner join postgres_broadleaf_federate."broadleaf.blc_fulfillment_group" bfg  ON bfg.order_id = bo.order_id
                                inner join postgres_broadleaf_federate."broadleaf.blc_fulfillment_order" bfo  ON bfo.fulfillment_group_id = bfg.fulfillment_group_id
                                inner join postgres_broadleaf_federate."broadleaf.fb_fulfillment_group"  ffg  ON ffg.fulfillment_group_id = bfg.fulfillment_group_id
                                inner join postgres_broadleaf_federate."broadleaf.fb_order"              fo   ON fo.order_id = bo.order_id
                                inner join postgres_broadleaf_federate."broadleaf.blc_order_payment"     bop  ON bop.order_id = bo.order_id
                                inner join postgres_broadleaf_federate."broadleaf.blc_site"              s    ON s.site_id = bo.site_disc
                                inner join postgres_broadleaf_federate."broadleaf.blc_customer"          bc   ON bc.customer_id = bo.customer_id
                                left join  postgres_broadleaf_federate."broadleaf.fb_order_type"         fot  ON fot.fb_order_type_id=fo.fb_order_type_id

                                WHERE fo.fb_order_status_id IN (1,6,7,8)
                                    AND bo.order_status = 'SUBMITTED'
                                    AND bfo.status NOT IN ('ARCHIVED','CANCELLED')
                                    AND bop.archived = 'N'
                                    AND (fot.name IS NULL OR fot.name <> 'REFUND')

                                group by 1,2
                               ) as a

                                left join  postgres_broadleaf_federate."broadleaf.fb_customer"           fc   ON fc.customer_id = a.customer_id
                                left join  postgres_broadleaf_federate."broadleaf.fb_business_type"      fbs  ON fc.business_type_id = fbs.business_type_id 
                                where  primer_dia_de_fulfillment >= '{desde}'
                                and  primer_dia_de_fulfillment <= '{hasta}'
                                and fbs.name in ('Restaurante')
                                    ) 

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

,info_cliente as (select distinct bo.customer_id 
                ,bo.order_id
                ,ffg.close_date as dia_de_fulfillment
                ,Extract(Week  from ffg.close_date) as week
                ,Extract(Year  from ffg.close_date) as year
                ,bweek
                ,byear
                ,first_order
                ,primer_dia_de_fulfillment

                ,CASE   WHEN s.site_identifier_value in ('CMX','GDL','MTY','PBC') then (bo.order_subtotal+coalesce(fo.total_tax_iva,0)+coalesce(bo.total_shipping,0))/19.19
                        WHEN s.site_identifier_value in ('SPO','BHZ','CWB') then (bo.order_subtotal+coalesce(fo.total_tax_iva,0)+coalesce(bo.total_shipping,0))/3.88
                        ELSE (bo.order_subtotal+coalesce(fo.total_tax_iva,0)+coalesce(bo.total_shipping,0))/3000 end as gmv
                        
                from  postgres_broadleaf_federate."broadleaf.blc_order"             bo   
                inner join primera_orden                                                 po   ON bo.customer_id = po.customer_id        
                inner join postgres_broadleaf_federate."broadleaf.blc_fulfillment_group" bfg  ON bfg.order_id = bo.order_id
                inner join postgres_broadleaf_federate."broadleaf.blc_fulfillment_order" bfo  ON bfo.fulfillment_group_id = bfg.fulfillment_group_id
                inner join postgres_broadleaf_federate."broadleaf.fb_fulfillment_group"  ffg  ON ffg.fulfillment_group_id = bfg.fulfillment_group_id
                inner join postgres_broadleaf_federate."broadleaf.fb_order"              fo   ON fo.order_id = bo.order_id
                inner join postgres_broadleaf_federate."broadleaf.blc_order_payment"     bop  ON bop.order_id = bo.order_id
                inner join postgres_broadleaf_federate."broadleaf.blc_site"              s    ON s.site_id = bo.site_disc
                inner join postgres_broadleaf_federate."broadleaf.blc_customer"          bc   ON bc.customer_id = bo.customer_id
                left join  postgres_broadleaf_federate."broadleaf.fb_order_type"         fot  ON fot.fb_order_type_id=fo.fb_order_type_id
                and ffg.close_date <= (primer_dia_de_fulfillment + interval '{TIME} {timeline}') 
                and ffg.close_date >= primer_dia_de_fulfillment
                ) 

,ordenes as ( select distinct customer_id
             ,order_id 
            from info_cliente)

,Ordenesxitem as (select distinct boi.order_item_id
                        ,bo.order_id
                        ,bo.customer_id

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
                            inner join ordenes                                                       po   ON bo.order_id = po.order_id
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

                    ,sum(discount_applied) as total_discount
                    ,sum(gmv_item) as gmv_subtotal_item
                    ,sum(volumen_kg) as size_order_kg_noreal
                    
                    ,count(distinct order_item_id)    as numero_lineas
                    ,count(distinct sub_category_id)  as numero_subcategorias
                    ,count(distinct category_id_main) as numero_categoria

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

group by customer_id)

select agrupacion_orden.*
,agrupacion_skus.*

from (select distinct  IC.customer_id
,sum(gmv) as gmv_ac
,count(distinct order_id) as numero_ordenes
,sum(gmv)/count(distinct order_id) as gmv_ac_orden
from info_cliente IC
group by customer_id
) as agrupacion_orden

inner join agrupacion_skus  on (agrupacion_skus.customer_id=agrupacion_orden.customer_id)

    """.format(desde=desde,hasta=hasta,TIME=TIME,timeline=timeline)
    
    dataframe = read_connection_data_warehouse.runQuery(query)
    
    return dataframe

################################################################################################################################################################################################################
################################################################################################################################################################################################################

def query_info_first_order_diosito(desde,hasta):
    """
    Esta función extrae la data de la primera orden
    
    """
    query = """
    
With primera_orden as(select a.* 
                            ,fbs.name as segmento
                            --,fc.segmentation_stage
                            from  (select  
                                 distinct bc.customer_id 
                                ,s.site_identifier_value
                                 ,min(bo.order_id) as first_order 
                                ,Extract(Week  from min(ffg.close_date)) as bweek
                                ,Extract(month from min(ffg.close_date)) as bmonth
                                ,Extract(Year  from min(ffg.close_date)) as byear
                                ,min(ffg.close_date) as primer_dia_de_fulfillment
                                ,min(bo.date_created) as date_first_order 
                                        
                                FROM postgres_broadleaf_federate."broadleaf.blc_order"  bo 
                                inner join postgres_broadleaf_federate."broadleaf.blc_fulfillment_group" bfg  ON bfg.order_id = bo.order_id
                                inner join postgres_broadleaf_federate."broadleaf.blc_fulfillment_order" bfo  ON bfo.fulfillment_group_id = bfg.fulfillment_group_id
                                inner join postgres_broadleaf_federate."broadleaf.fb_fulfillment_group"  ffg  ON ffg.fulfillment_group_id = bfg.fulfillment_group_id
                                inner join postgres_broadleaf_federate."broadleaf.fb_order"              fo   ON fo.order_id = bo.order_id
                                inner join postgres_broadleaf_federate."broadleaf.blc_order_payment"     bop  ON bop.order_id = bo.order_id
                                inner join postgres_broadleaf_federate."broadleaf.blc_site"              s    ON s.site_id = bo.site_disc
                                inner join postgres_broadleaf_federate."broadleaf.blc_customer"          bc   ON bc.customer_id = bo.customer_id
                                left join  postgres_broadleaf_federate."broadleaf.fb_order_type"         fot  ON fot.fb_order_type_id=fo.fb_order_type_id

                                WHERE fo.fb_order_status_id IN (1,6,7,8)
                                    AND bo.order_status = 'SUBMITTED'
                                    AND bfo.status NOT IN ('ARCHIVED','CANCELLED')
                                    AND bop.archived = 'N'
                                    AND (fot.name IS NULL OR fot.name <> 'REFUND')

                                group by 1,2
                               ) as a

                                left join  postgres_broadleaf_federate."broadleaf.fb_customer"           fc   ON fc.customer_id = a.customer_id
                                left join  postgres_broadleaf_federate."broadleaf.fb_business_type"      fbs  ON fc.business_type_id = fbs.business_type_id 
                                where  primer_dia_de_fulfillment >= '{desde}'
                                and  primer_dia_de_fulfillment <= '{hasta}'
                                and fbs.name in ('Restaurante')
                                    ) 
                                    
,primera_orden2 as (select  distinct customer_id 
                                ,primer_dia_de_fulfillment
                                from primera_orden)
                                
select distinct customer_id
            
                    , max(compro_w1) as compro_w1
                    , max(compro_w2) as compro_w2
                    , max(compro_w3) as compro_w3
                    , max(compro_w4) as compro_w4

                   from (select distinct primera_orden2.customer_id
                    ,case when (close_date >= primer_dia_de_fulfillment + interval '5 days'  and close_date <= primer_dia_de_fulfillment + interval '20 days') then 1 else 0 end as compro_e_5_20
                    ,case when (close_date >= primer_dia_de_fulfillment + interval '32 days') then 1 else 0 end as compro_m_32
                    ,case when (extract(week from close_date) = (extract(week from primer_dia_de_fulfillment) +1)) and  (extract(year from close_date) = extract(year from primer_dia_de_fulfillment)) then 1 else 0 end as compro_w1
                    ,case when (extract(week from close_date) = (extract(week from primer_dia_de_fulfillment) +2)) and  (extract(year from close_date) = extract(year from primer_dia_de_fulfillment)) then 1 else 0 end as compro_w2
                    ,case when (extract(week from close_date) = (extract(week from primer_dia_de_fulfillment) +3)) and  (extract(year from close_date) = extract(year from primer_dia_de_fulfillment)) then 1 else 0 end as compro_w3
                    ,case when (extract(week from close_date) = (extract(week from primer_dia_de_fulfillment) +4)) and  (extract(year from close_date) = extract(year from primer_dia_de_fulfillment)) then 1 else 0 end as compro_w4
                    ,case when (extract(week from close_date) = (extract(week from primer_dia_de_fulfillment) +5)) and  (extract(year from close_date) = extract(year from primer_dia_de_fulfillment)) then 1 else 0 end as compro_w5
                    ,case when (extract(week from close_date) = (extract(week from primer_dia_de_fulfillment) +6)) and  (extract(year from close_date) = extract(year from primer_dia_de_fulfillment)) then 1 else 0 end as compro_w6
                    ,case when (extract(week from close_date) = (extract(week from primer_dia_de_fulfillment) +7)) and  (extract(year from close_date) = extract(year from primer_dia_de_fulfillment)) then 1 else 0 end as compro_w7
                    ,case when (extract(week from close_date) = (extract(week from primer_dia_de_fulfillment) +8)) and  (extract(year from close_date) = extract(year from primer_dia_de_fulfillment)) then 1 else 0 end as compro_w8
                    ,case when (extract(week from close_date) = (extract(week from primer_dia_de_fulfillment) +9)) and  (extract(year from close_date) = extract(year from primer_dia_de_fulfillment)) then 1 else 0 end as compro_w9
                    
                            
                            FROM postgres_broadleaf_federate."broadleaf.blc_order" bo
                            inner join postgres_broadleaf_federate."broadleaf.blc_fulfillment_group" bfg  ON bfg.order_id = bo.order_id
                            inner join postgres_broadleaf_federate."broadleaf.blc_fulfillment_order" bfo  ON bfo.fulfillment_group_id = bfg.fulfillment_group_id
                            inner join postgres_broadleaf_federate."broadleaf.fb_fulfillment_group"  ffg  ON ffg.fulfillment_group_id = bfg.fulfillment_group_id
                            inner join postgres_broadleaf_federate."broadleaf.fb_order"              fo   ON fo.order_id = bo.order_id
                            inner join postgres_broadleaf_federate."broadleaf.blc_order_payment"     bop  ON bop.order_id = bo.order_id
                            inner join postgres_broadleaf_federate."broadleaf.blc_site"              s    ON s.site_id = bo.site_disc
                            inner join postgres_broadleaf_federate."broadleaf.blc_customer"          bc   ON bc.customer_id = bo.customer_id
                            inner join primera_orden2                                           on primera_orden2.customer_id=bc.customer_id
                            left join  postgres_broadleaf_federate."broadleaf.fb_order_type"      AS fot  ON fot.fb_order_type_id=fo.fb_order_type_id
                            left join  postgres_broadleaf_federate."broadleaf.fb_customer"           fc   ON fc.customer_id = bc.customer_id
                            left join  postgres_broadleaf_federate."broadleaf.fb_business_type"      fbs  ON fc.business_type_id = fbs.business_type_id)
                                
                                group by 1
    
    """.format(desde=desde,hasta=hasta)
    
    dataframe = read_connection_data_warehouse.runQuery(query)
    
    return dataframe

################################################################################################################################################################################################################
################################################################################################################################################################################################################

def query_base_formato(desde,hasta):
    """
    Esta función extrae la data de la primera orden
    
    """
    query = """
    """.format(desde=desde,hasta=hasta)
    
    dataframe = read_connection_data_warehouse.runQuery(query)
    
    return dataframe

################################################################################################################################################################################################################
################################################################################################################################################################################################################

def query_check_segmentos():
    """
    Esta función extrae la data de la primera orden
    
    """
    query = """
    select distinct customer_id, business_type_id,segmentation_stage,country_code,first_commercial from postgres_broadleaf_federate."broadleaf.fb_customer"
    """
    
    dataframe = read_connection_data_warehouse.runQuery(query)
    
    return dataframe