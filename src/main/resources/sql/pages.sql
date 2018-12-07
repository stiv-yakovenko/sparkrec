select
    n.nid as id -- айдишка страницы
  ,main_rubric.rubric_id_matchtv as rubric_id
  , case when n.status=1
  then 0
    else 1
    end as is_disabled -- признак "не рекомендовать"
  , from_unixtime(n.created) as created -- создано
  , from_unixtime(n.changed) as changed -- изменено
  , n.type
  , matchtv_terms.matchtv_url as main_rubric_url -- основная рубрика
  , term_data_main_rubric.name as main_rubric_name -- основная рубрика
  , rubrics.rubrics as rubrics_all -- список всех рубрик страницы
  , concat('/', url.dst ) AS url -- url страницы
  , pics.image_url -- url картинки
  , n.title -- заголовок страницы
  , nr.body -- html
  , v20.meta as meta_Клубы
  , v19.meta as meta_Люди
  , v21.meta as meta_Сборные
  , v5.meta as meta_Теги
from node n
  join node_revisions nr on nr.nid = n.nid
  left join content_field_main_rubric_2 main_rubric on main_rubric.nid = n.nid
  left join term_data term_data_main_rubric on term_data_main_rubric.tid =  main_rubric.rubric_id_matchtv
  left join matchtv_terms on matchtv_terms.term_id = main_rubric.rubric_id_matchtv
  join node_site_owner nso on nso.node_id = n.nid and nso.site_owner_id=2
  join url_alias url on url.src = concat( 'node/', n.nid) and url.site_owner_id = 2
  left join
  (
    select tn.node_id as node_id
      , GROUP_CONCAT('"', concat(matchtv_terms.matchtv_url,'":"', td.name,'"') SEPARATOR '; ') as rubrics
    from matchtv_term_node tn
      join matchtv_terms on matchtv_terms.term_id = tn.term_id
      join term_data td on td.tid = matchtv_terms.term_id
    group by 1
  ) as rubrics on rubrics.node_id = n.nid
  left join
  (
    select fn.node_id as node_id
      , concat('https://s-cdn.sportbox.ru/images/styles/537_269/', f.image_path_in_images) as image_url
    from fotoplatform_fotos f
      join fotoplatform_foto_node fn on fn.foto_id = f.id
    -- join node n on n.nid = fn.node_id
    -- where fn.node_id = 729519
    group by 1
  ) as pics on pics.node_id = n.nid
  left join
  (
    select tn.nid as node_id, GROUP_CONCAT(td.name SEPARATOR '; ') as meta
    from term_node tn -- on tn.nid = n.node_id
      left join term_data td on td.tid = tn.tid
    where td.vid=20 -- Клубы
    group by 1
  ) as v20 on v20.node_id = n.nid
  left join
  (
    select tn.nid as node_id, GROUP_CONCAT(td.name SEPARATOR '; ') as meta
    from term_node tn -- on tn.nid = n.node_id
      left join term_data td on td.tid = tn.tid
    where td.vid=19 -- Люди
    group by 1
  ) as v19 on v19.node_id = n.nid
  left join
  (
    select tn.nid as node_id, GROUP_CONCAT(td.name SEPARATOR '; ') as meta
    from newsdb12.term_node tn -- on tn.nid = n.node_id
      left join newsdb12.term_data td on td.tid = tn.tid
    where td.vid=21 -- Сборные
    group by 1
  ) as v21 on v21.node_id = n.nid
  left join
  (
    select tn.nid as node_id, td.name as meta -- GROUP_CONCAT(td.name SEPARATOR ', ')
    from term_node tn -- on tn.nid = n.node_id
      left join term_data td on td.tid = tn.tid
    where td.vid=5 -- Теги
    group by 1
  ) as v5 on v5.node_id = n.nid
where
  not url.dst like '%matchtvvideo_N%' -- matchtvnews_N -- spb_movie_fields.nid is null
order by 1 desc