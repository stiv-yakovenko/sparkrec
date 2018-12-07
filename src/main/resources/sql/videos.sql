select
  mf.id                                                                          as media_id,
  main_rubric.rubric_id_matchtv                                                  AS rubric_id,
  concat('/', url.dst)                                                           AS url,
  pics.image_url,
  case when (mf.rights_expire is null or mf.rights_expire > now()) and
            (mf.media_state_code != 2 or (mf.media_state_code = 3 and mf.has_archive)) and n.status = 1
    then 1
  else 0 end                                                                     as is_enabled,
  case when duration._seconds > 0
    then duration._seconds
  else unix_timestamp(coalesce(mf.finish, now())) - unix_timestamp(mf.start) end as duration,
  mf.media_state_code,
  case mf.media_state_code
  when 1
    then 'LIVE'
  when 2
    then 'ANNOUNCE'
  when 3
    then 'ARCHIVE' end                                                              media_state,
  n.nid                                                                          as node_id,
  from_unixtime(n.created)                                                       as created,
  from_unixtime(n.changed)                                                       as changed,
  n.type,
  n.title,
  matchtv_terms.matchtv_url                                                      as menu_url,
  term_data_main_rubric.name                                                     as menu_name
from spb_movie_fields mf left join (select
                                      media_id,
                                      max(file_duration_seconds) as _seconds
                                    from vdl_content_items
                                    group by media_id) duration on duration.media_id = mf.id
  join node n on n.nid = mf.nid
  join node_revisions nr on nr.nid = n.nid
  join node_site_owner nso on nso.node_id = n.nid and nso.site_owner_id = 2
  join url_alias url on url.src = concat('node/', n.nid) and url.site_owner_id = 2
  left join (select
               fn.node_id                                                                         as node_id,
               concat('https://s-cdn.sportbox.ru/images/styles/537_269/', f.image_path_in_images) as image_url
             from fotoplatform_fotos f
               join fotoplatform_foto_node fn on fn.foto_id = f.id
             group by 1) as pics on pics.node_id = n.nid
  left join content_field_main_rubric_2 main_rubric on main_rubric.nid = n.nid
  left join term_data term_data_main_rubric on term_data_main_rubric.tid = main_rubric.rubric_id_matchtv
  left join matchtv_terms on matchtv_terms.term_id = main_rubric.rubric_id_matchtv
order by 1 desc