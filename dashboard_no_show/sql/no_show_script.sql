-- ФИЛЬТРЫ

-- дата начала в формате 'дд.мм.гггг'
set session vars.date_start = '04.09.2025';
-- дата окончания в формате 'дд.мм.гггг'
set session vars.date_end = '04.09.2025';
-- вид оплаты через запятую: 'cash' для нала, 'dms' для ДМС, пустая строка  - неважно
set session vars.pay_types = 'cash, dms';
-- МО: 1 - Питер, 2 - Казань, 0  - неважно
set session vars.mo = 0;
-- признак дмс для ОЗ: true - да, false - нет, пустая строка  - неважно
set session vars.is_online_dms = '';
-- список oid'ов статусов назначений, которые считаются выполненными, через запятую
-- чтобы достать актуальные oid'ы статусов назначений:
-- select id, name from mir.presc_state;
set session vars.presc_states = 'sign, done_lab';
-- список исключений по oid'ам корневых папок назначений через запятую, пустая строка - без исключений
-- чтобы достать актуальные oid'ы корневых папок назначений
-- select id, name from mir.presctypefolder
-- where parent_id is null
-- and del = 0
set session vars.exclude_presctype_folders = '';
-- set session vars.exclude_presctype_folders = '6ac1cf80-2328-11e1-9ccb-37cdd850d0f3, 9ab84a00-2328-11e1-8575-afce54318779';
-- список исключений по oid'ам типов назначений, пустая строка - без исключений
-- хранятся в табличке mir.presctype
set session vars.exclude_presctypes = '';
-- set session vars.exclude_presctypes = '5b0a4da3-bfa8-4df8-990b-c08efc172bd3, a59e66ab-95be-430c-a25e-c71a49626847';
-- исключить назначения не имеющие цену у услуги или цену 0: true - да, false - нет
-- set session vars.exclude_no_price = false; // ПОКА НЕ РАБОТАЕТ
-- исключить расписания созданные на кабинет: true - да, false - нет
set session vars.exclude_cabinet = true;

-- СКРИПТ
with _filters as (
    select to_date(current_setting('vars.date_start'), 'dd.mm.yyyy')                                 as date_start,
           to_date(current_setting('vars.date_end'), 'dd.mm.yyyy')                                   as date_end,
           string_to_array(replace(current_setting('vars.pay_types'), ' ', ''), ',')                 as pay_types,
           nullif(current_setting('vars.mo'), '')::integer                                           as mo,
           nullif(current_setting('vars.is_online_dms'), '')::bool                                   as is_online_dms,
           string_to_array(replace(current_setting('vars.presc_states'), ' ', ''), ',')              as presc_states,
           string_to_array(replace(current_setting('vars.exclude_presctype_folders'), ' ', ''), ',') as exclude_presctype_folders,
           string_to_array(replace(current_setting('vars.exclude_presctypes'), ' ', ''), ',')        as exclude_presctypes,
--            current_setting('vars.exclude_no_price')::bool                                            as exclude_no_price,
           current_setting('vars.exclude_cabinet')::bool                                             as exclude_cabinet
),
     _presc_info as (
         select distinct sl.operator                       as sotr_oid,
                         p.id                              as presc_oid,
                         s.lpu                             as lpu_oid,
                         coalesce(s.sysuser, sotr.sysuser) as doc_sysuser_oid,
                         p.presc_state_id                  as presc_state,
                         pt.oid                            as presctype_oid,
                         sl.operation_date_time            as create_date_time,
                         s.starton                         as schedule_date_time,
                         ppl.oid                           as people_oid,
                         mdoc.id                           as mdoc_oid,
                         pi.auto_notification              as auto_notification,
                         ppl.account_id                    as account_id
         from mir.schedule s
                  join mir.schedule_log sl on sl.schedule = s.oid and sl.action = 1 and s.blockreason isnull
                  join mir.presc_schedule ps on sl.presc_schedule = ps.id
                  join mir.presc p on ps.presc_id = p.id
                  join mir.mdoc mdoc on p.mdoc_id = mdoc.id
                  join mir.people ppl on mdoc.people_id = ppl.oid
                  left join mir.pinfo pi on mdoc.people_id = pi.oid
                  join mir.visit v on p.visit_id = v.id
                  join mir.presctype pt on sl.presctype = pt.oid
                  join mir.presctypefolder ptf on pt.presctypefolderid_parent = ptf.id
                  left join mir.presc_service pse on p.id = pse.presc
                  left join mir.service_presctype sep on pse.service_presctype = sep.oid
                  left join mir.sotr sotr on s.sotr = sotr.oid
                  cross join lateral (select * from _filters) as f
         where case when f.exclude_cabinet is true then s.work_mode not like 'WorkTimeCabinet' else true end
--            and case
--                    when f.exclude_no_price is true then pse.oid is not null and mir.get_price_by_presc(p.id, pse.oid, sep.service) > 0
--                    else true end
           and case
                   when array_length(f.pay_types, 1) > 0 and p.id is not null then v.pay_type_id = any (f.pay_types)
                   else true end
           and case
                   when mo != 0 then
                       case
--                                                              'ООО "АВА-ПЕТЕР"',                      'Общество с ограниченной ответственностью НМЦ-Томография'
                           when mo = 1 then s.lpu = any (array ['ed62552d-b5c3-4ef2-a37d-a35926887a95', '5f8ad894-9bbe-49d4-81c8-3c14a2df9fa3'])
--                                                              'АО "АВА-Казань"',                      'ООО «НМЦ-Томография» филиал'
                           when mo = 2 then s.lpu = any (array ['0e0c9165-3956-4944-857c-e22df96c677c', '7cc2d104-9d12-434d-8d79-0ff1823bf437'])
                           else true
                           end
                   else true
             end
           and case
                   when f.is_online_dms is not null then
                       case
                           when f.is_online_dms is true then sl.commentary like '%ДМС%'
                           when f.is_online_dms is false then sl.commentary not like '%ДМС%'
                           end
                   else true
             end
           and case
                   when array_length(f.exclude_presctype_folders, 1) > 0 then not pt.presctypefolderid_parent = any (f.exclude_presctype_folders)
                   else true end
           and case
                   when array_length(f.exclude_presctypes, 1) > 0 then not pt.oid = any (f.exclude_presctypes)
                   else true end
           and sl.operation_date_time < s.starton
           and s.starton::date between f.date_start and f.date_end
     ),
     _alt_signed_prescs as (
         select distinct pi.sotr_oid  as sotr_oid,
                         pi.presc_oid as original_presc_oid
         from _presc_info pi
                  join mir.presc p on pi.mdoc_oid = p.mdoc_id and p.create_dt::date = pi.schedule_date_time::date
                  join mir.sotr s on p.creator_id = s.oid
                  left join mir.presc_schedule ps on p.id = ps.presc_id
                  cross join lateral (select * from _filters) as f
         where p.presc_state_id = any (f.presc_states)
           and p.create_dt > pi.schedule_date_time
           and s.sysuser = pi.doc_sysuser_oid
           and ps.id is null
     ),
     _people_info as (
         select pi.people_oid,
                count(pi.presc_oid) as                                              schedule_count,
                count(case when p.presc_state_id = any (f.presc_states) then 1 end) signed_count
         from _presc_info pi
                  join mir.mdoc mdoc on mdoc.people_id = pi.people_oid
                  left join mir.presc p on p.mdoc_id = mdoc.id and p.id not in (select presc_oid from _presc_info)
                  cross join lateral (select * from _filters) as f
         group by pi.people_oid
     ),
     _presc_pivot as (
         select to_char(pi.schedule_date_time, 'yyyy-mm-dd')                                                          as schedule_date,
                pi.sotr_oid                                                                                        as sotr_oid,
                pi.presc_oid                                                                                       as presc_oid,
                pi.lpu_oid                                                                                         as lpu_oid,
                case
                    when pi.presc_oid = any (select original_presc_oid from _alt_signed_prescs) then true
                    else pi.presc_state = any (f.presc_states)
                    end                                                                                            as is_signed,
                pi.presc_state,
                pi.presctype_oid                                                                                   as presctype_oid,
                round((extract(epoch from (pi.schedule_date_time - pi.create_date_time)) / 3600 / 24)::numeric, 2) as depth,
                pi.account_id is not null                                                                          as has_lk,
                case
                    when pi.auto_notification is null then false
                    else pi.auto_notification
                    end                                                                                            as auto_notification,
                ppli.signed_count = 0                                                                              as is_first_visit
         from _presc_info pi
                  join _people_info ppli on pi.people_oid = ppli.people_oid
                  cross join lateral (select * from _filters) as f
     ),
     _totals as (
         select pv.schedule_date                                                                                             as schedule_date,
                lg.fullname                                                                                                  as lpu,
                array_agg(distinct p.name)                                                                                   as post_names,
                mir.people_get_peplfio(s.sysuser)                                                                            as doctor_name,
                count(pv.presc_oid)                                                                                          as total,
                count(case when pv.is_signed is True then 1 end)                                                             as signed,
                count(case when pv.is_signed is False then 1 end)                                                            as no_show,
                coalesce(round((avg(case when pv.presc_state = any (f.presc_states) then pv.depth end))::numeric, 2), 0)     as signed_depth,
                count(case when pv.presc_state = any (f.presc_states) and pv.auto_notification is true then 1 end)           as signed_auto,
                count(case when pv.presc_state = any (f.presc_states) and pv.has_lk is true then 1 end)                      as signed_has_lk,
                count(case when pv.presc_state = any (f.presc_states) and pv.is_first_visit is true then 1 end)              as signed_is_first_visit,
                coalesce(round((avg(case when not pv.presc_state = any (f.presc_states) then pv.depth end))::numeric, 2), 0) as no_show_depth,
                count(case when not pv.presc_state = any (f.presc_states) and pv.auto_notification is true then 1 end)       as no_show_auto,
                count(case when not pv.presc_state = any (f.presc_states) and pv.has_lk is true then 1 end)                  as no_show_has_lk,
                count(case when not pv.presc_state = any (f.presc_states) and pv.is_first_visit is true then 1 end)          as no_show_is_first_visit
         from _presc_pivot pv
                  join mir.sotr s on s.oid = pv.sotr_oid
                  join mir.lpu_geo lg on pv.lpu_oid = lg.oid
                  join mir.post p on s.post = p.oid
                  cross join lateral (select * from _filters) as f
         group by pv.schedule_date, lg.fullname, mir.people_get_peplfio(s.sysuser)
         order by schedule_date desc
     )

select to_char(to_date(schedule_date, 'yyyy-mm-dd'), 'yyyy-mm-dd')                                                                            as date,
       lpu,
       case
           when
               lower(cast(post_names as text)) like '%медицинский регистратор%'
               or lower(cast(post_names as text)) like '%самозапись%'
           then 'Онлайн-запись'
           when
               lower(cast(post_names as text)) like '%врач%'
           then 'врач'
           when
               lower(cast(post_names as text)) like '%оператор%'
           then 'оператор'
           when
               lower(cast(post_names as text)) like '%администратор%'
           then 'администратор'
           when
               lower(cast(post_names as text)) like '%сестра%'
               or lower(cast(post_names as text)) like '%брат%'
           then 'медсестра/брат'
           when
               lower(cast(post_names as text)) like '%менеджер%'
           then 'менеджер'
           when
               lower(cast(post_names as text)) like '%лаборант%'
           then 'лаборант'
           when
               lower(cast(post_names as text)) like '%администратор%'
           then 'администратор'
           when
               lower(cast(post_names as text)) like '%акушер%'
           then 'акушер'
           else 'остальные'
       end as key_post_name,
       post_names,
       doctor_name,
       total,
       signed                                                                                                                           as signed_count,
       replace(round((case when total > 0 then signed::numeric / total::numeric else 0 end), 2)::varchar, '.', ',')                     as signed_pct,
       no_show                                                                                                                          as no_show_count,
       replace(round((case when total > 0 then no_show::numeric / total::numeric else 0 end), 2)::varchar, '.', ',')                    as no_show_pct,
       replace(signed_depth::varchar, '.', ',')                                                                                         as signed_depth_in_days,
       replace(round((case when signed > 0 then signed_auto::numeric / signed::numeric else 0 end), 2)::varchar, '.', ',')              as signed_auto_pct,
       replace(round((case when signed > 0 then signed_has_lk::numeric / signed::numeric else 0 end), 2)::varchar, '.', ',')            as signed_has_lk_pct,
       replace(round((case when signed > 0 then signed_is_first_visit::numeric / signed::numeric else 0 end), 2)::varchar, '.', ',')    as signed_is_first_pct,
       replace(no_show_depth::varchar, '.', ',')                                                                                        as no_show_depth_in_days,
       replace(round((case when no_show > 0 then no_show_auto::numeric / no_show::numeric else 0 end), 2)::varchar, '.', ',')           as no_show_auto_pct,
       replace(round((case when no_show > 0 then no_show_has_lk::numeric / no_show::numeric else 0 end), 2)::varchar, '.', ',')         as no_show_has_lk_pct,
       replace(round((case when no_show > 0 then no_show_is_first_visit::numeric / no_show::numeric else 0 end), 2)::varchar, '.', ',') as no_show_is_first_pct
from _totals;
