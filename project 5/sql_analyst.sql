
-- Сколько заявок приходило каждый день в июне 2022 года
 
 
select created_datetime::date as day
		,count(*)
from public.tasks
where date_trunc(‘month’, created_datetime) = ‘2022-06-01’
group by created_datetime::date
 
 
-- Список тем, для которых обращений было больше 10 в апреле 2022 года
 
 
select title
        ,count(*) 
from public.tasks
where date_trunc('month', created_datetime) = '04-01-2022' 
group by title
having count(*) > 10
 

-- Список клиентов, которые оставляли заявку, но ни одного звонка от менеджера по ним не было
 
select distinct client_id
from public.tasks t
left join public.calls c
on c.client_id = t.client_id
where c.client_id is null
 

-- Для каждого клиента выведите три его последних обращения и постройте распределение количества этих обращений по теме
 
WITH tasks_rn as (
select *
    ,row_number() over (partition by client_id order by created_datetime desc) as rn
from public.tasks
) 
Select title
		,count(*)
From tasks_rn
where rn < 4 
Group by title
 
 
-- Для каждой темы обращения найдите среднее время, которое проходит между клиентскими обращениями 
 
select title
    ,avg(delta_time)
from
(select title
    ,created_datetime
    ,created_datetime - lag(created_datetime) over (partition by title order by created_datetime) as delta_time
from public.tasks) a 
group by title
