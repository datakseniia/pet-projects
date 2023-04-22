select sum(cnt_weeks) / (count(*) + sum(cnt_weeks))::float * 100 as ratio_of_missing_weeks
    ,count(*) + sum(cnt_weeks) as cnt_weeks
    ,teacher_id
from
(select case when cnt_weeks > 1 then cnt_weeks-1 else 0 end as cnt_weeks
    ,teacher_id
from
(select
week_number - lag(week_number) over (partition by student_id order by week_number asc) as cnt_weeks
,student_id
,week_number
,teacher_id
from
(select
    dense_rank () over (order by date_trunc('week', lesson_day)) as week_number
    ,student_id
    ,teacher_id
from public.test_lessons) d
group by student_id, week_number, teacher_id
order by student_id, week_number) b) a
group by teacher_id
order by ratio_of_missing_weeks desc
