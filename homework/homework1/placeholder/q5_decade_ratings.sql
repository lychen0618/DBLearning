select 
cast(premiered/10*10 as VARCHAR) || "s" as decade, 
round(avg(rating), 2) as avg_rating,
max(rating),
min(rating),
count(*)
from titles
inner join ratings
on titles.title_id=ratings.title_id 
where premiered is not null
group by decade 
order by avg_rating desc, decade;