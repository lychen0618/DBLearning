with pg as (
    select people.person_id,
            people.name,
            titles.title_id,
            titles.primary_title from people
    inner join crew
    on people.person_id=crew.person_id
    inner join titles 
    on crew.title_id = titles.title_id
    where born=1955 and type="movie"
),
act_ratings as (
    select name,round(avg(ratings.rating),2) as rating
    from ratings
    inner join pg
    on ratings.title_id=pg.title_id
    group by pg.person_id
),
quantity as (
    select *,NTILE ( 10 ) over ( 
	order by rating) as percent from act_ratings
)
select name,rating
from quantity
where percent=9
order by rating desc,name;