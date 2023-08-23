with involve as (
    select title_id from crew
    inner join people
    on people.person_id=crew.person_id
    where name="Nicole Kidman" and born=1967
)
select DISTINCT name from people
inner join crew
on people.person_id=crew.person_id
inner join involve
on crew.title_id=involve.title_id
where category="actress" or category="actor"
order by name;
