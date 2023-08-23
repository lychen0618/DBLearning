with army as (
    select premiered from titles
    where original_title="Army of Thieves"
    limit 1
)

select count(*) from 
(select DISTINCT title_id from titles
inner join army
on titles.premiered=army.premiered);