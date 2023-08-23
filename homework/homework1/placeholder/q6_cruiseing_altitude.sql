select original_title,votes from 
    (select title_id from people
    inner join crew
    on people.person_id=crew.person_id
    where name like "%Cruise%" and born = 1962) as com
inner join titles on com.title_id=titles.title_id
inner join ratings on com.title_id=ratings.title_id
order by votes desc 
limit 10;