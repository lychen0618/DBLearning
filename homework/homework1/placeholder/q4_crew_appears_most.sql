select name,count(*) as NUM_APPEARANCES 
from people 
inner join crew 
on people.person_id=crew.person_id 
group by name 
order by NUM_APPEARANCES desc 
limit 20;