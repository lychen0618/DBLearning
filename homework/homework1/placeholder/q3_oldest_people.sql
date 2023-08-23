select name,2022-born 
from people where born>=1900 and died is null 
order by born,name 
limit 20;