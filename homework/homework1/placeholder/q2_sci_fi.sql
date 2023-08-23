select primary_title, premiered, cast(runtime_minutes as VARCHAR) || " (mins)" 
from titles 
where genres like "%Sci-Fi%" 
order by runtime_minutes desc 
limit 10;