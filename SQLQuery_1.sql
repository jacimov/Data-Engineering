--SELECT id, persona, score FROM events
--WHERE score > 900
--ORDER BY persona

--SELECT DISTINCT persona FROM events
--WHERE score > 900
--ORDER BY persona

--SELECT persona, MAX(latency), avg(score) AS mean_score
--FROM events
--GROUP BY persona
--HAVING MAX(latency) > 575
--ORDER BY mean_score ASC;

--SELECT persona, avg(score) AS mean_score
--FROM events
--GROUP BY persona
--HAVING avg(score) > 575
--ORDER BY mean_score ASC;

--select element, count(element)
--from events
--group by id
-- this is wrong

