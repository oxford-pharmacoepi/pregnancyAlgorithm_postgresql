-- Insert all events into #ValidOutcomes
INSERT INTO #ValidOutcomes (PERSON_ID, EVENT_ID)
SELECT PERSON_ID, EVENT_ID
FROM @resultsDatabaseSchema.FirstOutcomeEvent;

-- Select events into #deletedEvents based on date difference
SELECT e.PERSON_ID, e.EVENT_ID
INTO #deletedEvents
FROM #PregnancyEvents e
JOIN #pregnancy_events pe ON e.PERSON_ID = pe.PERSON_ID AND e.EVENT_ID = pe.EVENT_ID
JOIN @resultsDatabaseSchema.FirstOutcomeEvent fo ON fo.PERSON_ID = pe.PERSON_ID
JOIN #pregnancy_events foe ON foe.person_id = fo.person_id AND foe.EVENT_ID = fo.EVENT_ID
JOIN @resultsDatabaseSchema.outcome_limit ol ON ol.FIRST_PREG_CATEGORY = foe.Category AND ol.OUTCOME_PREG_CATEGORY = pe.Category
WHERE
    (EXTRACT(DAY FROM (foe.EVENT_DATE::timestamp - pe.EVENT_DATE::timestamp)) + 1) < ol.MIN_DAYS
;



with cteTargetPeople (person_id) as
(
  select distinct e.person_id
  from #PregnancyEvents e
  join #pregnancy_events pe on e.person_id = pe.person_id and e.event_id = pe.event_id
  where pe.category = 'LB'
)
select pe.PERSON_ID, pe.EVENT_ID
INTO #temp_PregnancyEvents
FROM #PregnancyEvents pe
join cteTargetPeople p on pe.person_id = p.person_id
left join #deletedEvents de on pe.person_id = de.person_id and pe.event_id = de.event_id
where de.person_id is null;

TRUNCATE TABLE #deletedEvents;
DROP TABLE #deletedEvents;

TRUNCATE TABLE #PregnancyEvents;
DROP TABLE #PregnancyEvents;

SELECT PERSON_ID, EVENT_ID
INTO #PregnancyEvents
from #temp_PregnancyEvents
;

TRUNCATE TABLE #temp_PregnancyEvents;
DROP TABLE #temp_PregnancyEvents;

TRUNCATE TABLE @resultsDatabaseSchema.FirstOutcomeEvent;
DROP TABLE @resultsDatabaseSchema.FirstOutcomeEvent;
