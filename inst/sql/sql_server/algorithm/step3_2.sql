-- Create the temporary table
SELECT a.person_id, a.event_id
INTO #FirstOutcomeEventInv
FROM @resultsDatabaseSchema.FirstOutcomeEvent a
JOIN #pregnancy_events foe ON foe.person_id = a.person_id AND foe.EVENT_ID = a.EVENT_ID
JOIN #pregnancy_events sp ON sp.person_id = a.person_id
WHERE sp.category IN ('AGP', 'PCONF') AND (EXTRACT(DAY FROM (foe.EVENT_DATE::timestamp - sp.event_date::timestamp)) + 1) > 0
                                        AND (EXTRACT(DAY FROM (foe.EVENT_DATE::timestamp - sp.event_date::timestamp)) + 1) <= 42;



-- Create Common Table Expressions (CTEs)
WITH ctePriorOutcomes AS (
    SELECT
        pe.person_id,
        pe.event_id,
        CASE WHEN pe.event_date <= foe.event_date THEN 1 ELSE 0 END AS prior
    FROM
        #ValidOutcomes e
        JOIN #pregnancy_events pe ON pe.EVENT_ID = e.EVENT_ID AND pe.person_id = e.person_id
        JOIN @resultsDatabaseSchema.FirstOutcomeEvent fo ON fo.PERSON_ID = pe.PERSON_ID
        JOIN #pregnancy_events foe ON foe.person_id = fo.person_id AND foe.EVENT_ID = fo.EVENT_ID
),
cteInvalidOutcomes AS (
    SELECT
        fo.person_id,
        fo.event_id
    FROM
        #ValidOutcomes e
        JOIN #pregnancy_events pe ON pe.EVENT_ID = e.EVENT_ID AND pe.person_id = e.person_id
        JOIN @resultsDatabaseSchema.FirstOutcomeEvent fo ON fo.PERSON_ID = pe.PERSON_ID
        JOIN #pregnancy_events foe ON foe.EVENT_ID = fo.EVENT_ID AND foe.person_id = fo.person_id
        JOIN ctePriorOutcomes po ON po.event_id = pe.event_id AND po.person_id = pe.person_id
        JOIN @resultsDatabaseSchema.outcome_limit o1 ON o1.FIRST_PREG_CATEGORY = foe.Category AND o1.OUTCOME_PREG_CATEGORY = pe.Category
        JOIN @resultsDatabaseSchema.outcome_limit o2 ON o2.FIRST_PREG_CATEGORY = pe.Category AND o2.OUTCOME_PREG_CATEGORY = foe.Category
    WHERE
        (ABS(EXTRACT(DAY FROM (foe.EVENT_DATE::timestamp - pe.EVENT_DATE::timestamp)) + 1) < o2.MIN_DAYS AND prior = 1)
        OR (ABS(EXTRACT(DAY FROM (foe.EVENT_DATE::timestamp - pe.EVENT_DATE::timestamp)) + 1) < o1.MIN_DAYS AND prior = 0)
)
-- Create the final selection and insertion
SELECT
    a.person_id,
    a.event_id
INTO #temp_ValidOutcomes
FROM
    @resultsDatabaseSchema.FirstOutcomeEvent a
    LEFT JOIN cteInvalidOutcomes b ON a.person_id = b.person_id AND a.event_id = b.event_id
    LEFT JOIN #FirstOutcomeEventInv c ON a.person_id = c.person_id AND a.event_id = c.EVENT_ID
WHERE
    b.event_id IS NULL AND c.EVENT_ID IS NULL;

-- Insert into the final table
INSERT INTO #ValidOutcomes
SELECT
    PERSON_ID,
    EVENT_ID
FROM
    #temp_ValidOutcomes;


DROP TABLE #temp_ValidOutcomes;

with cteTargetPeople (person_id) as
(
  select distinct e.person_id
  from #PregnancyEvents e
  join #pregnancy_events pe on e.person_id = pe.person_id and e.event_id = pe.event_id
  where pe.category = 'SB'
)
select pe.PERSON_ID, pe.EVENT_ID
INTO #temp_PregnancyEvents
FROM #PregnancyEvents pe
join cteTargetPeople p on pe.person_id = p.person_id
left join @resultsDatabaseSchema.FirstOutcomeEvent de on pe.person_id = de.person_id and pe.event_id = de.event_id
where de.person_id is null;

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

TRUNCATE TABLE #FirstOutcomeEventInv;
DROP TABLE #FirstOutcomeEventInv;
