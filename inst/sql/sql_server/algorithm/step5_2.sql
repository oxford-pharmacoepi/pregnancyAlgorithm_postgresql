-- validate ectopic pregnancy, surgery or MTX or disporop concept within 2 weeks

-- check if ECT has surgery within 2 weeks
-- Insert into #FirstOutcomeEventSurg1
SELECT person_id, event_id, EPISODE_END_DATE_REVISED, date_difference
INTO #FirstOutcomeEventSurg1
FROM
    (
        SELECT
            a.person_id,
            a.event_id,
            sp.event_date AS EPISODE_END_DATE_REVISED,
            EXTRACT(DAY FROM sp.event_date::timestamp - foe.EVENT_DATE::timestamp) AS date_difference,
            ROW_NUMBER() OVER (PARTITION BY a.person_id, a.event_id ORDER BY sp.event_date DESC) AS rn
        FROM
            @resultsDatabaseSchema.FirstOutcomeEvent a
        JOIN
            #pregnancy_events foe ON foe.person_id = a.person_id AND foe.EVENT_ID = a.EVENT_ID
        JOIN
            #pregnancy_events sp ON sp.person_id = a.person_id
        WHERE
            sp.category IN ('ECT_SURG1', 'ECT_SURG2', 'MTX')
            AND EXTRACT(DAY FROM sp.event_date::timestamp - foe.EVENT_DATE::timestamp) >= 0
            AND EXTRACT(DAY FROM sp.event_date::timestamp - foe.EVENT_DATE::timestamp) <= 14
    ) b
WHERE
    rn = 1;


-- check if ECT has surgery within 2 weeks for new outcome date- different concept code set
-- Insert into #FirstOutcomeEventSurg2
SELECT person_id, event_id, EPISODE_END_DATE_REVISED, date_difference
INTO #FirstOutcomeEventSurg2
FROM
    (
        SELECT
            a.person_id,
            a.event_id,
            sp.event_date AS EPISODE_END_DATE_REVISED,
            EXTRACT(DAY FROM sp.event_date::timestamp - foe.EVENT_DATE::timestamp) AS date_difference,
            ROW_NUMBER() OVER (PARTITION BY a.person_id, a.event_id ORDER BY sp.event_date DESC) AS rn
        FROM
            @resultsDatabaseSchema.FirstOutcomeEvent a
        JOIN
            #pregnancy_events foe ON foe.person_id = a.person_id AND foe.EVENT_ID = a.EVENT_ID
        JOIN
            #pregnancy_events sp ON sp.person_id = a.person_id
        WHERE
            sp.category IN ('ECT_SURG1', 'MTX')
            AND EXTRACT(DAY FROM sp.event_date::timestamp - foe.EVENT_DATE::timestamp) >= 0
            AND EXTRACT(DAY FROM sp.event_date::timestamp - foe.EVENT_DATE::timestamp) <= 14
    ) b
WHERE
    rn = 1;


-- if so update raw events file to reflect new date

UPDATE #pregnancy_events
SET event_date = Q.episode_end_date_revised
FROM 
(
  select A.person_id, A.event_id, A.episode_end_date_revised 
  from #FirstOutcomeEventSurg2 A
  join #pregnancy_events B on A.person_id = B.person_id
    and A.event_id = B.event_id
) Q
where #pregnancy_events.person_id = Q.person_id
 and #pregnancy_events.event_id = Q.event_id
;

  
--WHERE #FirstOutcomeEventSurg2.person_id = #pregnancy_events.person_id
--  and #FirstOutcomeEventSurg2.event_id = #pregnancy_events.event_id;


-- check if ECT is a rule-out diagnosis if there are active preg codes up to 6 weeks after
-- Insert into #FirstOutcomeEventInv
SELECT a.person_id, a.event_id
INTO #FirstOutcomeEventInv
FROM @resultsDatabaseSchema.FirstOutcomeEvent a
JOIN
    #pregnancy_events foe ON foe.person_id = a.person_id AND foe.EVENT_ID = a.EVENT_ID
JOIN
    #pregnancy_events sp ON sp.person_id = a.person_id
WHERE
    sp.category IN ('AGP', 'PCONF')
    AND EXTRACT(DAY FROM sp.event_date::timestamp - foe.EVENT_DATE::timestamp) > 0
    AND EXTRACT(DAY FROM sp.event_date::timestamp - foe.EVENT_DATE::timestamp) <= 42;


with ctePriorOutcomes as (
	select pe.person_id, pe.event_id,
		case when pe.event_date <= foe.event_date then 1 else 0 end as prior
	FROM #ValidOutcomes e
	JOIN #pregnancy_events pe on pe.EVENT_ID = e.EVENT_ID and pe.person_id = e.person_id
	JOIN @resultsDatabaseSchema.FirstOutcomeEvent fo on fo.PERSON_ID = pe.PERSON_ID
	JOIN #pregnancy_events foe on foe.person_id = fo.person_id and foe.EVENT_ID = fo.EVENT_ID
),
cteInvalidOutcomes as
(
	select fo.person_id, fo.event_id
	FROM #ValidOutcomes e
	JOIN #pregnancy_events pe on pe.EVENT_ID = e.EVENT_ID and pe.person_id = e.person_id
	JOIN @resultsDatabaseSchema.FirstOutcomeEvent fo on fo.PERSON_ID = pe.PERSON_ID
	JOIN #pregnancy_events foe on foe.EVENT_ID = fo.EVENT_ID and foe.person_id = fo.person_id
	JOIN ctePriorOutcomes po on po.event_id=pe.event_id and po.person_id = pe.person_id
	JOIN @resultsDatabaseSchema.outcome_limit o1 on o1.FIRST_PREG_CATEGORY = foe.Category AND o1.OUTCOME_PREG_CATEGORY = pe.Category
	JOIN @resultsDatabaseSchema.outcome_limit o2 on o2.FIRST_PREG_CATEGORY = pe.Category AND o2.OUTCOME_PREG_CATEGORY = foe.Category
WHERE (ABS(EXTRACT(DAY FROM foe.EVENT_DATE::timestamp - pe.EVENT_DATE::timestamp) + 1) < o2.MIN_DAYS AND prior = 1)
    OR (ABS(EXTRACT(DAY FROM foe.EVENT_DATE::timestamp - pe.EVENT_DATE::timestamp) + 1) < o1.MIN_DAYS AND prior = 0)
)
select a.person_id, a.event_id
INTO #temp_ValidOutcomes
from @resultsDatabaseSchema.FirstOutcomeEvent a
left join cteInvalidOutcomes b on a.person_id = b.person_id and a.event_id=b.event_id
left join #FirstOutcomeEventInv c on a.person_id = c.person_id and a.event_id=c.EVENT_ID
left join #FirstOutcomeEventSurg1 d on a.person_id = d.person_id and a.event_id=d.event_id
where b.event_id is null and c.EVENT_ID is null and d.event_id is not null;

INSERT INTO #ValidOutcomes
select PERSON_ID, EVENT_ID from #temp_ValidOutcomes;

DROP TABLE #temp_ValidOutcomes;

with cteTargetPeople (person_id) as
(
  select distinct e.person_id
  from #PregnancyEvents e
  join #pregnancy_events pe on e.person_id = pe.person_id and e.event_id = pe.event_id
  where pe.category = 'ECT'
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

TRUNCATE TABLE #FirstOutcomeEventSurg1;
DROP TABLE #FirstOutcomeEventSurg1;

TRUNCATE TABLE #FirstOutcomeEventSurg2;
DROP TABLE #FirstOutcomeEventSurg2;
