SELECT guid, COUNT(*) as duplicated_count
FROM card_row
GROUP BY guid
HAVING COUNT(*) > 1;