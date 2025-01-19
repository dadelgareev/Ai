DELETE FROM card_row
WHERE ctid NOT IN (
    SELECT MIN(ctid)
    FROM card_row
    GROUP BY guid
);
