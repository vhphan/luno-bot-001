DELETE
FROM one o
    USING
(
    SELECT o2.id
    FROM one o2
             LEFT JOIN two t ON t.one_id = o2.id
    WHERE t.one_id IS NULL
)
sq
WHERE sq.id = o.id;