DELETE
FROM Table1
WHERE (address, city, state, zip)
          NOT IN
      (SELECT address, city, state, zip FROM Table2);

INSERT INTO Table1
    (address, city, state, zip)
SELECT address, city, state, zip
FROM Table2
    EXCEPT
SELECT address, city, state, zip
FROM Table1;