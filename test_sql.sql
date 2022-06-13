----------------------------------------------------------------------------------------
------------------------------- Create data model --------------------------------------
----------------------------------------------------------------------------------------
DROP SCHEMA IF EXISTS test_oplit CASCADE;
CREATE SCHEMA test_oplit;

CREATE TABLE IF NOT EXISTS test_oplit.calendar (
        id serial PRIMARY KEY,
        calendar_date date NOT NULL);
        
CREATE TABLE IF NOT EXISTS test_oplit.import (
        id int PRIMARY KEY,
        enabled boolean NOT NULL,
        created timestamp DEFAULT NOW() NOT NULL,
        import_date date NOT NULL);
        
CREATE TABLE IF NOT EXISTS test_oplit.production (
        id serial PRIMARY KEY,
        enabled boolean NOT NULL,
        created timestamp DEFAULT NOW() NOT NULL,
        machine_id int NOT NULL,
        amount int DEFAULT 0 NOT NULL,
        import_id int NOT NULL,
        FOREIGN KEY (import_id) REFERENCES test_oplit.import (id)
        );


----------------------------------------------------------------------------------------
-------------------------------  Populate model ----------------------------------------
----------------------------------------------------------------------------------------
INSERT INTO test_oplit.calendar(calendar_date) (
        SELECT date_trunc('day', dd):: date
        FROM generate_series
                ( '2022-04-01'::timestamp 
                , '2022-05-01'::timestamp
                , '1 day'::interval
                ) as gs(dd)
);

INSERT INTO test_oplit.import(enabled, id, import_date) VALUES
        (TRUE, 1, '2022-04-01'),
        (TRUE, 2, '2022-04-02'),
        (TRUE, 3, '2022-04-03'),
        (TRUE, 4, '2022-04-04'),
        (TRUE, 5, '2022-04-05'),
        (TRUE, 6, '2022-04-08'),
        (TRUE, 7, '2022-04-09'),
        (TRUE, 8, '2022-04-10'),
        (TRUE, 9, '2022-04-15')
;

INSERT INTO test_oplit.production(enabled, machine_id, amount, import_id) VALUES
	(TRUE, 32, floor(random() * 100 + 1)::int, 1),
	(TRUE, 44, floor(random() * 100 + 1)::int, 1),
	(TRUE, 78, floor(random() * 100 + 1)::int, 1),
	(TRUE, 156, floor(random() * 100 + 1)::int, 1),
	(TRUE, 454, floor(random() * 100 + 1)::int, 1),
	(TRUE, 32, floor(random() * 100 + 1)::int, 2),
	(TRUE, 78, floor(random() * 100 + 1)::int, 2),
	(TRUE, 454, floor(random() * 100 + 1)::int, 2),
	(TRUE, 32, floor(random() * 100 + 1)::int, 4),
	(TRUE, 44, floor(random() * 100 + 1)::int, 4),
	(TRUE, 78, floor(random() * 100 + 1)::int, 4),
	(TRUE, 32, floor(random() * 100 + 1)::int, 7)
;

----------------------------------------------------------------------------------------
------------------------------ Production calendar -------------------------------------
----------------------------------------------------------------------------------------
CREATE TABLE test_oplit.question1 as (
SELECT 
        c.calendar_date as d,
        p.machine_id as machine_id,
        p.amount as amount
FROM test_oplit.production p 
LEFT JOIN test_oplit.import i ON (
        i.id = p.import_id
)
RIGHT JOIN test_oplit.calendar c ON (
        c.calendar_date = i.import_date
)
ORDER BY d DESC
)
;


----------------------------------------------------------------------------------------
---------------------------- Last production for any machine ---------------------------
----------------------------------------------------------------------------------------
CREATE TABLE test_oplit.question2 as (
	WITH 
	
	all_machine_ids AS (
	        SELECT DISTINCT machine_id as id
	        FROM test_oplit.production
	        WHERE enabled IS TRUE
	),
	
	res AS (
	SELECT 
	        c.calendar_date,
	        m_ids.id as machine_id,
	        p.amount as amount,
	        i.import_date as import_date,
	        RANK() OVER (
	                PARTITION BY (c.calendar_date, m_ids.id) 
	                ORDER BY (i.import_date - c.calendar_date) DESC
	        ) as r
	FROM test_oplit.calendar c
	CROSS JOIN all_machine_ids m_ids
	JOIN test_oplit.production p ON (
	        p.machine_id = m_ids.id
	)
	JOIN test_oplit.import i ON (
	        p.import_id = i.id
	        AND i.import_date <= c.calendar_date
	)
	ORDER BY c.calendar_date DESC, r ASC
	) 
	
	SELECT 
	        calendar_date,
	        ARRAY_AGG('machine: ' || machine_id || ' last production is: ' || amount) production
	FROM res
	WHERE r = 1
	GROUP BY 1
	ORDER BY 1 DESC
);
