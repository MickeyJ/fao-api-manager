LOAD 'age';

 SET search_path = ag_catalog, public, "$user";

SELECT name, relation::regclass
FROM ag_catalog.ag_label
WHERE graph = 'fao_graph'::regnamespace AND kind = 'e';

// DELETE All Relationships of Type
SELECT * FROM cypher('fao_graph', $$
MATCH ()-[r:PRODUCES]->()
DELETE r
RETURN count(r) AS deleted
$$) AS (deleted agtype);

// Distinct Items in Relationship
SELECT * FROM cypher('fao_graph', $$
MATCH (ac:AreaCode)-[e:EMITS]->(ic:ItemCode)
WITH DISTINCT
ic
RETURN
ic.item_code AS item_code,
ic.item AS item
 ORDER BY ic.item_code DESC
LIMIT 20
$$) AS (
item agtype,
item_code agtype
);

DO $$
DECLARE
r RECORD;

BEGIN
FOR r IN
SELECT schemaname, indexname
FROM pg_indexes
WHERE schemaname = 'fao_graph'
 AND indexname LIKE 'idx_%'
LIMIT 10
LOOP
EXECUTE 'DROP INDEX IF EXISTS ' || r.schemaname || '.' || r.indexname;

RAISE NOTICE 'Dropped index: %', r.indexname;

END LOOP;

END $$;

SELECT schemaname, indexname
FROM pg_indexes
WHERE schemaname = 'fao_graph'
 AND indexname LIKE 'idx_%'
