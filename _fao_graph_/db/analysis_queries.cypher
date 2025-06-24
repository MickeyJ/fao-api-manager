// Petroleum Products Emissions and Usage
SELECT * FROM cypher('fao_graph', $$
MATCH (ac:AreaCode)-[e:EMITS]->(ic:ItemCode)<-[u:USES]-(ac)
WITH u, e, ac, ic
WHERE 1 > 0
// AND e.source_dataset = 'emissions_agriculture_energy'
// AND u.source_dataset = 'emissions_agriculture_energy'
 AND ic.item_code = '6801'
 AND e.value > 14000
RETURN
ac.area,
ic.item_code,
ic.item,
e.element,
e.value,
e.unit,
u.element,
u.value,
u.unit,
e.flag
 ORDER BY ic.item_code DESC
LIMIT 20
$$) AS (
country agtype,
item agtype,
item_code agtype,
emits agtype,
e_value agtype,
e_unit agtype,
uses agtype,
u_value agtype,
u_unit agtype,
flag agtype
);

// Country Exporting more than their domestic supply (healthy food staples)
SELECT * FROM cypher('fao_graph', $$
MATCH (ac:AreaCode { source_dataset: 'food_balance_sheets' })-[t:TRADES]->
(ic:ItemCode { source_dataset: 'food_balance_sheets' })<-[p:PRODUCES]-(ac)
WITH
t, p, ic, ac,
t.value - p.value AS trade_prod_gap
WHERE 1 > 0
 AND NOT ac.area_code =~ "^5[1-5]0[0-7]$"
 AND t.element_code = "5911"
 AND p.element_code = "5301"
 AND t.year = p.year
 AND t.value > p.value
//AND ic.item_code IN ["2918", "2919", "2601", "2908", "2907", "2511", "2577"]
RETURN
t.year,
ac.area,
t.element,
t.value,
t.unit,
p.element,
p.value,
p.unit,
ic.item_code AS item_code,
ic.item AS item,
trade_prod_gap
 ORDER BY trade_prod_gap DESC
LIMIT 100
$$) AS (
year agtype,
country agtype,
t_type agtype,
t_value agtype,
t_unit agtype,
p_type agtype,
p_value agtype,
p_unit agtype,
item agtype,
item_code agtype,
trade_prod_gap agtype
);
