-- Delete duplicates, keeping the one with lowest ID
DELETE FROM prices p1
USING prices p2
WHERE p1.id > p2.id
  AND p1.area_code_id = p2.area_code_id
  AND p1.item_code_id = p2.item_code_id
  AND p1.element_code_id = p2.element_code_id
  AND p1.flag_id = p2.flag_id
  AND p1.year = p2.year
  AND p1.months_code = p2.months_code
  AND p1.unit = p2.unit
  AND p1.value = p2.value;