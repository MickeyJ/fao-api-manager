CREATE OR REPLACE FUNCTION generate_numeric_id(hash_values text[]) 
RETURNS INTEGER AS $$
DECLARE
    content text;
    hash_bytes bytea;
    numeric_id bigint;
BEGIN
    -- Join values with '|' separator
    content := array_to_string(hash_values, '|');
    
    -- Generate MD5 hash
    hash_bytes := decode(md5(content), 'hex');
    
    -- Convert first 8 bytes to bigint (big-endian)
    numeric_id := 
        (get_byte(hash_bytes, 0)::bigint << 56) +
        (get_byte(hash_bytes, 1)::bigint << 48) +
        (get_byte(hash_bytes, 2)::bigint << 40) +
        (get_byte(hash_bytes, 3)::bigint << 32) +
        (get_byte(hash_bytes, 4)::bigint << 24) +
        (get_byte(hash_bytes, 5)::bigint << 16) +
        (get_byte(hash_bytes, 6)::bigint << 8) +
        (get_byte(hash_bytes, 7)::bigint);
    
    -- Ensure it fits in PostgreSQL INTEGER type
    RETURN numeric_id % 2147483647;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- SET work_mem = '1GB';

-- UPDATE trade_detailed_trade_matrix 
-- SET reporter_country_code_id = generate_numeric_id(ARRAY[reporter_country_code, 'trade_detailed_trade_matrix']::text[]),
--     partner_country_code_id = generate_numeric_id(ARRAY[partner_country_code, 'trade_detailed_trade_matrix']::text[]);


-- UPDATE fertilizers_detailed_trade_matrix 
-- SET reporter_country_code_id = generate_numeric_id(ARRAY[reporter_country_code, 'fertilizers_detailed_trade_matrix']::text[]),
--     partner_country_code_id = generate_numeric_id(ARRAY[partner_country_code, 'fertilizers_detailed_trade_matrix']::text[]);
	
-- UPDATE forestry_trade_flows 
-- SET reporter_country_code_id = generate_numeric_id(ARRAY[reporter_country_code, 'forestry_trade_flows']::text[]),
--     partner_country_code_id = generate_numeric_id(ARRAY[partner_country_code, 'forestry_trade_flows']::text[]);

-- UPDATE development_assistance_to_agriculture 
-- SET recipient_country_code_id = generate_numeric_id(
--     ARRAY[recipient_country_code, 'development_assistance_to_agriculture']::text[]
-- );

-- UPDATE food_aid_shipments_wfp 
-- SET recipient_country_code_id = generate_numeric_id(
--     ARRAY[recipient_country_code, 'food_aid_shipments_wfp']::text[]
-- );


-- CREATE INDEX ix_development_assistance_to_agriculture_recipient_coun_b110 ON public.development_assistance_to_agriculture USING btree (recipient_country_code_id);
-- CREATE INDEX ix_fertilizers_detailed_trade_matrix_partner_country_code_id ON public.fertilizers_detailed_trade_matrix USING btree (partner_country_code_id);
-- CREATE INDEX ix_fertilizers_detailed_trade_matrix_reporter_country_code_id ON public.fertilizers_detailed_trade_matrix USING btree (reporter_country_code_id);
-- CREATE INDEX ix_food_aid_shipments_wfp_recipient_country_code_id ON public.food_aid_shipments_wfp USING btree (recipient_country_code_id);
-- CREATE INDEX ix_forestry_trade_flows_partner_country_code_id ON public.forestry_trade_flows USING btree (partner_country_code_id);  
-- CREATE INDEX ix_forestry_trade_flows_reporter_country_code_id ON public.forestry_trade_flows USING btree (reporter_country_code_id);CREATE INDEX ix_trade_detailed_trade_matrix_partner_country_code_id ON public.trade_detailed_trade_matrix USING btree (partner_country_code_id);
-- CREATE INDEX ix_trade_detailed_trade_matrix_reporter_country_code_id ON public.trade_detailed_trade_matrix USING btree (reporter_country_code_id);
-- alter table "public"."development_assistance_to_agriculture" add constraint "development_assistance_to_agricu_recipient_country_code_id_fkey" FOREIGN KEY (recipient_country_code_id) REFERENCES recipient_country_codes(id) not valid;
-- alter table "public"."development_assistance_to_agriculture" validate constraint "development_assistance_to_agricu_recipient_country_code_id_fkey";
-- alter table "public"."fertilizers_detailed_trade_matrix" add constraint "fertilizers_detailed_trade_matrix_partner_country_code_id_fkey" FOREIGN KEY (partner_country_code_id) REFERENCES partner_country_codes(id) not valid;
-- alter table "public"."fertilizers_detailed_trade_matrix" validate constraint "fertilizers_detailed_trade_matrix_partner_country_code_id_fkey";
-- alter table "public"."fertilizers_detailed_trade_matrix" add constraint "fertilizers_detailed_trade_matrix_reporter_country_code_id_fkey" FOREIGN KEY (reporter_country_code_id) REFERENCES reporter_country_codes(id) not valid;
-- alter table "public"."fertilizers_detailed_trade_matrix" validate constraint "fertilizers_detailed_trade_matrix_reporter_country_code_id_fkey";
-- alter table "public"."food_aid_shipments_wfp" add constraint "food_aid_shipments_wfp_recipient_country_code_id_fkey" FOREIGN KEY (recipient_country_code_id) REFERENCES recipient_country_codes(id) not valid;
-- alter table "public"."food_aid_shipments_wfp" validate constraint "food_aid_shipments_wfp_recipient_country_code_id_fkey";
-- alter table "public"."forestry_trade_flows" add constraint "forestry_trade_flows_partner_country_code_id_fkey" FOREIGN KEY (partner_country_code_id) REFERENCES partner_country_codes(id) not valid;
-- alter table "public"."forestry_trade_flows" validate constraint "forestry_trade_flows_partner_country_code_id_fkey";
-- alter table "public"."forestry_trade_flows" add constraint "forestry_trade_flows_reporter_country_code_id_fkey" FOREIGN KEY (reporter_country_code_id) REFERENCES reporter_country_codes(id) not valid;
-- alter table "public"."forestry_trade_flows" validate constraint "forestry_trade_flows_reporter_country_code_id_fkey";
-- alter table "public"."trade_detailed_trade_matrix" add constraint "trade_detailed_trade_matrix_partner_country_code_id_fkey" FOREIGN KEY (partner_country_code_id) REFERENCES partner_country_codes(id) not valid;
-- alter table "public"."trade_detailed_trade_matrix" validate constraint "trade_detailed_trade_matrix_partner_country_code_id_fkey";  
-- alter table "public"."trade_detailed_trade_matrix" add constraint "trade_detailed_trade_matrix_reporter_country_code_id_fkey" FOREIGN KEY (reporter_country_code_id) REFERENCES reporter_country_codes(id) not valid;
-- alter table "public"."trade_detailed_trade_matrix" validate constraint "trade_detailed_trade_matrix_reporter_country_code_id_fkey";