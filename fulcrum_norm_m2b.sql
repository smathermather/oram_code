--create normalized tables for metric and populate with existing oram records

CREATE TABLE nr.metric2b_norm AS SELECT oram_id, regexp_split_to_table(m2b_surrounding_land_use, ',')::text AS selection FROM
nr.cm_oram_data;


CREATE OR REPLACE VIEW nr.metric2b_score AS SELECT oram_id, avg(lookup.value) AS metric2b_score FROM nr.metric2b_norm norm2b
LEFT JOIN nr.oram_score_lookup_all lookup ON (norm2b.selection = lookup.selection) GROUP BY oram_id;



--function to insert new records from fulcrum export to normalized metric tables

CREATE FUNCTION nr.oram_metric2b_insert () RETURNS trigger AS $_$
BEGIN
WITH metric2b_split AS(
SELECT oram_id, regexp_split_to_table(m2b_surrounding_land_use, ',') AS selection2b FROM nr.cm_oram_data
)

INSERT INTO nr.metric2b_norm SELECT * FROM metric2b_split
WHERE NOT EXISTS
	(
	SELECT 1 FROM nr.metric2b_norm norm2b WHERE oram_id = split2b.oram_id
	);
RETURN NEW;
END $_$ LANGUAGE 'plpgsql';

CREATE TRIGGER metric2b_insert_trigger AFTER INSERT OR UPDATE OR DELETE ON nr.cm_oram_data FOR EACH ROW EXECUTE PROCEDURE nr.oram_metric2b_insert ();
	

--function to produce calculated metric score and aggregate by oram_id after any update to normalized metric table
	
CREATE FUNCTION nr.oram_metric2b_score () RETURNS trigger AS $_$
BEGIN	
WITH metric2b_value AS (
SELECT norm2b.oram_id, avg(lookup.value) AS metric2b_score FROM nr.metric2b_norm norm2b LEFT OUTER JOIN
	nr.oram_score_lookup_all lookup ON lookup.selection  = norm2b.selection GROUP BY oram_id),
	
upsert2b AS (UPDATE nr.metric2b_score score2b SET oram_id = value2b.oram_id, metric2b_score = value2b.metric2b_score
FROM metric2b_value value2b WHERE score2b.oram_id = value2b.oram_id)
	
INSERT INTO nr.metric2b_score SELECT oram_id, metric2b_score FROM metric2b_value value2b
WHERE NOT EXISTS
	(
	SELECT 1 FROM nr.metric2b_score score2b WHERE score2b.oram_id = value2b.oram_id
	)
;
RETURN NEW;
END $_$ LANGUAGE 'plpgsql';


--create trigger for function above
CREATE TRIGGER metric2b_score_trigger AFTER INSERT OR UPDATE OR DELETE ON nr.metric2b_norm FOR EACH ROW EXECUTE PROCEDURE nr.oram_metric2b_score ();
