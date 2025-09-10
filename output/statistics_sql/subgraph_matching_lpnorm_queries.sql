DROP TABLE IF EXISTS norms;
CREATE TABLE norms (
    relation_id   INTEGER NOT NULL, 
    join_var_id        INTEGER NOT NULL,
    aggregator_id INTEGER NOT NULL,
    pred_col_id        INTEGER,
    pred_value_id      INTEGER,
    l1 DOUBLE,
    l2 DOUBLE,
    l3 DOUBLE,
    l4 DOUBLE,
    l5 DOUBLE,
    l6 DOUBLE,
    l7 DOUBLE,
    l8 DOUBLE,
    l9 DOUBLE,
    l10 DOUBLE,
    l_inf DOUBLE
);


    DROP TABLE IF EXISTS degree_sequence;
    CREATE TEMP TABLE degree_sequence AS 
    SELECT s, COUNT(*) AS deg
    FROM dblp.edge
    GROUP BY s

INSERT INTO norms(relation_id, join_var_id, aggregator_id, pred_col_id, pred_value_id, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf)
    SELECT 
        0,
        0,
        0,
        NULL,
        NULL,
        SUM(POWER(deg,1)) AS l1, SUM(POWER(deg,2)) AS l2, SUM(POWER(deg,3)) AS l3, SUM(POWER(deg,4)) AS l4, SUM(POWER(deg,5)) AS l5, SUM(POWER(deg,6)) AS l6, SUM(POWER(deg,7)) AS l7, SUM(POWER(deg,8)) AS l8, SUM(POWER(deg,9)) AS l9, SUM(POWER(deg,10)) AS l10, MAX(deg) AS l_inf 
    FROM degree_sequence;


    DROP TABLE IF EXISTS degree_sequence;
    CREATE TEMP TABLE degree_sequence AS 
    SELECT t, COUNT(*) AS deg
    FROM dblp.edge
    GROUP BY t

INSERT INTO norms(relation_id, join_var_id, aggregator_id, pred_col_id, pred_value_id, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf)
    SELECT 
        0,
        0,
        0,
        NULL,
        NULL,
        SUM(POWER(deg,1)) AS l1, SUM(POWER(deg,2)) AS l2, SUM(POWER(deg,3)) AS l3, SUM(POWER(deg,4)) AS l4, SUM(POWER(deg,5)) AS l5, SUM(POWER(deg,6)) AS l6, SUM(POWER(deg,7)) AS l7, SUM(POWER(deg,8)) AS l8, SUM(POWER(deg,9)) AS l9, SUM(POWER(deg,10)) AS l10, MAX(deg) AS l_inf 
    FROM degree_sequence;


DROP TABLE IF EXISTS joined;
CREATE TEMP TABLE joined AS 
SELECT 
    e.s AS SOURCE, e.t AS TARGET, source_vertex.l AS SOURCE_LABEL, target_vertex.l AS TARGET_LABEL 
FROM dblp.edge AS e
JOIN dblp.vertex AS source_vertex ON e.s = source_vertex.i
JOIN dblp.vertex AS target_vertex ON e.t = target_vertex.i


DROP TABLE IF EXISTS degree_sequence_mcv;
CREATE TEMP TABLE degree_sequence_mcv AS 
SELECT 
    joined.SOURCE,
    joined.TARGET,
    joined.SOURCE_LABEL,
    joined.TARGET_LABEL,
    COUNT(*) AS deg
FROM joined
GROUP BY joined.SOURCE, joined.TARGET, joined.SOURCE_LABEL, joined.TARGET_LABEL
;

INSERT INTO norms(relation_id, join_var_id, aggregator_id, pred_col_id, pred_value_id, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf)
    SELECT 
        0,
        0,
        0,
        SOURCE_LABEL AS pred_col_id,
        TARGET_LABEL AS pred_value_id,
        SUM(POWER(deg,1)) AS l1, SUM(POWER(deg,2)) AS l2, SUM(POWER(deg,3)) AS l3, SUM(POWER(deg,4)) AS l4, SUM(POWER(deg,5)) AS l5, SUM(POWER(deg,6)) AS l6, SUM(POWER(deg,7)) AS l7, SUM(POWER(deg,8)) AS l8, SUM(POWER(deg,9)) AS l9, SUM(POWER(deg,10)) AS l10, MAX(deg) AS l_inf 
    FROM degree_sequence_mcv
    GROUP BY SOURCE, SOURCE_LABEL, TARGET_LABEL

INSERT INTO norms(relation_id, join_var_id, aggregator_id, pred_col_id, pred_value_id, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf)
    SELECT 
        0,
        0,
        0,
        SOURCE_LABEL AS pred_col_id,
        TARGET_LABEL AS pred_value_id,
        SUM(POWER(deg,1)) AS l1, SUM(POWER(deg,2)) AS l2, SUM(POWER(deg,3)) AS l3, SUM(POWER(deg,4)) AS l4, SUM(POWER(deg,5)) AS l5, SUM(POWER(deg,6)) AS l6, SUM(POWER(deg,7)) AS l7, SUM(POWER(deg,8)) AS l8, SUM(POWER(deg,9)) AS l9, SUM(POWER(deg,10)) AS l10, MAX(deg) AS l_inf 
    FROM degree_sequence_mcv
    GROUP BY TARGET, SOURCE_LABEL, TARGET_LABEL

