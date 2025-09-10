DROP TABLE IF EXISTS norms;
CREATE TABLE norms (
    
    relation_name TEXT NOT NULL,
    join_var_name TEXT NOT NULL,
    aggregator_name TEXT NOT NULL,
    pred_col_name TEXT,
    pk_relation_name TEXT,
    fk_join_var_name TEXT,
    pk_join_var_name TEXT,
    
    pred_value_id      INTEGER,
    l0 DOUBLE,
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

----------------------------------
-- START: generate all mcvs and histograms
----------------------------------


----------------------------------
-- END: generate all mcvs and histograms
----------------------------------


----------------------------------
-- START: generate all lpnorm queries for AKA_NAME
----------------------------------


DROP TABLE IF EXISTS Degree_AKA_NAME_PERSON_ID;

-- Create aggregator table Degree_AKA_NAME_PERSON_ID
CREATE TEMP TABLE Degree_AKA_NAME_PERSON_ID AS
SELECT
    AKA_NAME.PERSON_ID AS PERSON_ID,
    COUNT(*) AS deg
FROM AKA_NAME

WHERE AKA_NAME.PERSON_ID IS NOT NULL
GROUP BY
    AKA_NAME.PERSON_ID
HAVING COUNT(*) > 0
ORDER BY deg DESC
;

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'AKA_NAME' AS relation_name,
    'PERSON_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    1 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_AKA_NAME_PERSON_ID
  LIMIT 1
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'AKA_NAME' AS relation_name,
    'PERSON_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    2 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_AKA_NAME_PERSON_ID
  LIMIT 2
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'AKA_NAME' AS relation_name,
    'PERSON_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    4 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_AKA_NAME_PERSON_ID
  LIMIT 4
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'AKA_NAME' AS relation_name,
    'PERSON_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    8 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_AKA_NAME_PERSON_ID
  LIMIT 8
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'AKA_NAME' AS relation_name,
    'PERSON_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    16 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_AKA_NAME_PERSON_ID
  LIMIT 16
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'AKA_NAME' AS relation_name,
    'PERSON_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    32 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_AKA_NAME_PERSON_ID
  LIMIT 32
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'AKA_NAME' AS relation_name,
    'PERSON_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    64 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_AKA_NAME_PERSON_ID
  LIMIT 64
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'AKA_NAME' AS relation_name,
    'PERSON_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    128 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_AKA_NAME_PERSON_ID
  LIMIT 128
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'AKA_NAME' AS relation_name,
    'PERSON_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    256 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_AKA_NAME_PERSON_ID
  LIMIT 256
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'AKA_NAME' AS relation_name,
    'PERSON_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    512 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_AKA_NAME_PERSON_ID
  LIMIT 512
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'AKA_NAME' AS relation_name,
    'PERSON_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    1024 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_AKA_NAME_PERSON_ID
  LIMIT 1024
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'AKA_NAME' AS relation_name,
    'PERSON_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    2048 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_AKA_NAME_PERSON_ID
  LIMIT 2048
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'AKA_NAME' AS relation_name,
    'PERSON_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    4096 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_AKA_NAME_PERSON_ID
  LIMIT 4096
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'AKA_NAME' AS relation_name,
    'PERSON_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    8192 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_AKA_NAME_PERSON_ID
  LIMIT 8192
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'AKA_NAME' AS relation_name,
    'PERSON_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    16384 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_AKA_NAME_PERSON_ID
  LIMIT 16384
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'AKA_NAME' AS relation_name,
    'PERSON_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    32768 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_AKA_NAME_PERSON_ID
  LIMIT 32768
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'AKA_NAME' AS relation_name,
    'PERSON_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    65536 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_AKA_NAME_PERSON_ID
  LIMIT 65536
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'AKA_NAME' AS relation_name,
    'PERSON_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    131072 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_AKA_NAME_PERSON_ID
  LIMIT 131072
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'AKA_NAME' AS relation_name,
    'PERSON_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    262144 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_AKA_NAME_PERSON_ID
  LIMIT 262144
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'AKA_NAME' AS relation_name,
    'PERSON_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    524288 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_AKA_NAME_PERSON_ID
  LIMIT 524288
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'AKA_NAME' AS relation_name,
    'PERSON_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    588222 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_AKA_NAME_PERSON_ID
  LIMIT 588222
);

----------------------------------
-- START: generate all lpnorm queries for AKA_TITLE
----------------------------------


DROP TABLE IF EXISTS Degree_AKA_TITLE_KIND_ID;

-- Create aggregator table Degree_AKA_TITLE_KIND_ID
CREATE TEMP TABLE Degree_AKA_TITLE_KIND_ID AS
SELECT
    AKA_TITLE.KIND_ID AS KIND_ID,
    COUNT(*) AS deg
FROM AKA_TITLE

WHERE AKA_TITLE.KIND_ID IS NOT NULL
GROUP BY
    AKA_TITLE.KIND_ID
HAVING COUNT(*) > 0
ORDER BY deg DESC
;

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'AKA_TITLE' AS relation_name,
    'KIND_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    1 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_AKA_TITLE_KIND_ID
  LIMIT 1
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'AKA_TITLE' AS relation_name,
    'KIND_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    2 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_AKA_TITLE_KIND_ID
  LIMIT 2
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'AKA_TITLE' AS relation_name,
    'KIND_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    4 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_AKA_TITLE_KIND_ID
  LIMIT 4
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'AKA_TITLE' AS relation_name,
    'KIND_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    6 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_AKA_TITLE_KIND_ID
  LIMIT 6
);

DROP TABLE IF EXISTS Degree_AKA_TITLE_MOVIE_ID;

-- Create aggregator table Degree_AKA_TITLE_MOVIE_ID
CREATE TEMP TABLE Degree_AKA_TITLE_MOVIE_ID AS
SELECT
    AKA_TITLE.MOVIE_ID AS MOVIE_ID,
    COUNT(*) AS deg
FROM AKA_TITLE

WHERE AKA_TITLE.MOVIE_ID IS NOT NULL
GROUP BY
    AKA_TITLE.MOVIE_ID
HAVING COUNT(*) > 0
ORDER BY deg DESC
;

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'AKA_TITLE' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    1 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_AKA_TITLE_MOVIE_ID
  LIMIT 1
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'AKA_TITLE' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    2 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_AKA_TITLE_MOVIE_ID
  LIMIT 2
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'AKA_TITLE' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    4 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_AKA_TITLE_MOVIE_ID
  LIMIT 4
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'AKA_TITLE' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    8 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_AKA_TITLE_MOVIE_ID
  LIMIT 8
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'AKA_TITLE' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    16 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_AKA_TITLE_MOVIE_ID
  LIMIT 16
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'AKA_TITLE' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    32 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_AKA_TITLE_MOVIE_ID
  LIMIT 32
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'AKA_TITLE' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    64 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_AKA_TITLE_MOVIE_ID
  LIMIT 64
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'AKA_TITLE' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    128 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_AKA_TITLE_MOVIE_ID
  LIMIT 128
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'AKA_TITLE' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    256 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_AKA_TITLE_MOVIE_ID
  LIMIT 256
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'AKA_TITLE' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    512 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_AKA_TITLE_MOVIE_ID
  LIMIT 512
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'AKA_TITLE' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    1024 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_AKA_TITLE_MOVIE_ID
  LIMIT 1024
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'AKA_TITLE' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    2048 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_AKA_TITLE_MOVIE_ID
  LIMIT 2048
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'AKA_TITLE' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    4096 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_AKA_TITLE_MOVIE_ID
  LIMIT 4096
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'AKA_TITLE' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    8192 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_AKA_TITLE_MOVIE_ID
  LIMIT 8192
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'AKA_TITLE' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    16384 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_AKA_TITLE_MOVIE_ID
  LIMIT 16384
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'AKA_TITLE' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    32768 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_AKA_TITLE_MOVIE_ID
  LIMIT 32768
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'AKA_TITLE' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    65536 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_AKA_TITLE_MOVIE_ID
  LIMIT 65536
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'AKA_TITLE' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    131072 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_AKA_TITLE_MOVIE_ID
  LIMIT 131072
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'AKA_TITLE' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    205631 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_AKA_TITLE_MOVIE_ID
  LIMIT 205631
);

----------------------------------
-- START: generate all lpnorm queries for CAST_INFO
----------------------------------


DROP TABLE IF EXISTS Degree_CAST_INFO_MOVIE_ID;

-- Create aggregator table Degree_CAST_INFO_MOVIE_ID
CREATE TEMP TABLE Degree_CAST_INFO_MOVIE_ID AS
SELECT
    CAST_INFO.MOVIE_ID AS MOVIE_ID,
    COUNT(*) AS deg
FROM CAST_INFO

WHERE CAST_INFO.MOVIE_ID IS NOT NULL
GROUP BY
    CAST_INFO.MOVIE_ID
HAVING COUNT(*) > 0
ORDER BY deg DESC
;

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CAST_INFO' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    1 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CAST_INFO_MOVIE_ID
  LIMIT 1
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CAST_INFO' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    2 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CAST_INFO_MOVIE_ID
  LIMIT 2
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CAST_INFO' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    4 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CAST_INFO_MOVIE_ID
  LIMIT 4
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CAST_INFO' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    8 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CAST_INFO_MOVIE_ID
  LIMIT 8
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CAST_INFO' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    16 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CAST_INFO_MOVIE_ID
  LIMIT 16
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CAST_INFO' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    32 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CAST_INFO_MOVIE_ID
  LIMIT 32
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CAST_INFO' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    64 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CAST_INFO_MOVIE_ID
  LIMIT 64
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CAST_INFO' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    128 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CAST_INFO_MOVIE_ID
  LIMIT 128
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CAST_INFO' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    256 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CAST_INFO_MOVIE_ID
  LIMIT 256
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CAST_INFO' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    512 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CAST_INFO_MOVIE_ID
  LIMIT 512
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CAST_INFO' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    1024 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CAST_INFO_MOVIE_ID
  LIMIT 1024
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CAST_INFO' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    2048 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CAST_INFO_MOVIE_ID
  LIMIT 2048
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CAST_INFO' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    4096 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CAST_INFO_MOVIE_ID
  LIMIT 4096
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CAST_INFO' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    8192 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CAST_INFO_MOVIE_ID
  LIMIT 8192
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CAST_INFO' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    16384 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CAST_INFO_MOVIE_ID
  LIMIT 16384
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CAST_INFO' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    32768 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CAST_INFO_MOVIE_ID
  LIMIT 32768
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CAST_INFO' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    65536 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CAST_INFO_MOVIE_ID
  LIMIT 65536
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CAST_INFO' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    131072 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CAST_INFO_MOVIE_ID
  LIMIT 131072
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CAST_INFO' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    262144 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CAST_INFO_MOVIE_ID
  LIMIT 262144
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CAST_INFO' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    524288 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CAST_INFO_MOVIE_ID
  LIMIT 524288
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CAST_INFO' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    1048576 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CAST_INFO_MOVIE_ID
  LIMIT 1048576
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CAST_INFO' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    2097152 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CAST_INFO_MOVIE_ID
  LIMIT 2097152
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CAST_INFO' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    2331601 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CAST_INFO_MOVIE_ID
  LIMIT 2331601
);

DROP TABLE IF EXISTS Degree_CAST_INFO_PERSON_ID;

-- Create aggregator table Degree_CAST_INFO_PERSON_ID
CREATE TEMP TABLE Degree_CAST_INFO_PERSON_ID AS
SELECT
    CAST_INFO.PERSON_ID AS PERSON_ID,
    COUNT(*) AS deg
FROM CAST_INFO

WHERE CAST_INFO.PERSON_ID IS NOT NULL
GROUP BY
    CAST_INFO.PERSON_ID
HAVING COUNT(*) > 0
ORDER BY deg DESC
;

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CAST_INFO' AS relation_name,
    'PERSON_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    1 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CAST_INFO_PERSON_ID
  LIMIT 1
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CAST_INFO' AS relation_name,
    'PERSON_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    2 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CAST_INFO_PERSON_ID
  LIMIT 2
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CAST_INFO' AS relation_name,
    'PERSON_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    4 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CAST_INFO_PERSON_ID
  LIMIT 4
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CAST_INFO' AS relation_name,
    'PERSON_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    8 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CAST_INFO_PERSON_ID
  LIMIT 8
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CAST_INFO' AS relation_name,
    'PERSON_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    16 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CAST_INFO_PERSON_ID
  LIMIT 16
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CAST_INFO' AS relation_name,
    'PERSON_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    32 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CAST_INFO_PERSON_ID
  LIMIT 32
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CAST_INFO' AS relation_name,
    'PERSON_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    64 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CAST_INFO_PERSON_ID
  LIMIT 64
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CAST_INFO' AS relation_name,
    'PERSON_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    128 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CAST_INFO_PERSON_ID
  LIMIT 128
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CAST_INFO' AS relation_name,
    'PERSON_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    256 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CAST_INFO_PERSON_ID
  LIMIT 256
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CAST_INFO' AS relation_name,
    'PERSON_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    512 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CAST_INFO_PERSON_ID
  LIMIT 512
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CAST_INFO' AS relation_name,
    'PERSON_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    1024 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CAST_INFO_PERSON_ID
  LIMIT 1024
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CAST_INFO' AS relation_name,
    'PERSON_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    2048 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CAST_INFO_PERSON_ID
  LIMIT 2048
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CAST_INFO' AS relation_name,
    'PERSON_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    4096 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CAST_INFO_PERSON_ID
  LIMIT 4096
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CAST_INFO' AS relation_name,
    'PERSON_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    8192 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CAST_INFO_PERSON_ID
  LIMIT 8192
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CAST_INFO' AS relation_name,
    'PERSON_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    16384 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CAST_INFO_PERSON_ID
  LIMIT 16384
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CAST_INFO' AS relation_name,
    'PERSON_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    32768 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CAST_INFO_PERSON_ID
  LIMIT 32768
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CAST_INFO' AS relation_name,
    'PERSON_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    65536 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CAST_INFO_PERSON_ID
  LIMIT 65536
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CAST_INFO' AS relation_name,
    'PERSON_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    131072 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CAST_INFO_PERSON_ID
  LIMIT 131072
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CAST_INFO' AS relation_name,
    'PERSON_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    262144 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CAST_INFO_PERSON_ID
  LIMIT 262144
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CAST_INFO' AS relation_name,
    'PERSON_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    524288 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CAST_INFO_PERSON_ID
  LIMIT 524288
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CAST_INFO' AS relation_name,
    'PERSON_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    1048576 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CAST_INFO_PERSON_ID
  LIMIT 1048576
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CAST_INFO' AS relation_name,
    'PERSON_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    2097152 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CAST_INFO_PERSON_ID
  LIMIT 2097152
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CAST_INFO' AS relation_name,
    'PERSON_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    4051810 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CAST_INFO_PERSON_ID
  LIMIT 4051810
);

DROP TABLE IF EXISTS Degree_CAST_INFO_PERSON_ROLE_ID;

-- Create aggregator table Degree_CAST_INFO_PERSON_ROLE_ID
CREATE TEMP TABLE Degree_CAST_INFO_PERSON_ROLE_ID AS
SELECT
    CAST_INFO.PERSON_ROLE_ID AS PERSON_ROLE_ID,
    COUNT(*) AS deg
FROM CAST_INFO

WHERE CAST_INFO.PERSON_ROLE_ID IS NOT NULL
GROUP BY
    CAST_INFO.PERSON_ROLE_ID
HAVING COUNT(*) > 0
ORDER BY deg DESC
;

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CAST_INFO' AS relation_name,
    'PERSON_ROLE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    1 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CAST_INFO_PERSON_ROLE_ID
  LIMIT 1
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CAST_INFO' AS relation_name,
    'PERSON_ROLE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    2 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CAST_INFO_PERSON_ROLE_ID
  LIMIT 2
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CAST_INFO' AS relation_name,
    'PERSON_ROLE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    4 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CAST_INFO_PERSON_ROLE_ID
  LIMIT 4
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CAST_INFO' AS relation_name,
    'PERSON_ROLE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    8 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CAST_INFO_PERSON_ROLE_ID
  LIMIT 8
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CAST_INFO' AS relation_name,
    'PERSON_ROLE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    16 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CAST_INFO_PERSON_ROLE_ID
  LIMIT 16
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CAST_INFO' AS relation_name,
    'PERSON_ROLE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    32 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CAST_INFO_PERSON_ROLE_ID
  LIMIT 32
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CAST_INFO' AS relation_name,
    'PERSON_ROLE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    64 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CAST_INFO_PERSON_ROLE_ID
  LIMIT 64
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CAST_INFO' AS relation_name,
    'PERSON_ROLE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    128 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CAST_INFO_PERSON_ROLE_ID
  LIMIT 128
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CAST_INFO' AS relation_name,
    'PERSON_ROLE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    256 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CAST_INFO_PERSON_ROLE_ID
  LIMIT 256
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CAST_INFO' AS relation_name,
    'PERSON_ROLE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    512 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CAST_INFO_PERSON_ROLE_ID
  LIMIT 512
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CAST_INFO' AS relation_name,
    'PERSON_ROLE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    1024 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CAST_INFO_PERSON_ROLE_ID
  LIMIT 1024
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CAST_INFO' AS relation_name,
    'PERSON_ROLE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    2048 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CAST_INFO_PERSON_ROLE_ID
  LIMIT 2048
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CAST_INFO' AS relation_name,
    'PERSON_ROLE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    4096 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CAST_INFO_PERSON_ROLE_ID
  LIMIT 4096
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CAST_INFO' AS relation_name,
    'PERSON_ROLE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    8192 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CAST_INFO_PERSON_ROLE_ID
  LIMIT 8192
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CAST_INFO' AS relation_name,
    'PERSON_ROLE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    16384 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CAST_INFO_PERSON_ROLE_ID
  LIMIT 16384
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CAST_INFO' AS relation_name,
    'PERSON_ROLE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    32768 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CAST_INFO_PERSON_ROLE_ID
  LIMIT 32768
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CAST_INFO' AS relation_name,
    'PERSON_ROLE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    65536 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CAST_INFO_PERSON_ROLE_ID
  LIMIT 65536
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CAST_INFO' AS relation_name,
    'PERSON_ROLE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    131072 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CAST_INFO_PERSON_ROLE_ID
  LIMIT 131072
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CAST_INFO' AS relation_name,
    'PERSON_ROLE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    262144 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CAST_INFO_PERSON_ROLE_ID
  LIMIT 262144
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CAST_INFO' AS relation_name,
    'PERSON_ROLE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    524288 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CAST_INFO_PERSON_ROLE_ID
  LIMIT 524288
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CAST_INFO' AS relation_name,
    'PERSON_ROLE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    1048576 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CAST_INFO_PERSON_ROLE_ID
  LIMIT 1048576
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CAST_INFO' AS relation_name,
    'PERSON_ROLE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    2097152 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CAST_INFO_PERSON_ROLE_ID
  LIMIT 2097152
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CAST_INFO' AS relation_name,
    'PERSON_ROLE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    3140339 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CAST_INFO_PERSON_ROLE_ID
  LIMIT 3140339
);

DROP TABLE IF EXISTS Degree_CAST_INFO_ROLE_ID;

-- Create aggregator table Degree_CAST_INFO_ROLE_ID
CREATE TEMP TABLE Degree_CAST_INFO_ROLE_ID AS
SELECT
    CAST_INFO.ROLE_ID AS ROLE_ID,
    COUNT(*) AS deg
FROM CAST_INFO

WHERE CAST_INFO.ROLE_ID IS NOT NULL
GROUP BY
    CAST_INFO.ROLE_ID
HAVING COUNT(*) > 0
ORDER BY deg DESC
;

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CAST_INFO' AS relation_name,
    'ROLE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    1 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CAST_INFO_ROLE_ID
  LIMIT 1
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CAST_INFO' AS relation_name,
    'ROLE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    2 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CAST_INFO_ROLE_ID
  LIMIT 2
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CAST_INFO' AS relation_name,
    'ROLE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    4 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CAST_INFO_ROLE_ID
  LIMIT 4
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CAST_INFO' AS relation_name,
    'ROLE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    8 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CAST_INFO_ROLE_ID
  LIMIT 8
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CAST_INFO' AS relation_name,
    'ROLE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    11 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CAST_INFO_ROLE_ID
  LIMIT 11
);

----------------------------------
-- START: generate all lpnorm queries for CHAR_NAME
----------------------------------


DROP TABLE IF EXISTS Degree_CHAR_NAME_ID;

-- Create aggregator table Degree_CHAR_NAME_ID
CREATE TEMP TABLE Degree_CHAR_NAME_ID AS
SELECT
    CHAR_NAME.ID AS ID,
    COUNT(*) AS deg
FROM CHAR_NAME

WHERE CHAR_NAME.ID IS NOT NULL
GROUP BY
    CHAR_NAME.ID
HAVING COUNT(*) > 0
ORDER BY deg DESC
;

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CHAR_NAME' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    1 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CHAR_NAME_ID
  LIMIT 1
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CHAR_NAME' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    2 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CHAR_NAME_ID
  LIMIT 2
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CHAR_NAME' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    4 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CHAR_NAME_ID
  LIMIT 4
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CHAR_NAME' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    8 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CHAR_NAME_ID
  LIMIT 8
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CHAR_NAME' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    16 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CHAR_NAME_ID
  LIMIT 16
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CHAR_NAME' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    32 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CHAR_NAME_ID
  LIMIT 32
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CHAR_NAME' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    64 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CHAR_NAME_ID
  LIMIT 64
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CHAR_NAME' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    128 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CHAR_NAME_ID
  LIMIT 128
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CHAR_NAME' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    256 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CHAR_NAME_ID
  LIMIT 256
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CHAR_NAME' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    512 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CHAR_NAME_ID
  LIMIT 512
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CHAR_NAME' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    1024 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CHAR_NAME_ID
  LIMIT 1024
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CHAR_NAME' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    2048 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CHAR_NAME_ID
  LIMIT 2048
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CHAR_NAME' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    4096 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CHAR_NAME_ID
  LIMIT 4096
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CHAR_NAME' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    8192 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CHAR_NAME_ID
  LIMIT 8192
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CHAR_NAME' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    16384 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CHAR_NAME_ID
  LIMIT 16384
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CHAR_NAME' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    32768 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CHAR_NAME_ID
  LIMIT 32768
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CHAR_NAME' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    65536 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CHAR_NAME_ID
  LIMIT 65536
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CHAR_NAME' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    131072 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CHAR_NAME_ID
  LIMIT 131072
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CHAR_NAME' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    262144 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CHAR_NAME_ID
  LIMIT 262144
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CHAR_NAME' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    524288 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CHAR_NAME_ID
  LIMIT 524288
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CHAR_NAME' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    1048576 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CHAR_NAME_ID
  LIMIT 1048576
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CHAR_NAME' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    2097152 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CHAR_NAME_ID
  LIMIT 2097152
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CHAR_NAME' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    3140339 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_CHAR_NAME_ID
  LIMIT 3140339
);

----------------------------------
-- START: generate all lpnorm queries for COMP_CAST_TYPE
----------------------------------


DROP TABLE IF EXISTS Degree_COMP_CAST_TYPE_ID;

-- Create aggregator table Degree_COMP_CAST_TYPE_ID
CREATE TEMP TABLE Degree_COMP_CAST_TYPE_ID AS
SELECT
    COMP_CAST_TYPE.ID AS ID,
    COUNT(*) AS deg
FROM COMP_CAST_TYPE

WHERE COMP_CAST_TYPE.ID IS NOT NULL
GROUP BY
    COMP_CAST_TYPE.ID
HAVING COUNT(*) > 0
ORDER BY deg DESC
;

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'COMP_CAST_TYPE' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    1 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_COMP_CAST_TYPE_ID
  LIMIT 1
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'COMP_CAST_TYPE' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    2 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_COMP_CAST_TYPE_ID
  LIMIT 2
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'COMP_CAST_TYPE' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    4 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_COMP_CAST_TYPE_ID
  LIMIT 4
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'COMP_CAST_TYPE' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    4 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_COMP_CAST_TYPE_ID
  LIMIT 4
);

----------------------------------
-- START: generate all lpnorm queries for COMPANY_NAME
----------------------------------


DROP TABLE IF EXISTS Degree_COMPANY_NAME_ID;

-- Create aggregator table Degree_COMPANY_NAME_ID
CREATE TEMP TABLE Degree_COMPANY_NAME_ID AS
SELECT
    COMPANY_NAME.ID AS ID,
    COUNT(*) AS deg
FROM COMPANY_NAME

WHERE COMPANY_NAME.ID IS NOT NULL
GROUP BY
    COMPANY_NAME.ID
HAVING COUNT(*) > 0
ORDER BY deg DESC
;

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'COMPANY_NAME' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    1 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_COMPANY_NAME_ID
  LIMIT 1
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'COMPANY_NAME' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    2 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_COMPANY_NAME_ID
  LIMIT 2
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'COMPANY_NAME' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    4 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_COMPANY_NAME_ID
  LIMIT 4
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'COMPANY_NAME' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    8 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_COMPANY_NAME_ID
  LIMIT 8
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'COMPANY_NAME' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    16 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_COMPANY_NAME_ID
  LIMIT 16
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'COMPANY_NAME' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    32 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_COMPANY_NAME_ID
  LIMIT 32
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'COMPANY_NAME' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    64 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_COMPANY_NAME_ID
  LIMIT 64
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'COMPANY_NAME' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    128 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_COMPANY_NAME_ID
  LIMIT 128
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'COMPANY_NAME' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    256 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_COMPANY_NAME_ID
  LIMIT 256
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'COMPANY_NAME' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    512 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_COMPANY_NAME_ID
  LIMIT 512
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'COMPANY_NAME' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    1024 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_COMPANY_NAME_ID
  LIMIT 1024
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'COMPANY_NAME' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    2048 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_COMPANY_NAME_ID
  LIMIT 2048
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'COMPANY_NAME' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    4096 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_COMPANY_NAME_ID
  LIMIT 4096
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'COMPANY_NAME' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    8192 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_COMPANY_NAME_ID
  LIMIT 8192
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'COMPANY_NAME' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    16384 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_COMPANY_NAME_ID
  LIMIT 16384
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'COMPANY_NAME' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    32768 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_COMPANY_NAME_ID
  LIMIT 32768
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'COMPANY_NAME' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    65536 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_COMPANY_NAME_ID
  LIMIT 65536
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'COMPANY_NAME' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    131072 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_COMPANY_NAME_ID
  LIMIT 131072
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'COMPANY_NAME' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    234997 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_COMPANY_NAME_ID
  LIMIT 234997
);

----------------------------------
-- START: generate all lpnorm queries for COMPANY_TYPE
----------------------------------


DROP TABLE IF EXISTS Degree_COMPANY_TYPE_ID;

-- Create aggregator table Degree_COMPANY_TYPE_ID
CREATE TEMP TABLE Degree_COMPANY_TYPE_ID AS
SELECT
    COMPANY_TYPE.ID AS ID,
    COUNT(*) AS deg
FROM COMPANY_TYPE

WHERE COMPANY_TYPE.ID IS NOT NULL
GROUP BY
    COMPANY_TYPE.ID
HAVING COUNT(*) > 0
ORDER BY deg DESC
;

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'COMPANY_TYPE' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    1 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_COMPANY_TYPE_ID
  LIMIT 1
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'COMPANY_TYPE' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    2 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_COMPANY_TYPE_ID
  LIMIT 2
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'COMPANY_TYPE' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    4 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_COMPANY_TYPE_ID
  LIMIT 4
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'COMPANY_TYPE' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    4 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_COMPANY_TYPE_ID
  LIMIT 4
);

----------------------------------
-- START: generate all lpnorm queries for COMPLETE_CAST
----------------------------------


DROP TABLE IF EXISTS Degree_COMPLETE_CAST_MOVIE_ID;

-- Create aggregator table Degree_COMPLETE_CAST_MOVIE_ID
CREATE TEMP TABLE Degree_COMPLETE_CAST_MOVIE_ID AS
SELECT
    COMPLETE_CAST.MOVIE_ID AS MOVIE_ID,
    COUNT(*) AS deg
FROM COMPLETE_CAST

WHERE COMPLETE_CAST.MOVIE_ID IS NOT NULL
GROUP BY
    COMPLETE_CAST.MOVIE_ID
HAVING COUNT(*) > 0
ORDER BY deg DESC
;

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'COMPLETE_CAST' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    1 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_COMPLETE_CAST_MOVIE_ID
  LIMIT 1
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'COMPLETE_CAST' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    2 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_COMPLETE_CAST_MOVIE_ID
  LIMIT 2
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'COMPLETE_CAST' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    4 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_COMPLETE_CAST_MOVIE_ID
  LIMIT 4
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'COMPLETE_CAST' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    8 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_COMPLETE_CAST_MOVIE_ID
  LIMIT 8
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'COMPLETE_CAST' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    16 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_COMPLETE_CAST_MOVIE_ID
  LIMIT 16
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'COMPLETE_CAST' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    32 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_COMPLETE_CAST_MOVIE_ID
  LIMIT 32
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'COMPLETE_CAST' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    64 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_COMPLETE_CAST_MOVIE_ID
  LIMIT 64
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'COMPLETE_CAST' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    128 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_COMPLETE_CAST_MOVIE_ID
  LIMIT 128
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'COMPLETE_CAST' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    256 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_COMPLETE_CAST_MOVIE_ID
  LIMIT 256
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'COMPLETE_CAST' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    512 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_COMPLETE_CAST_MOVIE_ID
  LIMIT 512
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'COMPLETE_CAST' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    1024 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_COMPLETE_CAST_MOVIE_ID
  LIMIT 1024
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'COMPLETE_CAST' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    2048 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_COMPLETE_CAST_MOVIE_ID
  LIMIT 2048
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'COMPLETE_CAST' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    4096 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_COMPLETE_CAST_MOVIE_ID
  LIMIT 4096
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'COMPLETE_CAST' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    8192 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_COMPLETE_CAST_MOVIE_ID
  LIMIT 8192
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'COMPLETE_CAST' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    16384 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_COMPLETE_CAST_MOVIE_ID
  LIMIT 16384
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'COMPLETE_CAST' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    32768 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_COMPLETE_CAST_MOVIE_ID
  LIMIT 32768
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'COMPLETE_CAST' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    65536 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_COMPLETE_CAST_MOVIE_ID
  LIMIT 65536
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'COMPLETE_CAST' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    93514 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_COMPLETE_CAST_MOVIE_ID
  LIMIT 93514
);

DROP TABLE IF EXISTS Degree_COMPLETE_CAST_STATUS_ID;

-- Create aggregator table Degree_COMPLETE_CAST_STATUS_ID
CREATE TEMP TABLE Degree_COMPLETE_CAST_STATUS_ID AS
SELECT
    COMPLETE_CAST.STATUS_ID AS STATUS_ID,
    COUNT(*) AS deg
FROM COMPLETE_CAST

WHERE COMPLETE_CAST.STATUS_ID IS NOT NULL
GROUP BY
    COMPLETE_CAST.STATUS_ID
HAVING COUNT(*) > 0
ORDER BY deg DESC
;

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'COMPLETE_CAST' AS relation_name,
    'STATUS_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    1 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_COMPLETE_CAST_STATUS_ID
  LIMIT 1
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'COMPLETE_CAST' AS relation_name,
    'STATUS_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    2 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_COMPLETE_CAST_STATUS_ID
  LIMIT 2
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'COMPLETE_CAST' AS relation_name,
    'STATUS_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    2 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_COMPLETE_CAST_STATUS_ID
  LIMIT 2
);

DROP TABLE IF EXISTS Degree_COMPLETE_CAST_SUBJECT_ID;

-- Create aggregator table Degree_COMPLETE_CAST_SUBJECT_ID
CREATE TEMP TABLE Degree_COMPLETE_CAST_SUBJECT_ID AS
SELECT
    COMPLETE_CAST.SUBJECT_ID AS SUBJECT_ID,
    COUNT(*) AS deg
FROM COMPLETE_CAST

WHERE COMPLETE_CAST.SUBJECT_ID IS NOT NULL
GROUP BY
    COMPLETE_CAST.SUBJECT_ID
HAVING COUNT(*) > 0
ORDER BY deg DESC
;

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'COMPLETE_CAST' AS relation_name,
    'SUBJECT_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    1 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_COMPLETE_CAST_SUBJECT_ID
  LIMIT 1
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'COMPLETE_CAST' AS relation_name,
    'SUBJECT_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    2 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_COMPLETE_CAST_SUBJECT_ID
  LIMIT 2
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'COMPLETE_CAST' AS relation_name,
    'SUBJECT_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    2 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_COMPLETE_CAST_SUBJECT_ID
  LIMIT 2
);

----------------------------------
-- START: generate all lpnorm queries for INFO_TYPE
----------------------------------


DROP TABLE IF EXISTS Degree_INFO_TYPE_ID;

-- Create aggregator table Degree_INFO_TYPE_ID
CREATE TEMP TABLE Degree_INFO_TYPE_ID AS
SELECT
    INFO_TYPE.ID AS ID,
    COUNT(*) AS deg
FROM INFO_TYPE

WHERE INFO_TYPE.ID IS NOT NULL
GROUP BY
    INFO_TYPE.ID
HAVING COUNT(*) > 0
ORDER BY deg DESC
;

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'INFO_TYPE' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    1 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_INFO_TYPE_ID
  LIMIT 1
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'INFO_TYPE' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    2 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_INFO_TYPE_ID
  LIMIT 2
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'INFO_TYPE' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    4 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_INFO_TYPE_ID
  LIMIT 4
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'INFO_TYPE' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    8 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_INFO_TYPE_ID
  LIMIT 8
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'INFO_TYPE' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    16 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_INFO_TYPE_ID
  LIMIT 16
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'INFO_TYPE' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    32 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_INFO_TYPE_ID
  LIMIT 32
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'INFO_TYPE' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    64 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_INFO_TYPE_ID
  LIMIT 64
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'INFO_TYPE' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    113 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_INFO_TYPE_ID
  LIMIT 113
);

----------------------------------
-- START: generate all lpnorm queries for KEYWORD
----------------------------------


DROP TABLE IF EXISTS Degree_KEYWORD_ID;

-- Create aggregator table Degree_KEYWORD_ID
CREATE TEMP TABLE Degree_KEYWORD_ID AS
SELECT
    KEYWORD.ID AS ID,
    COUNT(*) AS deg
FROM KEYWORD

WHERE KEYWORD.ID IS NOT NULL
GROUP BY
    KEYWORD.ID
HAVING COUNT(*) > 0
ORDER BY deg DESC
;

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'KEYWORD' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    1 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_KEYWORD_ID
  LIMIT 1
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'KEYWORD' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    2 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_KEYWORD_ID
  LIMIT 2
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'KEYWORD' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    4 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_KEYWORD_ID
  LIMIT 4
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'KEYWORD' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    8 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_KEYWORD_ID
  LIMIT 8
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'KEYWORD' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    16 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_KEYWORD_ID
  LIMIT 16
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'KEYWORD' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    32 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_KEYWORD_ID
  LIMIT 32
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'KEYWORD' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    64 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_KEYWORD_ID
  LIMIT 64
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'KEYWORD' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    128 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_KEYWORD_ID
  LIMIT 128
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'KEYWORD' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    256 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_KEYWORD_ID
  LIMIT 256
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'KEYWORD' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    512 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_KEYWORD_ID
  LIMIT 512
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'KEYWORD' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    1024 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_KEYWORD_ID
  LIMIT 1024
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'KEYWORD' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    2048 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_KEYWORD_ID
  LIMIT 2048
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'KEYWORD' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    4096 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_KEYWORD_ID
  LIMIT 4096
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'KEYWORD' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    8192 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_KEYWORD_ID
  LIMIT 8192
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'KEYWORD' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    16384 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_KEYWORD_ID
  LIMIT 16384
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'KEYWORD' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    32768 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_KEYWORD_ID
  LIMIT 32768
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'KEYWORD' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    65536 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_KEYWORD_ID
  LIMIT 65536
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'KEYWORD' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    131072 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_KEYWORD_ID
  LIMIT 131072
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'KEYWORD' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    134170 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_KEYWORD_ID
  LIMIT 134170
);

----------------------------------
-- START: generate all lpnorm queries for KIND_TYPE
----------------------------------


DROP TABLE IF EXISTS Degree_KIND_TYPE_ID;

-- Create aggregator table Degree_KIND_TYPE_ID
CREATE TEMP TABLE Degree_KIND_TYPE_ID AS
SELECT
    KIND_TYPE.ID AS ID,
    COUNT(*) AS deg
FROM KIND_TYPE

WHERE KIND_TYPE.ID IS NOT NULL
GROUP BY
    KIND_TYPE.ID
HAVING COUNT(*) > 0
ORDER BY deg DESC
;

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'KIND_TYPE' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    1 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_KIND_TYPE_ID
  LIMIT 1
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'KIND_TYPE' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    2 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_KIND_TYPE_ID
  LIMIT 2
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'KIND_TYPE' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    4 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_KIND_TYPE_ID
  LIMIT 4
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'KIND_TYPE' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    7 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_KIND_TYPE_ID
  LIMIT 7
);

----------------------------------
-- START: generate all lpnorm queries for LINK_TYPE
----------------------------------


DROP TABLE IF EXISTS Degree_LINK_TYPE_ID;

-- Create aggregator table Degree_LINK_TYPE_ID
CREATE TEMP TABLE Degree_LINK_TYPE_ID AS
SELECT
    LINK_TYPE.ID AS ID,
    COUNT(*) AS deg
FROM LINK_TYPE

WHERE LINK_TYPE.ID IS NOT NULL
GROUP BY
    LINK_TYPE.ID
HAVING COUNT(*) > 0
ORDER BY deg DESC
;

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'LINK_TYPE' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    1 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_LINK_TYPE_ID
  LIMIT 1
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'LINK_TYPE' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    2 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_LINK_TYPE_ID
  LIMIT 2
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'LINK_TYPE' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    4 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_LINK_TYPE_ID
  LIMIT 4
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'LINK_TYPE' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    8 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_LINK_TYPE_ID
  LIMIT 8
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'LINK_TYPE' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    16 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_LINK_TYPE_ID
  LIMIT 16
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'LINK_TYPE' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    18 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_LINK_TYPE_ID
  LIMIT 18
);

----------------------------------
-- START: generate all lpnorm queries for MOVIE_COMPANIES
----------------------------------


DROP TABLE IF EXISTS Degree_MOVIE_COMPANIES_COMPANY_ID;

-- Create aggregator table Degree_MOVIE_COMPANIES_COMPANY_ID
CREATE TEMP TABLE Degree_MOVIE_COMPANIES_COMPANY_ID AS
SELECT
    MOVIE_COMPANIES.COMPANY_ID AS COMPANY_ID,
    COUNT(*) AS deg
FROM MOVIE_COMPANIES

WHERE MOVIE_COMPANIES.COMPANY_ID IS NOT NULL
GROUP BY
    MOVIE_COMPANIES.COMPANY_ID
HAVING COUNT(*) > 0
ORDER BY deg DESC
;

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_COMPANIES' AS relation_name,
    'COMPANY_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    1 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_COMPANIES_COMPANY_ID
  LIMIT 1
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_COMPANIES' AS relation_name,
    'COMPANY_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    2 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_COMPANIES_COMPANY_ID
  LIMIT 2
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_COMPANIES' AS relation_name,
    'COMPANY_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    4 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_COMPANIES_COMPANY_ID
  LIMIT 4
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_COMPANIES' AS relation_name,
    'COMPANY_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    8 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_COMPANIES_COMPANY_ID
  LIMIT 8
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_COMPANIES' AS relation_name,
    'COMPANY_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    16 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_COMPANIES_COMPANY_ID
  LIMIT 16
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_COMPANIES' AS relation_name,
    'COMPANY_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    32 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_COMPANIES_COMPANY_ID
  LIMIT 32
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_COMPANIES' AS relation_name,
    'COMPANY_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    64 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_COMPANIES_COMPANY_ID
  LIMIT 64
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_COMPANIES' AS relation_name,
    'COMPANY_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    128 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_COMPANIES_COMPANY_ID
  LIMIT 128
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_COMPANIES' AS relation_name,
    'COMPANY_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    256 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_COMPANIES_COMPANY_ID
  LIMIT 256
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_COMPANIES' AS relation_name,
    'COMPANY_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    512 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_COMPANIES_COMPANY_ID
  LIMIT 512
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_COMPANIES' AS relation_name,
    'COMPANY_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    1024 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_COMPANIES_COMPANY_ID
  LIMIT 1024
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_COMPANIES' AS relation_name,
    'COMPANY_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    2048 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_COMPANIES_COMPANY_ID
  LIMIT 2048
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_COMPANIES' AS relation_name,
    'COMPANY_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    4096 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_COMPANIES_COMPANY_ID
  LIMIT 4096
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_COMPANIES' AS relation_name,
    'COMPANY_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    8192 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_COMPANIES_COMPANY_ID
  LIMIT 8192
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_COMPANIES' AS relation_name,
    'COMPANY_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    16384 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_COMPANIES_COMPANY_ID
  LIMIT 16384
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_COMPANIES' AS relation_name,
    'COMPANY_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    32768 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_COMPANIES_COMPANY_ID
  LIMIT 32768
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_COMPANIES' AS relation_name,
    'COMPANY_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    65536 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_COMPANIES_COMPANY_ID
  LIMIT 65536
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_COMPANIES' AS relation_name,
    'COMPANY_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    131072 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_COMPANIES_COMPANY_ID
  LIMIT 131072
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_COMPANIES' AS relation_name,
    'COMPANY_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    234997 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_COMPANIES_COMPANY_ID
  LIMIT 234997
);

DROP TABLE IF EXISTS Degree_MOVIE_COMPANIES_COMPANY_TYPE_ID;

-- Create aggregator table Degree_MOVIE_COMPANIES_COMPANY_TYPE_ID
CREATE TEMP TABLE Degree_MOVIE_COMPANIES_COMPANY_TYPE_ID AS
SELECT
    MOVIE_COMPANIES.COMPANY_TYPE_ID AS COMPANY_TYPE_ID,
    COUNT(*) AS deg
FROM MOVIE_COMPANIES

WHERE MOVIE_COMPANIES.COMPANY_TYPE_ID IS NOT NULL
GROUP BY
    MOVIE_COMPANIES.COMPANY_TYPE_ID
HAVING COUNT(*) > 0
ORDER BY deg DESC
;

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_COMPANIES' AS relation_name,
    'COMPANY_TYPE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    1 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_COMPANIES_COMPANY_TYPE_ID
  LIMIT 1
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_COMPANIES' AS relation_name,
    'COMPANY_TYPE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    2 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_COMPANIES_COMPANY_TYPE_ID
  LIMIT 2
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_COMPANIES' AS relation_name,
    'COMPANY_TYPE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    2 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_COMPANIES_COMPANY_TYPE_ID
  LIMIT 2
);

DROP TABLE IF EXISTS Degree_MOVIE_COMPANIES_MOVIE_ID;

-- Create aggregator table Degree_MOVIE_COMPANIES_MOVIE_ID
CREATE TEMP TABLE Degree_MOVIE_COMPANIES_MOVIE_ID AS
SELECT
    MOVIE_COMPANIES.MOVIE_ID AS MOVIE_ID,
    COUNT(*) AS deg
FROM MOVIE_COMPANIES

WHERE MOVIE_COMPANIES.MOVIE_ID IS NOT NULL
GROUP BY
    MOVIE_COMPANIES.MOVIE_ID
HAVING COUNT(*) > 0
ORDER BY deg DESC
;

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_COMPANIES' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    1 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_COMPANIES_MOVIE_ID
  LIMIT 1
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_COMPANIES' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    2 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_COMPANIES_MOVIE_ID
  LIMIT 2
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_COMPANIES' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    4 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_COMPANIES_MOVIE_ID
  LIMIT 4
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_COMPANIES' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    8 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_COMPANIES_MOVIE_ID
  LIMIT 8
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_COMPANIES' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    16 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_COMPANIES_MOVIE_ID
  LIMIT 16
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_COMPANIES' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    32 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_COMPANIES_MOVIE_ID
  LIMIT 32
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_COMPANIES' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    64 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_COMPANIES_MOVIE_ID
  LIMIT 64
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_COMPANIES' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    128 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_COMPANIES_MOVIE_ID
  LIMIT 128
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_COMPANIES' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    256 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_COMPANIES_MOVIE_ID
  LIMIT 256
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_COMPANIES' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    512 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_COMPANIES_MOVIE_ID
  LIMIT 512
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_COMPANIES' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    1024 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_COMPANIES_MOVIE_ID
  LIMIT 1024
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_COMPANIES' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    2048 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_COMPANIES_MOVIE_ID
  LIMIT 2048
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_COMPANIES' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    4096 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_COMPANIES_MOVIE_ID
  LIMIT 4096
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_COMPANIES' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    8192 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_COMPANIES_MOVIE_ID
  LIMIT 8192
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_COMPANIES' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    16384 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_COMPANIES_MOVIE_ID
  LIMIT 16384
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_COMPANIES' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    32768 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_COMPANIES_MOVIE_ID
  LIMIT 32768
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_COMPANIES' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    65536 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_COMPANIES_MOVIE_ID
  LIMIT 65536
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_COMPANIES' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    131072 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_COMPANIES_MOVIE_ID
  LIMIT 131072
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_COMPANIES' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    262144 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_COMPANIES_MOVIE_ID
  LIMIT 262144
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_COMPANIES' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    524288 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_COMPANIES_MOVIE_ID
  LIMIT 524288
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_COMPANIES' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    1048576 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_COMPANIES_MOVIE_ID
  LIMIT 1048576
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_COMPANIES' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    1087236 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_COMPANIES_MOVIE_ID
  LIMIT 1087236
);

----------------------------------
-- START: generate all lpnorm queries for MOVIE_INFO_IDX
----------------------------------


DROP TABLE IF EXISTS Degree_MOVIE_INFO_IDX_INFO_TYPE_ID;

-- Create aggregator table Degree_MOVIE_INFO_IDX_INFO_TYPE_ID
CREATE TEMP TABLE Degree_MOVIE_INFO_IDX_INFO_TYPE_ID AS
SELECT
    MOVIE_INFO_IDX.INFO_TYPE_ID AS INFO_TYPE_ID,
    COUNT(*) AS deg
FROM MOVIE_INFO_IDX

WHERE MOVIE_INFO_IDX.INFO_TYPE_ID IS NOT NULL
GROUP BY
    MOVIE_INFO_IDX.INFO_TYPE_ID
HAVING COUNT(*) > 0
ORDER BY deg DESC
;

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_INFO_IDX' AS relation_name,
    'INFO_TYPE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    1 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_INFO_IDX_INFO_TYPE_ID
  LIMIT 1
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_INFO_IDX' AS relation_name,
    'INFO_TYPE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    2 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_INFO_IDX_INFO_TYPE_ID
  LIMIT 2
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_INFO_IDX' AS relation_name,
    'INFO_TYPE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    4 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_INFO_IDX_INFO_TYPE_ID
  LIMIT 4
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_INFO_IDX' AS relation_name,
    'INFO_TYPE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    5 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_INFO_IDX_INFO_TYPE_ID
  LIMIT 5
);

DROP TABLE IF EXISTS Degree_MOVIE_INFO_IDX_MOVIE_ID;

-- Create aggregator table Degree_MOVIE_INFO_IDX_MOVIE_ID
CREATE TEMP TABLE Degree_MOVIE_INFO_IDX_MOVIE_ID AS
SELECT
    MOVIE_INFO_IDX.MOVIE_ID AS MOVIE_ID,
    COUNT(*) AS deg
FROM MOVIE_INFO_IDX

WHERE MOVIE_INFO_IDX.MOVIE_ID IS NOT NULL
GROUP BY
    MOVIE_INFO_IDX.MOVIE_ID
HAVING COUNT(*) > 0
ORDER BY deg DESC
;

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_INFO_IDX' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    1 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_INFO_IDX_MOVIE_ID
  LIMIT 1
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_INFO_IDX' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    2 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_INFO_IDX_MOVIE_ID
  LIMIT 2
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_INFO_IDX' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    4 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_INFO_IDX_MOVIE_ID
  LIMIT 4
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_INFO_IDX' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    8 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_INFO_IDX_MOVIE_ID
  LIMIT 8
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_INFO_IDX' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    16 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_INFO_IDX_MOVIE_ID
  LIMIT 16
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_INFO_IDX' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    32 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_INFO_IDX_MOVIE_ID
  LIMIT 32
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_INFO_IDX' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    64 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_INFO_IDX_MOVIE_ID
  LIMIT 64
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_INFO_IDX' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    128 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_INFO_IDX_MOVIE_ID
  LIMIT 128
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_INFO_IDX' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    256 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_INFO_IDX_MOVIE_ID
  LIMIT 256
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_INFO_IDX' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    512 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_INFO_IDX_MOVIE_ID
  LIMIT 512
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_INFO_IDX' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    1024 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_INFO_IDX_MOVIE_ID
  LIMIT 1024
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_INFO_IDX' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    2048 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_INFO_IDX_MOVIE_ID
  LIMIT 2048
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_INFO_IDX' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    4096 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_INFO_IDX_MOVIE_ID
  LIMIT 4096
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_INFO_IDX' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    8192 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_INFO_IDX_MOVIE_ID
  LIMIT 8192
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_INFO_IDX' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    16384 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_INFO_IDX_MOVIE_ID
  LIMIT 16384
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_INFO_IDX' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    32768 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_INFO_IDX_MOVIE_ID
  LIMIT 32768
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_INFO_IDX' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    65536 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_INFO_IDX_MOVIE_ID
  LIMIT 65536
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_INFO_IDX' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    131072 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_INFO_IDX_MOVIE_ID
  LIMIT 131072
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_INFO_IDX' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    262144 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_INFO_IDX_MOVIE_ID
  LIMIT 262144
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_INFO_IDX' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    459925 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_INFO_IDX_MOVIE_ID
  LIMIT 459925
);

----------------------------------
-- START: generate all lpnorm queries for MOVIE_KEYWORD
----------------------------------


DROP TABLE IF EXISTS Degree_MOVIE_KEYWORD_KEYWORD_ID;

-- Create aggregator table Degree_MOVIE_KEYWORD_KEYWORD_ID
CREATE TEMP TABLE Degree_MOVIE_KEYWORD_KEYWORD_ID AS
SELECT
    MOVIE_KEYWORD.KEYWORD_ID AS KEYWORD_ID,
    COUNT(*) AS deg
FROM MOVIE_KEYWORD

WHERE MOVIE_KEYWORD.KEYWORD_ID IS NOT NULL
GROUP BY
    MOVIE_KEYWORD.KEYWORD_ID
HAVING COUNT(*) > 0
ORDER BY deg DESC
;

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_KEYWORD' AS relation_name,
    'KEYWORD_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    1 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_KEYWORD_KEYWORD_ID
  LIMIT 1
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_KEYWORD' AS relation_name,
    'KEYWORD_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    2 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_KEYWORD_KEYWORD_ID
  LIMIT 2
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_KEYWORD' AS relation_name,
    'KEYWORD_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    4 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_KEYWORD_KEYWORD_ID
  LIMIT 4
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_KEYWORD' AS relation_name,
    'KEYWORD_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    8 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_KEYWORD_KEYWORD_ID
  LIMIT 8
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_KEYWORD' AS relation_name,
    'KEYWORD_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    16 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_KEYWORD_KEYWORD_ID
  LIMIT 16
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_KEYWORD' AS relation_name,
    'KEYWORD_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    32 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_KEYWORD_KEYWORD_ID
  LIMIT 32
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_KEYWORD' AS relation_name,
    'KEYWORD_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    64 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_KEYWORD_KEYWORD_ID
  LIMIT 64
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_KEYWORD' AS relation_name,
    'KEYWORD_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    128 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_KEYWORD_KEYWORD_ID
  LIMIT 128
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_KEYWORD' AS relation_name,
    'KEYWORD_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    256 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_KEYWORD_KEYWORD_ID
  LIMIT 256
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_KEYWORD' AS relation_name,
    'KEYWORD_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    512 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_KEYWORD_KEYWORD_ID
  LIMIT 512
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_KEYWORD' AS relation_name,
    'KEYWORD_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    1024 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_KEYWORD_KEYWORD_ID
  LIMIT 1024
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_KEYWORD' AS relation_name,
    'KEYWORD_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    2048 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_KEYWORD_KEYWORD_ID
  LIMIT 2048
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_KEYWORD' AS relation_name,
    'KEYWORD_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    4096 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_KEYWORD_KEYWORD_ID
  LIMIT 4096
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_KEYWORD' AS relation_name,
    'KEYWORD_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    8192 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_KEYWORD_KEYWORD_ID
  LIMIT 8192
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_KEYWORD' AS relation_name,
    'KEYWORD_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    16384 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_KEYWORD_KEYWORD_ID
  LIMIT 16384
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_KEYWORD' AS relation_name,
    'KEYWORD_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    32768 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_KEYWORD_KEYWORD_ID
  LIMIT 32768
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_KEYWORD' AS relation_name,
    'KEYWORD_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    65536 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_KEYWORD_KEYWORD_ID
  LIMIT 65536
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_KEYWORD' AS relation_name,
    'KEYWORD_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    131072 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_KEYWORD_KEYWORD_ID
  LIMIT 131072
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_KEYWORD' AS relation_name,
    'KEYWORD_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    134170 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_KEYWORD_KEYWORD_ID
  LIMIT 134170
);

DROP TABLE IF EXISTS Degree_MOVIE_KEYWORD_MOVIE_ID;

-- Create aggregator table Degree_MOVIE_KEYWORD_MOVIE_ID
CREATE TEMP TABLE Degree_MOVIE_KEYWORD_MOVIE_ID AS
SELECT
    MOVIE_KEYWORD.MOVIE_ID AS MOVIE_ID,
    COUNT(*) AS deg
FROM MOVIE_KEYWORD

WHERE MOVIE_KEYWORD.MOVIE_ID IS NOT NULL
GROUP BY
    MOVIE_KEYWORD.MOVIE_ID
HAVING COUNT(*) > 0
ORDER BY deg DESC
;

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_KEYWORD' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    1 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_KEYWORD_MOVIE_ID
  LIMIT 1
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_KEYWORD' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    2 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_KEYWORD_MOVIE_ID
  LIMIT 2
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_KEYWORD' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    4 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_KEYWORD_MOVIE_ID
  LIMIT 4
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_KEYWORD' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    8 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_KEYWORD_MOVIE_ID
  LIMIT 8
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_KEYWORD' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    16 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_KEYWORD_MOVIE_ID
  LIMIT 16
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_KEYWORD' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    32 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_KEYWORD_MOVIE_ID
  LIMIT 32
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_KEYWORD' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    64 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_KEYWORD_MOVIE_ID
  LIMIT 64
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_KEYWORD' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    128 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_KEYWORD_MOVIE_ID
  LIMIT 128
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_KEYWORD' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    256 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_KEYWORD_MOVIE_ID
  LIMIT 256
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_KEYWORD' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    512 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_KEYWORD_MOVIE_ID
  LIMIT 512
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_KEYWORD' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    1024 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_KEYWORD_MOVIE_ID
  LIMIT 1024
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_KEYWORD' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    2048 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_KEYWORD_MOVIE_ID
  LIMIT 2048
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_KEYWORD' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    4096 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_KEYWORD_MOVIE_ID
  LIMIT 4096
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_KEYWORD' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    8192 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_KEYWORD_MOVIE_ID
  LIMIT 8192
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_KEYWORD' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    16384 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_KEYWORD_MOVIE_ID
  LIMIT 16384
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_KEYWORD' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    32768 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_KEYWORD_MOVIE_ID
  LIMIT 32768
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_KEYWORD' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    65536 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_KEYWORD_MOVIE_ID
  LIMIT 65536
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_KEYWORD' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    131072 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_KEYWORD_MOVIE_ID
  LIMIT 131072
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_KEYWORD' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    262144 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_KEYWORD_MOVIE_ID
  LIMIT 262144
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_KEYWORD' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    476794 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_KEYWORD_MOVIE_ID
  LIMIT 476794
);

----------------------------------
-- START: generate all lpnorm queries for MOVIE_LINK
----------------------------------


DROP TABLE IF EXISTS Degree_MOVIE_LINK_LINK_TYPE_ID;

-- Create aggregator table Degree_MOVIE_LINK_LINK_TYPE_ID
CREATE TEMP TABLE Degree_MOVIE_LINK_LINK_TYPE_ID AS
SELECT
    MOVIE_LINK.LINK_TYPE_ID AS LINK_TYPE_ID,
    COUNT(*) AS deg
FROM MOVIE_LINK

WHERE MOVIE_LINK.LINK_TYPE_ID IS NOT NULL
GROUP BY
    MOVIE_LINK.LINK_TYPE_ID
HAVING COUNT(*) > 0
ORDER BY deg DESC
;

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_LINK' AS relation_name,
    'LINK_TYPE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    1 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_LINK_LINK_TYPE_ID
  LIMIT 1
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_LINK' AS relation_name,
    'LINK_TYPE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    2 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_LINK_LINK_TYPE_ID
  LIMIT 2
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_LINK' AS relation_name,
    'LINK_TYPE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    4 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_LINK_LINK_TYPE_ID
  LIMIT 4
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_LINK' AS relation_name,
    'LINK_TYPE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    8 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_LINK_LINK_TYPE_ID
  LIMIT 8
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_LINK' AS relation_name,
    'LINK_TYPE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    16 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_LINK_LINK_TYPE_ID
  LIMIT 16
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_LINK' AS relation_name,
    'LINK_TYPE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    16 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_LINK_LINK_TYPE_ID
  LIMIT 16
);

DROP TABLE IF EXISTS Degree_MOVIE_LINK_LINKED_MOVIE_ID;

-- Create aggregator table Degree_MOVIE_LINK_LINKED_MOVIE_ID
CREATE TEMP TABLE Degree_MOVIE_LINK_LINKED_MOVIE_ID AS
SELECT
    MOVIE_LINK.LINKED_MOVIE_ID AS LINKED_MOVIE_ID,
    COUNT(*) AS deg
FROM MOVIE_LINK

WHERE MOVIE_LINK.LINKED_MOVIE_ID IS NOT NULL
GROUP BY
    MOVIE_LINK.LINKED_MOVIE_ID
HAVING COUNT(*) > 0
ORDER BY deg DESC
;

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_LINK' AS relation_name,
    'LINKED_MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    1 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_LINK_LINKED_MOVIE_ID
  LIMIT 1
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_LINK' AS relation_name,
    'LINKED_MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    2 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_LINK_LINKED_MOVIE_ID
  LIMIT 2
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_LINK' AS relation_name,
    'LINKED_MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    4 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_LINK_LINKED_MOVIE_ID
  LIMIT 4
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_LINK' AS relation_name,
    'LINKED_MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    8 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_LINK_LINKED_MOVIE_ID
  LIMIT 8
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_LINK' AS relation_name,
    'LINKED_MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    16 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_LINK_LINKED_MOVIE_ID
  LIMIT 16
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_LINK' AS relation_name,
    'LINKED_MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    32 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_LINK_LINKED_MOVIE_ID
  LIMIT 32
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_LINK' AS relation_name,
    'LINKED_MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    64 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_LINK_LINKED_MOVIE_ID
  LIMIT 64
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_LINK' AS relation_name,
    'LINKED_MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    128 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_LINK_LINKED_MOVIE_ID
  LIMIT 128
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_LINK' AS relation_name,
    'LINKED_MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    256 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_LINK_LINKED_MOVIE_ID
  LIMIT 256
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_LINK' AS relation_name,
    'LINKED_MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    512 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_LINK_LINKED_MOVIE_ID
  LIMIT 512
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_LINK' AS relation_name,
    'LINKED_MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    1024 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_LINK_LINKED_MOVIE_ID
  LIMIT 1024
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_LINK' AS relation_name,
    'LINKED_MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    2048 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_LINK_LINKED_MOVIE_ID
  LIMIT 2048
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_LINK' AS relation_name,
    'LINKED_MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    4096 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_LINK_LINKED_MOVIE_ID
  LIMIT 4096
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_LINK' AS relation_name,
    'LINKED_MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    8192 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_LINK_LINKED_MOVIE_ID
  LIMIT 8192
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_LINK' AS relation_name,
    'LINKED_MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    16169 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_LINK_LINKED_MOVIE_ID
  LIMIT 16169
);

DROP TABLE IF EXISTS Degree_MOVIE_LINK_MOVIE_ID;

-- Create aggregator table Degree_MOVIE_LINK_MOVIE_ID
CREATE TEMP TABLE Degree_MOVIE_LINK_MOVIE_ID AS
SELECT
    MOVIE_LINK.MOVIE_ID AS MOVIE_ID,
    COUNT(*) AS deg
FROM MOVIE_LINK

WHERE MOVIE_LINK.MOVIE_ID IS NOT NULL
GROUP BY
    MOVIE_LINK.MOVIE_ID
HAVING COUNT(*) > 0
ORDER BY deg DESC
;

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_LINK' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    1 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_LINK_MOVIE_ID
  LIMIT 1
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_LINK' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    2 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_LINK_MOVIE_ID
  LIMIT 2
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_LINK' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    4 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_LINK_MOVIE_ID
  LIMIT 4
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_LINK' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    8 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_LINK_MOVIE_ID
  LIMIT 8
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_LINK' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    16 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_LINK_MOVIE_ID
  LIMIT 16
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_LINK' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    32 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_LINK_MOVIE_ID
  LIMIT 32
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_LINK' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    64 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_LINK_MOVIE_ID
  LIMIT 64
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_LINK' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    128 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_LINK_MOVIE_ID
  LIMIT 128
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_LINK' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    256 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_LINK_MOVIE_ID
  LIMIT 256
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_LINK' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    512 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_LINK_MOVIE_ID
  LIMIT 512
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_LINK' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    1024 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_LINK_MOVIE_ID
  LIMIT 1024
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_LINK' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    2048 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_LINK_MOVIE_ID
  LIMIT 2048
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_LINK' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    4096 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_LINK_MOVIE_ID
  LIMIT 4096
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_LINK' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    6411 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_LINK_MOVIE_ID
  LIMIT 6411
);

----------------------------------
-- START: generate all lpnorm queries for NAME
----------------------------------


DROP TABLE IF EXISTS Degree_NAME_ID;

-- Create aggregator table Degree_NAME_ID
CREATE TEMP TABLE Degree_NAME_ID AS
SELECT
    NAME.ID AS ID,
    COUNT(*) AS deg
FROM NAME

WHERE NAME.ID IS NOT NULL
GROUP BY
    NAME.ID
HAVING COUNT(*) > 0
ORDER BY deg DESC
;

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'NAME' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    1 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_NAME_ID
  LIMIT 1
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'NAME' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    2 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_NAME_ID
  LIMIT 2
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'NAME' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    4 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_NAME_ID
  LIMIT 4
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'NAME' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    8 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_NAME_ID
  LIMIT 8
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'NAME' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    16 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_NAME_ID
  LIMIT 16
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'NAME' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    32 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_NAME_ID
  LIMIT 32
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'NAME' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    64 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_NAME_ID
  LIMIT 64
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'NAME' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    128 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_NAME_ID
  LIMIT 128
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'NAME' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    256 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_NAME_ID
  LIMIT 256
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'NAME' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    512 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_NAME_ID
  LIMIT 512
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'NAME' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    1024 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_NAME_ID
  LIMIT 1024
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'NAME' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    2048 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_NAME_ID
  LIMIT 2048
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'NAME' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    4096 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_NAME_ID
  LIMIT 4096
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'NAME' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    8192 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_NAME_ID
  LIMIT 8192
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'NAME' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    16384 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_NAME_ID
  LIMIT 16384
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'NAME' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    32768 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_NAME_ID
  LIMIT 32768
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'NAME' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    65536 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_NAME_ID
  LIMIT 65536
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'NAME' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    131072 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_NAME_ID
  LIMIT 131072
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'NAME' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    262144 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_NAME_ID
  LIMIT 262144
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'NAME' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    524288 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_NAME_ID
  LIMIT 524288
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'NAME' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    1048576 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_NAME_ID
  LIMIT 1048576
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'NAME' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    2097152 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_NAME_ID
  LIMIT 2097152
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'NAME' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    4167491 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_NAME_ID
  LIMIT 4167491
);

----------------------------------
-- START: generate all lpnorm queries for ROLE_TYPE
----------------------------------


DROP TABLE IF EXISTS Degree_ROLE_TYPE_ID;

-- Create aggregator table Degree_ROLE_TYPE_ID
CREATE TEMP TABLE Degree_ROLE_TYPE_ID AS
SELECT
    ROLE_TYPE.ID AS ID,
    COUNT(*) AS deg
FROM ROLE_TYPE

WHERE ROLE_TYPE.ID IS NOT NULL
GROUP BY
    ROLE_TYPE.ID
HAVING COUNT(*) > 0
ORDER BY deg DESC
;

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'ROLE_TYPE' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    1 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_ROLE_TYPE_ID
  LIMIT 1
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'ROLE_TYPE' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    2 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_ROLE_TYPE_ID
  LIMIT 2
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'ROLE_TYPE' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    4 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_ROLE_TYPE_ID
  LIMIT 4
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'ROLE_TYPE' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    8 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_ROLE_TYPE_ID
  LIMIT 8
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'ROLE_TYPE' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    12 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_ROLE_TYPE_ID
  LIMIT 12
);

----------------------------------
-- START: generate all lpnorm queries for TITLE
----------------------------------


DROP TABLE IF EXISTS Degree_TITLE_ID;

-- Create aggregator table Degree_TITLE_ID
CREATE TEMP TABLE Degree_TITLE_ID AS
SELECT
    TITLE.ID AS ID,
    COUNT(*) AS deg
FROM TITLE

WHERE TITLE.ID IS NOT NULL
GROUP BY
    TITLE.ID
HAVING COUNT(*) > 0
ORDER BY deg DESC
;

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'TITLE' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    1 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_TITLE_ID
  LIMIT 1
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'TITLE' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    2 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_TITLE_ID
  LIMIT 2
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'TITLE' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    4 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_TITLE_ID
  LIMIT 4
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'TITLE' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    8 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_TITLE_ID
  LIMIT 8
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'TITLE' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    16 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_TITLE_ID
  LIMIT 16
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'TITLE' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    32 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_TITLE_ID
  LIMIT 32
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'TITLE' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    64 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_TITLE_ID
  LIMIT 64
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'TITLE' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    128 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_TITLE_ID
  LIMIT 128
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'TITLE' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    256 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_TITLE_ID
  LIMIT 256
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'TITLE' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    512 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_TITLE_ID
  LIMIT 512
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'TITLE' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    1024 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_TITLE_ID
  LIMIT 1024
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'TITLE' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    2048 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_TITLE_ID
  LIMIT 2048
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'TITLE' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    4096 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_TITLE_ID
  LIMIT 4096
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'TITLE' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    8192 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_TITLE_ID
  LIMIT 8192
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'TITLE' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    16384 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_TITLE_ID
  LIMIT 16384
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'TITLE' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    32768 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_TITLE_ID
  LIMIT 32768
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'TITLE' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    65536 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_TITLE_ID
  LIMIT 65536
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'TITLE' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    131072 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_TITLE_ID
  LIMIT 131072
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'TITLE' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    262144 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_TITLE_ID
  LIMIT 262144
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'TITLE' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    524288 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_TITLE_ID
  LIMIT 524288
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'TITLE' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    1048576 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_TITLE_ID
  LIMIT 1048576
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'TITLE' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    2097152 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_TITLE_ID
  LIMIT 2097152
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'TITLE' AS relation_name,
    'ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    2528312 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_TITLE_ID
  LIMIT 2528312
);

DROP TABLE IF EXISTS Degree_TITLE_KIND_ID;

-- Create aggregator table Degree_TITLE_KIND_ID
CREATE TEMP TABLE Degree_TITLE_KIND_ID AS
SELECT
    TITLE.KIND_ID AS KIND_ID,
    COUNT(*) AS deg
FROM TITLE

WHERE TITLE.KIND_ID IS NOT NULL
GROUP BY
    TITLE.KIND_ID
HAVING COUNT(*) > 0
ORDER BY deg DESC
;

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'TITLE' AS relation_name,
    'KIND_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    1 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_TITLE_KIND_ID
  LIMIT 1
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'TITLE' AS relation_name,
    'KIND_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    2 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_TITLE_KIND_ID
  LIMIT 2
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'TITLE' AS relation_name,
    'KIND_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    4 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_TITLE_KIND_ID
  LIMIT 4
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'TITLE' AS relation_name,
    'KIND_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    6 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_TITLE_KIND_ID
  LIMIT 6
);

----------------------------------
-- START: generate all lpnorm queries for MOVIE_INFO
----------------------------------


DROP TABLE IF EXISTS Degree_MOVIE_INFO_INFO_TYPE_ID;

-- Create aggregator table Degree_MOVIE_INFO_INFO_TYPE_ID
CREATE TEMP TABLE Degree_MOVIE_INFO_INFO_TYPE_ID AS
SELECT
    MOVIE_INFO.INFO_TYPE_ID AS INFO_TYPE_ID,
    COUNT(*) AS deg
FROM MOVIE_INFO

WHERE MOVIE_INFO.INFO_TYPE_ID IS NOT NULL
GROUP BY
    MOVIE_INFO.INFO_TYPE_ID
HAVING COUNT(*) > 0
ORDER BY deg DESC
;

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_INFO' AS relation_name,
    'INFO_TYPE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    1 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_INFO_INFO_TYPE_ID
  LIMIT 1
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_INFO' AS relation_name,
    'INFO_TYPE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    2 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_INFO_INFO_TYPE_ID
  LIMIT 2
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_INFO' AS relation_name,
    'INFO_TYPE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    4 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_INFO_INFO_TYPE_ID
  LIMIT 4
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_INFO' AS relation_name,
    'INFO_TYPE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    8 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_INFO_INFO_TYPE_ID
  LIMIT 8
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_INFO' AS relation_name,
    'INFO_TYPE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    16 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_INFO_INFO_TYPE_ID
  LIMIT 16
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_INFO' AS relation_name,
    'INFO_TYPE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    32 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_INFO_INFO_TYPE_ID
  LIMIT 32
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_INFO' AS relation_name,
    'INFO_TYPE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    64 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_INFO_INFO_TYPE_ID
  LIMIT 64
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_INFO' AS relation_name,
    'INFO_TYPE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    71 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_INFO_INFO_TYPE_ID
  LIMIT 71
);

DROP TABLE IF EXISTS Degree_MOVIE_INFO_MOVIE_ID;

-- Create aggregator table Degree_MOVIE_INFO_MOVIE_ID
CREATE TEMP TABLE Degree_MOVIE_INFO_MOVIE_ID AS
SELECT
    MOVIE_INFO.MOVIE_ID AS MOVIE_ID,
    COUNT(*) AS deg
FROM MOVIE_INFO

WHERE MOVIE_INFO.MOVIE_ID IS NOT NULL
GROUP BY
    MOVIE_INFO.MOVIE_ID
HAVING COUNT(*) > 0
ORDER BY deg DESC
;

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_INFO' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    1 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_INFO_MOVIE_ID
  LIMIT 1
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_INFO' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    2 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_INFO_MOVIE_ID
  LIMIT 2
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_INFO' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    4 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_INFO_MOVIE_ID
  LIMIT 4
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_INFO' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    8 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_INFO_MOVIE_ID
  LIMIT 8
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_INFO' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    16 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_INFO_MOVIE_ID
  LIMIT 16
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_INFO' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    32 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_INFO_MOVIE_ID
  LIMIT 32
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_INFO' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    64 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_INFO_MOVIE_ID
  LIMIT 64
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_INFO' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    128 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_INFO_MOVIE_ID
  LIMIT 128
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_INFO' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    256 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_INFO_MOVIE_ID
  LIMIT 256
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_INFO' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    512 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_INFO_MOVIE_ID
  LIMIT 512
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_INFO' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    1024 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_INFO_MOVIE_ID
  LIMIT 1024
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_INFO' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    2048 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_INFO_MOVIE_ID
  LIMIT 2048
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_INFO' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    4096 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_INFO_MOVIE_ID
  LIMIT 4096
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_INFO' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    8192 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_INFO_MOVIE_ID
  LIMIT 8192
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_INFO' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    16384 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_INFO_MOVIE_ID
  LIMIT 16384
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_INFO' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    32768 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_INFO_MOVIE_ID
  LIMIT 32768
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_INFO' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    65536 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_INFO_MOVIE_ID
  LIMIT 65536
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_INFO' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    131072 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_INFO_MOVIE_ID
  LIMIT 131072
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_INFO' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    262144 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_INFO_MOVIE_ID
  LIMIT 262144
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_INFO' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    524288 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_INFO_MOVIE_ID
  LIMIT 524288
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_INFO' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    1048576 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_INFO_MOVIE_ID
  LIMIT 1048576
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_INFO' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    2097152 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_INFO_MOVIE_ID
  LIMIT 2097152
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_INFO' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    2468825 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_MOVIE_INFO_MOVIE_ID
  LIMIT 2468825
);

----------------------------------
-- START: generate all lpnorm queries for PERSON_INFO
----------------------------------


DROP TABLE IF EXISTS Degree_PERSON_INFO_INFO_TYPE_ID;

-- Create aggregator table Degree_PERSON_INFO_INFO_TYPE_ID
CREATE TEMP TABLE Degree_PERSON_INFO_INFO_TYPE_ID AS
SELECT
    PERSON_INFO.INFO_TYPE_ID AS INFO_TYPE_ID,
    COUNT(*) AS deg
FROM PERSON_INFO

WHERE PERSON_INFO.INFO_TYPE_ID IS NOT NULL
GROUP BY
    PERSON_INFO.INFO_TYPE_ID
HAVING COUNT(*) > 0
ORDER BY deg DESC
;

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'PERSON_INFO' AS relation_name,
    'INFO_TYPE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    1 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_PERSON_INFO_INFO_TYPE_ID
  LIMIT 1
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'PERSON_INFO' AS relation_name,
    'INFO_TYPE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    2 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_PERSON_INFO_INFO_TYPE_ID
  LIMIT 2
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'PERSON_INFO' AS relation_name,
    'INFO_TYPE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    4 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_PERSON_INFO_INFO_TYPE_ID
  LIMIT 4
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'PERSON_INFO' AS relation_name,
    'INFO_TYPE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    8 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_PERSON_INFO_INFO_TYPE_ID
  LIMIT 8
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'PERSON_INFO' AS relation_name,
    'INFO_TYPE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    16 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_PERSON_INFO_INFO_TYPE_ID
  LIMIT 16
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'PERSON_INFO' AS relation_name,
    'INFO_TYPE_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    22 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_PERSON_INFO_INFO_TYPE_ID
  LIMIT 22
);

DROP TABLE IF EXISTS Degree_PERSON_INFO_PERSON_ID;

-- Create aggregator table Degree_PERSON_INFO_PERSON_ID
CREATE TEMP TABLE Degree_PERSON_INFO_PERSON_ID AS
SELECT
    PERSON_INFO.PERSON_ID AS PERSON_ID,
    COUNT(*) AS deg
FROM PERSON_INFO

WHERE PERSON_INFO.PERSON_ID IS NOT NULL
GROUP BY
    PERSON_INFO.PERSON_ID
HAVING COUNT(*) > 0
ORDER BY deg DESC
;

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'PERSON_INFO' AS relation_name,
    'PERSON_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    1 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_PERSON_INFO_PERSON_ID
  LIMIT 1
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'PERSON_INFO' AS relation_name,
    'PERSON_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    2 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_PERSON_INFO_PERSON_ID
  LIMIT 2
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'PERSON_INFO' AS relation_name,
    'PERSON_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    4 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_PERSON_INFO_PERSON_ID
  LIMIT 4
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'PERSON_INFO' AS relation_name,
    'PERSON_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    8 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_PERSON_INFO_PERSON_ID
  LIMIT 8
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'PERSON_INFO' AS relation_name,
    'PERSON_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    16 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_PERSON_INFO_PERSON_ID
  LIMIT 16
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'PERSON_INFO' AS relation_name,
    'PERSON_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    32 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_PERSON_INFO_PERSON_ID
  LIMIT 32
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'PERSON_INFO' AS relation_name,
    'PERSON_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    64 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_PERSON_INFO_PERSON_ID
  LIMIT 64
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'PERSON_INFO' AS relation_name,
    'PERSON_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    128 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_PERSON_INFO_PERSON_ID
  LIMIT 128
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'PERSON_INFO' AS relation_name,
    'PERSON_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    256 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_PERSON_INFO_PERSON_ID
  LIMIT 256
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'PERSON_INFO' AS relation_name,
    'PERSON_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    512 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_PERSON_INFO_PERSON_ID
  LIMIT 512
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'PERSON_INFO' AS relation_name,
    'PERSON_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    1024 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_PERSON_INFO_PERSON_ID
  LIMIT 1024
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'PERSON_INFO' AS relation_name,
    'PERSON_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    2048 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_PERSON_INFO_PERSON_ID
  LIMIT 2048
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'PERSON_INFO' AS relation_name,
    'PERSON_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    4096 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_PERSON_INFO_PERSON_ID
  LIMIT 4096
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'PERSON_INFO' AS relation_name,
    'PERSON_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    8192 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_PERSON_INFO_PERSON_ID
  LIMIT 8192
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'PERSON_INFO' AS relation_name,
    'PERSON_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    16384 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_PERSON_INFO_PERSON_ID
  LIMIT 16384
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'PERSON_INFO' AS relation_name,
    'PERSON_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    32768 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_PERSON_INFO_PERSON_ID
  LIMIT 32768
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'PERSON_INFO' AS relation_name,
    'PERSON_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    65536 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_PERSON_INFO_PERSON_ID
  LIMIT 65536
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'PERSON_INFO' AS relation_name,
    'PERSON_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    131072 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_PERSON_INFO_PERSON_ID
  LIMIT 131072
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'PERSON_INFO' AS relation_name,
    'PERSON_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    262144 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_PERSON_INFO_PERSON_ID
  LIMIT 262144
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'PERSON_INFO' AS relation_name,
    'PERSON_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    524288 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_PERSON_INFO_PERSON_ID
  LIMIT 524288
);

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    
    pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'PERSON_INFO' AS relation_name,
    'PERSON_ID' AS join_var_name,
    'NOPRED' AS aggregator_name,
    NULL AS pred_col_name,
    
    550721 AS pred_value_id,
  COUNT(*) AS l0,
  SUM(POWER(deg, 1)) AS l1,
  SUM(POWER(deg, 2)) AS l2,
  SUM(POWER(deg, 3)) AS l3,
  SUM(POWER(deg, 4)) AS l4,
  SUM(POWER(deg, 5)) AS l5,
  SUM(POWER(deg, 6)) AS l6,
  SUM(POWER(deg, 7)) AS l7,
  SUM(POWER(deg, 8)) AS l8,
  SUM(POWER(deg, 9)) AS l9,
  SUM(POWER(deg, 10)) AS l10,
  MAX(deg) AS l_inf
FROM (
  SELECT * FROM Degree_PERSON_INFO_PERSON_ID
  LIMIT 550721
);

