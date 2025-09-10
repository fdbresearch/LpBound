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


DROP TABLE IF EXISTS CAST_INFO_ROLE_ID_mcv;

-- Create MCV table for CAST_INFO.ROLE_ID, with a unique mcv_id
CREATE TABLE CAST_INFO_ROLE_ID_mcv AS
WITH base AS (
    SELECT ROLE_ID, COUNT(*) AS freq
    FROM CAST_INFO
    WHERE ROLE_ID IS NOT NULL
    GROUP BY ROLE_ID
    ORDER BY freq DESC
    LIMIT 5000
)
SELECT
    ROW_NUMBER() OVER (ORDER BY freq DESC) AS mcv_id,
    ROLE_ID,
    freq
FROM base;

DROP TABLE IF EXISTS MOVIE_COMPANIES_COMPANY_ID_mcv;

-- Create MCV table for MOVIE_COMPANIES.COMPANY_ID, with a unique mcv_id
CREATE TABLE MOVIE_COMPANIES_COMPANY_ID_mcv AS
WITH base AS (
    SELECT COMPANY_ID, COUNT(*) AS freq
    FROM MOVIE_COMPANIES
    WHERE COMPANY_ID IS NOT NULL
    GROUP BY COMPANY_ID
    ORDER BY freq DESC
    LIMIT 5000
)
SELECT
    ROW_NUMBER() OVER (ORDER BY freq DESC) AS mcv_id,
    COMPANY_ID,
    freq
FROM base;

DROP TABLE IF EXISTS MOVIE_COMPANIES_COMPANY_TYPE_ID_mcv;

-- Create MCV table for MOVIE_COMPANIES.COMPANY_TYPE_ID, with a unique mcv_id
CREATE TABLE MOVIE_COMPANIES_COMPANY_TYPE_ID_mcv AS
WITH base AS (
    SELECT COMPANY_TYPE_ID, COUNT(*) AS freq
    FROM MOVIE_COMPANIES
    WHERE COMPANY_TYPE_ID IS NOT NULL
    GROUP BY COMPANY_TYPE_ID
    ORDER BY freq DESC
    LIMIT 5000
)
SELECT
    ROW_NUMBER() OVER (ORDER BY freq DESC) AS mcv_id,
    COMPANY_TYPE_ID,
    freq
FROM base;

DROP TABLE IF EXISTS MOVIE_INFO_IDX_INFO_TYPE_ID_mcv;

-- Create MCV table for MOVIE_INFO_IDX.INFO_TYPE_ID, with a unique mcv_id
CREATE TABLE MOVIE_INFO_IDX_INFO_TYPE_ID_mcv AS
WITH base AS (
    SELECT INFO_TYPE_ID, COUNT(*) AS freq
    FROM MOVIE_INFO_IDX
    WHERE INFO_TYPE_ID IS NOT NULL
    GROUP BY INFO_TYPE_ID
    ORDER BY freq DESC
    LIMIT 5000
)
SELECT
    ROW_NUMBER() OVER (ORDER BY freq DESC) AS mcv_id,
    INFO_TYPE_ID,
    freq
FROM base;

DROP TABLE IF EXISTS MOVIE_KEYWORD_KEYWORD_ID_mcv;

-- Create MCV table for MOVIE_KEYWORD.KEYWORD_ID, with a unique mcv_id
CREATE TABLE MOVIE_KEYWORD_KEYWORD_ID_mcv AS
WITH base AS (
    SELECT KEYWORD_ID, COUNT(*) AS freq
    FROM MOVIE_KEYWORD
    WHERE KEYWORD_ID IS NOT NULL
    GROUP BY KEYWORD_ID
    ORDER BY freq DESC
    LIMIT 5000
)
SELECT
    ROW_NUMBER() OVER (ORDER BY freq DESC) AS mcv_id,
    KEYWORD_ID,
    freq
FROM base;

DROP TABLE IF EXISTS TITLE_KIND_ID_mcv;

-- Create MCV table for TITLE.KIND_ID, with a unique mcv_id
CREATE TABLE TITLE_KIND_ID_mcv AS
WITH base AS (
    SELECT KIND_ID, COUNT(*) AS freq
    FROM TITLE
    WHERE KIND_ID IS NOT NULL
    GROUP BY KIND_ID
    ORDER BY freq DESC
    LIMIT 5000
)
SELECT
    ROW_NUMBER() OVER (ORDER BY freq DESC) AS mcv_id,
    KIND_ID,
    freq
FROM base;

DROP TABLE IF EXISTS TITLE_PRODUCTION_YEAR_mcv;

-- Create MCV table for TITLE.PRODUCTION_YEAR, with a unique mcv_id
CREATE TABLE TITLE_PRODUCTION_YEAR_mcv AS
WITH base AS (
    SELECT PRODUCTION_YEAR, COUNT(*) AS freq
    FROM TITLE
    WHERE PRODUCTION_YEAR IS NOT NULL
    GROUP BY PRODUCTION_YEAR
    ORDER BY freq DESC
    LIMIT 5000
)
SELECT
    ROW_NUMBER() OVER (ORDER BY freq DESC) AS mcv_id,
    PRODUCTION_YEAR,
    freq
FROM base;

DROP TABLE IF EXISTS TITLE_PRODUCTION_YEAR_histogram;
CREATE TABLE TITLE_PRODUCTION_YEAR_histogram (
    bucket_id   INT NOT NULL,
    layer       INT NOT NULL,
    lower_bound INT,
    upper_bound INT,
    bucket_type TEXT
);


INSERT INTO TITLE_PRODUCTION_YEAR_histogram (bucket_id, layer, lower_bound, upper_bound, bucket_type)
VALUES (0, 1, NULL, 1888, 'base_1'), (1, 1, 1888, 1889, 'base_1'), (2, 1, 1889, 1890, 'base_1'), (3, 1, 1890, 1891, 'base_1'), (4, 1, 1891, 1892, 'base_1'), (5, 1, 1892, 1893, 'base_1'), (6, 1, 1893, 1894, 'base_1'), (7, 1, 1894, 1895, 'base_1'), (8, 1, 1895, 1896, 'base_1'), (9, 1, 1896, 1897, 'base_1'), (10, 1, 1897, 1898, 'base_1'), (11, 1, 1898, 1899, 'base_1'), (12, 1, 1899, 1900, 'base_1'), (13, 1, 1900, 1901, 'base_1'), (14, 1, 1901, 1902, 'base_1'), (15, 1, 1902, 1903, 'base_1'), (16, 1, 1903, 1904, 'base_1'), (17, 1, 1904, 1905, 'base_1'), (18, 1, 1905, 1906, 'base_1'), (19, 1, 1906, 1907, 'base_1'), (20, 1, 1907, 1908, 'base_1'), (21, 1, 1908, 1909, 'base_1'), (22, 1, 1909, 1910, 'base_1'), (23, 1, 1910, 1911, 'base_1'), (24, 1, 1911, 1912, 'base_1'), (25, 1, 1912, 1913, 'base_1'), (26, 1, 1913, 1914, 'base_1'), (27, 1, 1914, 1915, 'base_1'), (28, 1, 1915, 1916, 'base_1'), (29, 1, 1916, 1917, 'base_1'), (30, 1, 1917, 1918, 'base_1'), (31, 1, 1918, 1919, 'base_1'), (32, 1, 1919, 1920, 'base_1'), (33, 1, 1920, 1921, 'base_1'), (34, 1, 1921, 1922, 'base_1'), (35, 1, 1922, 1923, 'base_1'), (36, 1, 1923, 1924, 'base_1'), (37, 1, 1924, 1925, 'base_1'), (38, 1, 1925, 1926, 'base_1'), (39, 1, 1926, 1927, 'base_1'), (40, 1, 1927, 1928, 'base_1'), (41, 1, 1928, 1929, 'base_1'), (42, 1, 1929, 1931, 'base_1'), (43, 1, 1931, 1932, 'base_1'), (44, 1, 1932, 1933, 'base_1'), (45, 1, 1933, 1934, 'base_1'), (46, 1, 1934, 1935, 'base_1'), (47, 1, 1935, 1936, 'base_1'), (48, 1, 1936, 1937, 'base_1'), (49, 1, 1937, 1938, 'base_1'), (50, 1, 1938, 1939, 'base_1'), (51, 1, 1939, 1940, 'base_1'), (52, 1, 1940, 1941, 'base_1'), (53, 1, 1941, 1942, 'base_1'), (54, 1, 1942, 1943, 'base_1'), (55, 1, 1943, 1944, 'base_1'), (56, 1, 1944, 1945, 'base_1'), (57, 1, 1945, 1946, 'base_1'), (58, 1, 1946, 1947, 'base_1'), (59, 1, 1947, 1948, 'base_1'), (60, 1, 1948, 1949, 'base_1'), (61, 1, 1949, 1950, 'base_1'), (62, 1, 1950, 1951, 'base_1'), (63, 1, 1951, 1952, 'base_1'), (64, 1, 1952, 1953, 'base_1'), (65, 1, 1953, 1954, 'base_1'), (66, 1, 1954, 1955, 'base_1'), (67, 1, 1955, 1956, 'base_1'), (68, 1, 1956, 1957, 'base_1'), (69, 1, 1957, 1958, 'base_1'), (70, 1, 1958, 1959, 'base_1'), (71, 1, 1959, 1960, 'base_1'), (72, 1, 1960, 1961, 'base_1'), (73, 1, 1961, 1962, 'base_1'), (74, 1, 1962, 1963, 'base_1'), (75, 1, 1963, 1964, 'base_1'), (76, 1, 1964, 1965, 'base_1'), (77, 1, 1965, 1966, 'base_1'), (78, 1, 1966, 1967, 'base_1'), (79, 1, 1967, 1968, 'base_1'), (80, 1, 1968, 1969, 'base_1'), (81, 1, 1969, 1970, 'base_1'), (82, 1, 1970, 1971, 'base_1'), (83, 1, 1971, 1972, 'base_1'), (84, 1, 1972, 1973, 'base_1'), (85, 1, 1973, 1975, 'base_1'), (86, 1, 1975, 1976, 'base_1'), (87, 1, 1976, 1977, 'base_1'), (88, 1, 1977, 1978, 'base_1'), (89, 1, 1978, 1979, 'base_1'), (90, 1, 1979, 1980, 'base_1'), (91, 1, 1980, 1981, 'base_1'), (92, 1, 1981, 1982, 'base_1'), (93, 1, 1982, 1983, 'base_1'), (94, 1, 1983, 1984, 'base_1'), (95, 1, 1984, 1985, 'base_1'), (96, 1, 1985, 1986, 'base_1'), (97, 1, 1986, 1987, 'base_1'), (98, 1, 1987, 1988, 'base_1'), (99, 1, 1988, 1989, 'base_1'), (100, 1, 1989, 1990, 'base_1'), (101, 1, 1990, 1991, 'base_1'), (102, 1, 1991, 1992, 'base_1'), (103, 1, 1992, 1993, 'base_1'), (104, 1, 1993, 1994, 'base_1'), (105, 1, 1994, 1995, 'base_1'), (106, 1, 1995, 1996, 'base_1'), (107, 1, 1996, 1997, 'base_1'), (108, 1, 1997, 1998, 'base_1'), (109, 1, 1998, 1999, 'base_1'), (110, 1, 1999, 2000, 'base_1'), (111, 1, 2000, 2001, 'base_1'), (112, 1, 2001, 2002, 'base_1'), (113, 1, 2002, 2003, 'base_1'), (114, 1, 2003, 2004, 'base_1'), (115, 1, 2004, 2005, 'base_1'), (116, 1, 2005, 2006, 'base_1'), (117, 1, 2006, 2007, 'base_1'), (118, 1, 2007, 2008, 'base_1'), (119, 1, 2008, 2009, 'base_1'), (120, 1, 2009, 2010, 'base_1'), (121, 1, 2010, 2011, 'base_1'), (122, 1, 2011, 2012, 'base_1'), (123, 1, 2012, 2013, 'base_1'), (124, 1, 2013, 2014, 'base_1'), (125, 1, 2014, 2015, 'base_1'), (126, 1, 2015, 2016, 'base_1'), (127, 1, 2016, 2017, 'base_1'), (128, 1, 2017, NULL, 'base_1'), (129, 2, NULL, 1889, 'base_2'), (130, 2, 1889, 1891, 'base_2'), (131, 2, 1891, 1893, 'base_2'), (132, 2, 1893, 1895, 'base_2'), (133, 2, 1895, 1897, 'base_2'), (134, 2, 1897, 1899, 'base_2'), (135, 2, 1899, 1901, 'base_2'), (136, 2, 1901, 1903, 'base_2'), (137, 2, 1903, 1905, 'base_2'), (138, 2, 1905, 1907, 'base_2'), (139, 2, 1907, 1909, 'base_2'), (140, 2, 1909, 1911, 'base_2'), (141, 2, 1911, 1913, 'base_2'), (142, 2, 1913, 1915, 'base_2'), (143, 2, 1915, 1917, 'base_2'), (144, 2, 1917, 1919, 'base_2'), (145, 2, 1919, 1921, 'base_2'), (146, 2, 1921, 1923, 'base_2'), (147, 2, 1923, 1925, 'base_2'), (148, 2, 1925, 1927, 'base_2'), (149, 2, 1927, 1929, 'base_2'), (150, 2, 1929, 1932, 'base_2'), (151, 2, 1932, 1934, 'base_2'), (152, 2, 1934, 1936, 'base_2'), (153, 2, 1936, 1938, 'base_2'), (154, 2, 1938, 1940, 'base_2'), (155, 2, 1940, 1942, 'base_2'), (156, 2, 1942, 1944, 'base_2'), (157, 2, 1944, 1946, 'base_2'), (158, 2, 1946, 1948, 'base_2'), (159, 2, 1948, 1950, 'base_2'), (160, 2, 1950, 1952, 'base_2'), (161, 2, 1952, 1954, 'base_2'), (162, 2, 1954, 1956, 'base_2'), (163, 2, 1956, 1958, 'base_2'), (164, 2, 1958, 1960, 'base_2'), (165, 2, 1960, 1962, 'base_2'), (166, 2, 1962, 1964, 'base_2'), (167, 2, 1964, 1966, 'base_2'), (168, 2, 1966, 1968, 'base_2'), (169, 2, 1968, 1970, 'base_2'), (170, 2, 1970, 1972, 'base_2'), (171, 2, 1972, 1975, 'base_2'), (172, 2, 1975, 1977, 'base_2'), (173, 2, 1977, 1979, 'base_2'), (174, 2, 1979, 1981, 'base_2'), (175, 2, 1981, 1983, 'base_2'), (176, 2, 1983, 1985, 'base_2'), (177, 2, 1985, 1987, 'base_2'), (178, 2, 1987, 1989, 'base_2'), (179, 2, 1989, 1991, 'base_2'), (180, 2, 1991, 1993, 'base_2'), (181, 2, 1993, 1995, 'base_2'), (182, 2, 1995, 1997, 'base_2'), (183, 2, 1997, 1999, 'base_2'), (184, 2, 1999, 2001, 'base_2'), (185, 2, 2001, 2003, 'base_2'), (186, 2, 2003, 2005, 'base_2'), (187, 2, 2005, 2007, 'base_2'), (188, 2, 2007, 2009, 'base_2'), (189, 2, 2009, 2011, 'base_2'), (190, 2, 2011, 2013, 'base_2'), (191, 2, 2013, 2015, 'base_2'), (192, 2, 2015, 2017, 'base_2'), (193, 2, 2017, NULL, 'base_2'), (194, 4, NULL, 1891, 'base_4'), (195, 4, 1891, 1895, 'base_4'), (196, 4, 1895, 1899, 'base_4'), (197, 4, 1899, 1903, 'base_4'), (198, 4, 1903, 1907, 'base_4'), (199, 4, 1907, 1911, 'base_4'), (200, 4, 1911, 1915, 'base_4'), (201, 4, 1915, 1919, 'base_4'), (202, 4, 1919, 1923, 'base_4'), (203, 4, 1923, 1927, 'base_4'), (204, 4, 1927, 1932, 'base_4'), (205, 4, 1932, 1936, 'base_4'), (206, 4, 1936, 1940, 'base_4'), (207, 4, 1940, 1944, 'base_4'), (208, 4, 1944, 1948, 'base_4'), (209, 4, 1948, 1952, 'base_4'), (210, 4, 1952, 1956, 'base_4'), (211, 4, 1956, 1960, 'base_4'), (212, 4, 1960, 1964, 'base_4'), (213, 4, 1964, 1968, 'base_4'), (214, 4, 1968, 1972, 'base_4'), (215, 4, 1972, 1977, 'base_4'), (216, 4, 1977, 1981, 'base_4'), (217, 4, 1981, 1985, 'base_4'), (218, 4, 1985, 1989, 'base_4'), (219, 4, 1989, 1993, 'base_4'), (220, 4, 1993, 1997, 'base_4'), (221, 4, 1997, 2001, 'base_4'), (222, 4, 2001, 2005, 'base_4'), (223, 4, 2005, 2009, 'base_4'), (224, 4, 2009, 2013, 'base_4'), (225, 4, 2013, 2017, 'base_4'), (226, 4, 2017, NULL, 'base_4'), (227, 4, NULL, 1890, 'offset_4'), (228, 4, 1890, 1894, 'offset_4'), (229, 4, 1894, 1898, 'offset_4'), (230, 4, 1898, 1902, 'offset_4'), (231, 4, 1902, 1906, 'offset_4'), (232, 4, 1906, 1910, 'offset_4'), (233, 4, 1910, 1914, 'offset_4'), (234, 4, 1914, 1918, 'offset_4'), (235, 4, 1918, 1922, 'offset_4'), (236, 4, 1922, 1926, 'offset_4'), (237, 4, 1926, 1931, 'offset_4'), (238, 4, 1931, 1935, 'offset_4'), (239, 4, 1935, 1939, 'offset_4'), (240, 4, 1939, 1943, 'offset_4'), (241, 4, 1943, 1947, 'offset_4'), (242, 4, 1947, 1951, 'offset_4'), (243, 4, 1951, 1955, 'offset_4'), (244, 4, 1955, 1959, 'offset_4'), (245, 4, 1959, 1963, 'offset_4'), (246, 4, 1963, 1967, 'offset_4'), (247, 4, 1967, 1971, 'offset_4'), (248, 4, 1971, 1976, 'offset_4'), (249, 4, 1976, 1980, 'offset_4'), (250, 4, 1980, 1984, 'offset_4'), (251, 4, 1984, 1988, 'offset_4'), (252, 4, 1988, 1992, 'offset_4'), (253, 4, 1992, 1996, 'offset_4'), (254, 4, 1996, 2000, 'offset_4'), (255, 4, 2000, 2004, 'offset_4'), (256, 4, 2004, 2008, 'offset_4'), (257, 4, 2008, 2012, 'offset_4'), (258, 4, 2012, 2016, 'offset_4'), (259, 4, 2016, NULL, 'offset_4'), (260, 8, NULL, 1895, 'base_8'), (261, 8, 1895, 1903, 'base_8'), (262, 8, 1903, 1911, 'base_8'), (263, 8, 1911, 1919, 'base_8'), (264, 8, 1919, 1927, 'base_8'), (265, 8, 1927, 1936, 'base_8'), (266, 8, 1936, 1944, 'base_8'), (267, 8, 1944, 1952, 'base_8'), (268, 8, 1952, 1960, 'base_8'), (269, 8, 1960, 1968, 'base_8'), (270, 8, 1968, 1977, 'base_8'), (271, 8, 1977, 1985, 'base_8'), (272, 8, 1985, 1993, 'base_8'), (273, 8, 1993, 2001, 'base_8'), (274, 8, 2001, 2009, 'base_8'), (275, 8, 2009, 2017, 'base_8'), (276, 8, 2017, NULL, 'base_8'), (277, 8, NULL, 1892, 'offset_8'), (278, 8, 1892, 1900, 'offset_8'), (279, 8, 1900, 1908, 'offset_8'), (280, 8, 1908, 1916, 'offset_8'), (281, 8, 1916, 1924, 'offset_8'), (282, 8, 1924, 1933, 'offset_8'), (283, 8, 1933, 1941, 'offset_8'), (284, 8, 1941, 1949, 'offset_8'), (285, 8, 1949, 1957, 'offset_8'), (286, 8, 1957, 1965, 'offset_8'), (287, 8, 1965, 1973, 'offset_8'), (288, 8, 1973, 1982, 'offset_8'), (289, 8, 1982, 1990, 'offset_8'), (290, 8, 1990, 1998, 'offset_8'), (291, 8, 1998, 2006, 'offset_8'), (292, 8, 2006, 2014, 'offset_8'), (293, 8, 2014, NULL, 'offset_8'), (294, 16, NULL, 1903, 'base_16'), (295, 16, 1903, 1919, 'base_16'), (296, 16, 1919, 1936, 'base_16'), (297, 16, 1936, 1952, 'base_16'), (298, 16, 1952, 1968, 'base_16'), (299, 16, 1968, 1985, 'base_16'), (300, 16, 1985, 2001, 'base_16'), (301, 16, 2001, 2017, 'base_16'), (302, 16, 2017, NULL, 'base_16'), (303, 16, NULL, 1896, 'offset_16'), (304, 16, 1896, 1912, 'offset_16'), (305, 16, 1912, 1928, 'offset_16'), (306, 16, 1928, 1945, 'offset_16'), (307, 16, 1945, 1961, 'offset_16'), (308, 16, 1961, 1978, 'offset_16'), (309, 16, 1978, 1994, 'offset_16'), (310, 16, 1994, 2010, 'offset_16'), (311, 16, 2010, NULL, 'offset_16'), (312, 32, NULL, 1919, 'base_32'), (313, 32, 1919, 1952, 'base_32'), (314, 32, 1952, 1985, 'base_32'), (315, 32, 1985, 2017, 'base_32'), (316, 32, 2017, NULL, 'base_32'), (317, 32, NULL, 1904, 'offset_32'), (318, 32, 1904, 1937, 'offset_32'), (319, 32, 1937, 1969, 'offset_32'), (320, 32, 1969, 2002, 'offset_32'), (321, 32, 2002, NULL, 'offset_32'), (322, 64, NULL, 1952, 'base_64'), (323, 64, 1952, 2017, 'base_64'), (324, 64, 2017, NULL, 'base_64'), (325, 64, NULL, 1920, 'offset_64'), (326, 64, 1920, 1986, 'offset_64'), (327, 64, 1986, NULL, 'offset_64'), (328, 128, NULL, 2017, 'base_128'), (329, 128, 2017, NULL, 'base_128'), (330, 128, NULL, 1953, 'offset_128'), (331, 128, 1953, NULL, 'offset_128'), (332, 256, NULL, NULL, 'N/A')
;
    

DROP TABLE IF EXISTS MOVIE_INFO_INFO_TYPE_ID_mcv;

-- Create MCV table for MOVIE_INFO.INFO_TYPE_ID, with a unique mcv_id
CREATE TABLE MOVIE_INFO_INFO_TYPE_ID_mcv AS
WITH base AS (
    SELECT INFO_TYPE_ID, COUNT(*) AS freq
    FROM MOVIE_INFO
    WHERE INFO_TYPE_ID IS NOT NULL
    GROUP BY INFO_TYPE_ID
    ORDER BY freq DESC
    LIMIT 5000
)
SELECT
    ROW_NUMBER() OVER (ORDER BY freq DESC) AS mcv_id,
    INFO_TYPE_ID,
    freq
FROM base;

----------------------------------
-- END: generate all mcvs and histograms
----------------------------------


----------------------------------
-- START: generate all lpnorm queries for CAST_INFO
----------------------------------


DROP TABLE IF EXISTS CAST_INFO_MOVIE_ID_ID_TITLE;

-- Create temp table CAST_INFO_MOVIE_ID_ID_TITLE (FK->PK join)
CREATE TEMP TABLE CAST_INFO_MOVIE_ID_ID_TITLE AS
SELECT
    CAST_INFO.MOVIE_ID AS MOVIE_ID,
    TITLE.PRODUCTION_YEAR AS TITLE_PRODUCTION_YEAR, TITLE.KIND_ID AS TITLE_KIND_ID
FROM CAST_INFO
JOIN TITLE
  ON CAST_INFO.MOVIE_ID = TITLE.ID
;

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

DROP TABLE IF EXISTS Degree_CAST_INFO_ROLE_ID_MOVIE_ID;

-- Create aggregator table Degree_CAST_INFO_ROLE_ID_MOVIE_ID for MCV CAST_INFO.ROLE_ID -- MCV
CREATE TEMP TABLE Degree_CAST_INFO_ROLE_ID_MOVIE_ID AS
SELECT
    mcvt.mcv_id AS mcv_id,
    CAST_INFO.MOVIE_ID AS MOVIE_ID,
    COUNT(*) AS deg
FROM CAST_INFO
JOIN CAST_INFO_ROLE_ID_mcv mcvt ON CAST_INFO.ROLE_ID = mcvt.ROLE_ID
WHERE CAST_INFO.MOVIE_ID IS NOT NULL
GROUP BY
    mcvt.mcv_id, CAST_INFO.MOVIE_ID
;

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    fk_join_var_name,
    pk_relation_name,
    pk_join_var_name,
    
  pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CAST_INFO' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'MCV' AS aggregator_name,
    'ROLE_ID' AS pred_col_name,
    NULL AS fk_join_var_name,
    NULL AS pk_relation_name,
    NULL AS pk_join_var_name,
    
    mcv_id AS pred_value,
    COUNT(*) AS l0,
    SUM(POWER(deg, 1)) AS l1,
    SUM(POWER(deg, 2)) AS l2,
    SUM(POWER(deg, 3)) AS l3,
    SUM(POWER(deg, 4)) AS l4,
    SUM(POWER(deg, 5)) AS l5,
    SUM(POWER(deg, 6)) AS l6,
    SUM(POWER(deg, 7)) AS l7,
    SUM(POWER(deg, 8)) AS l8,
    SUM(POWER(deg, 9)) AS l9,
    SUM(POWER(deg, 10)) AS l10,
    MAX(deg) AS l_inf
FROM Degree_CAST_INFO_ROLE_ID_MOVIE_ID
GROUP BY mcv_id
;

DROP TABLE IF EXISTS Degree_CAST_INFO_ROLE_ID_MOVIE_ID;

-- Create aggregator table Degree_CAST_INFO_ROLE_ID_MOVIE_ID for NONMCV CAST_INFO.ROLE_ID -- MCV
CREATE TEMP TABLE Degree_CAST_INFO_ROLE_ID_MOVIE_ID AS
SELECT
    CAST_INFO.ROLE_ID AS ROLE_ID,
    CAST_INFO.MOVIE_ID AS MOVIE_ID,
    COUNT(*) AS deg
FROM CAST_INFO
LEFT JOIN CAST_INFO_ROLE_ID_mcv mcvt ON CAST_INFO.ROLE_ID = mcvt.ROLE_ID
WHERE CAST_INFO.MOVIE_ID IS NOT NULL AND mcvt.mcv_id IS NULL 
GROUP BY
    CAST_INFO.ROLE_ID, CAST_INFO.MOVIE_ID
;

-- Insert into norms for Non-MCV CAST_INFO ROLE_ID MOVIE_ID NONMCV
-- Among all non-MCV values in Degree_CAST_INFO_ROLE_ID_MOVIE_ID, find max of each Lp
WITH perValue AS (
  SELECT
    ROLE_ID,
    COUNT(*) AS l0, SUM(POWER(deg, 1)) AS l1, SUM(POWER(deg, 2)) AS l2, SUM(POWER(deg, 3)) AS l3, SUM(POWER(deg, 4)) AS l4, SUM(POWER(deg, 5)) AS l5, SUM(POWER(deg, 6)) AS l6, SUM(POWER(deg, 7)) AS l7, SUM(POWER(deg, 8)) AS l8, SUM(POWER(deg, 9)) AS l9, SUM(POWER(deg, 10)) AS l10, MAX(deg) AS l_inf
  FROM Degree_CAST_INFO_ROLE_ID_MOVIE_ID
  GROUP BY ROLE_ID
  HAVING ROLE_ID IS NOT NULL
)
INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    fk_join_var_name,
    pk_relation_name,
    pk_join_var_name,
    
  pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CAST_INFO' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NONMCV' AS aggregator_name,
    'ROLE_ID' AS pred_col_name,
    NULL AS fk_join_var_name,
    NULL AS pk_relation_name,
    NULL AS pk_join_var_name,
    
  NULL              AS pred_value_id,
  MAX(l0) AS l0, MAX(l1) AS l1, MAX(l2) AS l2, MAX(l3) AS l3, MAX(l4) AS l4, MAX(l5) AS l5, MAX(l6) AS l6, MAX(l7) AS l7, MAX(l8) AS l8, MAX(l9) AS l9, MAX(l10) AS l10, MAX(l_inf) AS l_inf
FROM perValue
HAVING COUNT(*) > 0  -- If you need to ensure some rows are returned
;

DROP TABLE IF EXISTS Degree_CAST_INFO_MOVIE_ID_ID_TITLE_KIND_ID_MOVIE_ID;

-- Create aggregator table Degree_CAST_INFO_MOVIE_ID_ID_TITLE_KIND_ID_MOVIE_ID for MCV CAST_INFO_MOVIE_ID_ID_TITLE.TITLE_KIND_ID -- FKPK_MCV
CREATE TEMP TABLE Degree_CAST_INFO_MOVIE_ID_ID_TITLE_KIND_ID_MOVIE_ID AS
SELECT
    mcvt.mcv_id AS mcv_id,
    CAST_INFO_MOVIE_ID_ID_TITLE.MOVIE_ID AS MOVIE_ID,
    COUNT(*) AS deg
FROM CAST_INFO_MOVIE_ID_ID_TITLE
JOIN TITLE_KIND_ID_mcv mcvt ON CAST_INFO_MOVIE_ID_ID_TITLE.TITLE_KIND_ID = mcvt.KIND_ID
WHERE CAST_INFO_MOVIE_ID_ID_TITLE.MOVIE_ID IS NOT NULL
GROUP BY
    mcvt.mcv_id, CAST_INFO_MOVIE_ID_ID_TITLE.MOVIE_ID
;

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    fk_join_var_name,
    pk_relation_name,
    pk_join_var_name,
    
  pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CAST_INFO' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'MCV' AS aggregator_name,
    'KIND_ID' AS pred_col_name,
    'MOVIE_ID' AS fk_join_var_name,
    'TITLE' AS pk_relation_name,
    'ID' AS pk_join_var_name,
    
    mcv_id AS pred_value,
    COUNT(*) AS l0,
    SUM(POWER(deg, 1)) AS l1,
    SUM(POWER(deg, 2)) AS l2,
    SUM(POWER(deg, 3)) AS l3,
    SUM(POWER(deg, 4)) AS l4,
    SUM(POWER(deg, 5)) AS l5,
    SUM(POWER(deg, 6)) AS l6,
    SUM(POWER(deg, 7)) AS l7,
    SUM(POWER(deg, 8)) AS l8,
    SUM(POWER(deg, 9)) AS l9,
    SUM(POWER(deg, 10)) AS l10,
    MAX(deg) AS l_inf
FROM Degree_CAST_INFO_MOVIE_ID_ID_TITLE_KIND_ID_MOVIE_ID
GROUP BY mcv_id
;

DROP TABLE IF EXISTS Degree_CAST_INFO_MOVIE_ID_ID_TITLE_KIND_ID_MOVIE_ID;

-- Create aggregator table Degree_CAST_INFO_MOVIE_ID_ID_TITLE_KIND_ID_MOVIE_ID for NONMCV CAST_INFO_MOVIE_ID_ID_TITLE.TITLE_KIND_ID -- FKPK_MCV
CREATE TEMP TABLE Degree_CAST_INFO_MOVIE_ID_ID_TITLE_KIND_ID_MOVIE_ID AS
SELECT
    CAST_INFO_MOVIE_ID_ID_TITLE.TITLE_KIND_ID AS KIND_ID,
    CAST_INFO_MOVIE_ID_ID_TITLE.MOVIE_ID AS MOVIE_ID,
    COUNT(*) AS deg
FROM CAST_INFO_MOVIE_ID_ID_TITLE
LEFT JOIN TITLE_KIND_ID_mcv mcvt ON CAST_INFO_MOVIE_ID_ID_TITLE.TITLE_KIND_ID = mcvt.KIND_ID
WHERE CAST_INFO_MOVIE_ID_ID_TITLE.MOVIE_ID IS NOT NULL AND mcvt.mcv_id IS NULL 
GROUP BY
    CAST_INFO_MOVIE_ID_ID_TITLE.TITLE_KIND_ID, CAST_INFO_MOVIE_ID_ID_TITLE.MOVIE_ID
;

-- Insert into norms for Non-MCV CAST_INFO_MOVIE_ID_ID_TITLE KIND_ID MOVIE_ID NONMCV
-- Among all non-MCV values in Degree_CAST_INFO_MOVIE_ID_ID_TITLE_KIND_ID_MOVIE_ID, find max of each Lp
WITH perValue AS (
  SELECT
    KIND_ID,
    COUNT(*) AS l0, SUM(POWER(deg, 1)) AS l1, SUM(POWER(deg, 2)) AS l2, SUM(POWER(deg, 3)) AS l3, SUM(POWER(deg, 4)) AS l4, SUM(POWER(deg, 5)) AS l5, SUM(POWER(deg, 6)) AS l6, SUM(POWER(deg, 7)) AS l7, SUM(POWER(deg, 8)) AS l8, SUM(POWER(deg, 9)) AS l9, SUM(POWER(deg, 10)) AS l10, MAX(deg) AS l_inf
  FROM Degree_CAST_INFO_MOVIE_ID_ID_TITLE_KIND_ID_MOVIE_ID
  GROUP BY KIND_ID
  HAVING KIND_ID IS NOT NULL
)
INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    fk_join_var_name,
    pk_relation_name,
    pk_join_var_name,
    
  pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CAST_INFO' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NONMCV' AS aggregator_name,
    'KIND_ID' AS pred_col_name,
    'MOVIE_ID' AS fk_join_var_name,
    'TITLE' AS pk_relation_name,
    'ID' AS pk_join_var_name,
    
  NULL              AS pred_value_id,
  MAX(l0) AS l0, MAX(l1) AS l1, MAX(l2) AS l2, MAX(l3) AS l3, MAX(l4) AS l4, MAX(l5) AS l5, MAX(l6) AS l6, MAX(l7) AS l7, MAX(l8) AS l8, MAX(l9) AS l9, MAX(l10) AS l10, MAX(l_inf) AS l_inf
FROM perValue
HAVING COUNT(*) > 0  -- If you need to ensure some rows are returned
;

DROP TABLE IF EXISTS Degree_CAST_INFO_MOVIE_ID_ID_TITLE_PRODUCTION_YEAR_MOVIE_ID;

-- Create aggregator table Degree_CAST_INFO_MOVIE_ID_ID_TITLE_PRODUCTION_YEAR_MOVIE_ID for MCV CAST_INFO_MOVIE_ID_ID_TITLE.TITLE_PRODUCTION_YEAR -- FKPK_MCV
CREATE TEMP TABLE Degree_CAST_INFO_MOVIE_ID_ID_TITLE_PRODUCTION_YEAR_MOVIE_ID AS
SELECT
    mcvt.mcv_id AS mcv_id,
    CAST_INFO_MOVIE_ID_ID_TITLE.MOVIE_ID AS MOVIE_ID,
    COUNT(*) AS deg
FROM CAST_INFO_MOVIE_ID_ID_TITLE
JOIN TITLE_PRODUCTION_YEAR_mcv mcvt ON CAST_INFO_MOVIE_ID_ID_TITLE.TITLE_PRODUCTION_YEAR = mcvt.PRODUCTION_YEAR
WHERE CAST_INFO_MOVIE_ID_ID_TITLE.MOVIE_ID IS NOT NULL
GROUP BY
    mcvt.mcv_id, CAST_INFO_MOVIE_ID_ID_TITLE.MOVIE_ID
;

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    fk_join_var_name,
    pk_relation_name,
    pk_join_var_name,
    
  pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CAST_INFO' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'MCV' AS aggregator_name,
    'PRODUCTION_YEAR' AS pred_col_name,
    'MOVIE_ID' AS fk_join_var_name,
    'TITLE' AS pk_relation_name,
    'ID' AS pk_join_var_name,
    
    mcv_id AS pred_value,
    COUNT(*) AS l0,
    SUM(POWER(deg, 1)) AS l1,
    SUM(POWER(deg, 2)) AS l2,
    SUM(POWER(deg, 3)) AS l3,
    SUM(POWER(deg, 4)) AS l4,
    SUM(POWER(deg, 5)) AS l5,
    SUM(POWER(deg, 6)) AS l6,
    SUM(POWER(deg, 7)) AS l7,
    SUM(POWER(deg, 8)) AS l8,
    SUM(POWER(deg, 9)) AS l9,
    SUM(POWER(deg, 10)) AS l10,
    MAX(deg) AS l_inf
FROM Degree_CAST_INFO_MOVIE_ID_ID_TITLE_PRODUCTION_YEAR_MOVIE_ID
GROUP BY mcv_id
;

DROP TABLE IF EXISTS Degree_CAST_INFO_MOVIE_ID_ID_TITLE_PRODUCTION_YEAR_MOVIE_ID;

-- Create aggregator table Degree_CAST_INFO_MOVIE_ID_ID_TITLE_PRODUCTION_YEAR_MOVIE_ID for NONMCV CAST_INFO_MOVIE_ID_ID_TITLE.TITLE_PRODUCTION_YEAR -- FKPK_MCV
CREATE TEMP TABLE Degree_CAST_INFO_MOVIE_ID_ID_TITLE_PRODUCTION_YEAR_MOVIE_ID AS
SELECT
    CAST_INFO_MOVIE_ID_ID_TITLE.TITLE_PRODUCTION_YEAR AS PRODUCTION_YEAR,
    CAST_INFO_MOVIE_ID_ID_TITLE.MOVIE_ID AS MOVIE_ID,
    COUNT(*) AS deg
FROM CAST_INFO_MOVIE_ID_ID_TITLE
LEFT JOIN TITLE_PRODUCTION_YEAR_mcv mcvt ON CAST_INFO_MOVIE_ID_ID_TITLE.TITLE_PRODUCTION_YEAR = mcvt.PRODUCTION_YEAR
WHERE CAST_INFO_MOVIE_ID_ID_TITLE.MOVIE_ID IS NOT NULL AND mcvt.mcv_id IS NULL 
GROUP BY
    CAST_INFO_MOVIE_ID_ID_TITLE.TITLE_PRODUCTION_YEAR, CAST_INFO_MOVIE_ID_ID_TITLE.MOVIE_ID
;

-- Insert into norms for Non-MCV CAST_INFO_MOVIE_ID_ID_TITLE PRODUCTION_YEAR MOVIE_ID NONMCV
-- Among all non-MCV values in Degree_CAST_INFO_MOVIE_ID_ID_TITLE_PRODUCTION_YEAR_MOVIE_ID, find max of each Lp
WITH perValue AS (
  SELECT
    PRODUCTION_YEAR,
    COUNT(*) AS l0, SUM(POWER(deg, 1)) AS l1, SUM(POWER(deg, 2)) AS l2, SUM(POWER(deg, 3)) AS l3, SUM(POWER(deg, 4)) AS l4, SUM(POWER(deg, 5)) AS l5, SUM(POWER(deg, 6)) AS l6, SUM(POWER(deg, 7)) AS l7, SUM(POWER(deg, 8)) AS l8, SUM(POWER(deg, 9)) AS l9, SUM(POWER(deg, 10)) AS l10, MAX(deg) AS l_inf
  FROM Degree_CAST_INFO_MOVIE_ID_ID_TITLE_PRODUCTION_YEAR_MOVIE_ID
  GROUP BY PRODUCTION_YEAR
  HAVING PRODUCTION_YEAR IS NOT NULL
)
INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    fk_join_var_name,
    pk_relation_name,
    pk_join_var_name,
    
  pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CAST_INFO' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NONMCV' AS aggregator_name,
    'PRODUCTION_YEAR' AS pred_col_name,
    'MOVIE_ID' AS fk_join_var_name,
    'TITLE' AS pk_relation_name,
    'ID' AS pk_join_var_name,
    
  NULL              AS pred_value_id,
  MAX(l0) AS l0, MAX(l1) AS l1, MAX(l2) AS l2, MAX(l3) AS l3, MAX(l4) AS l4, MAX(l5) AS l5, MAX(l6) AS l6, MAX(l7) AS l7, MAX(l8) AS l8, MAX(l9) AS l9, MAX(l10) AS l10, MAX(l_inf) AS l_inf
FROM perValue
HAVING COUNT(*) > 0  -- If you need to ensure some rows are returned
;

DROP TABLE IF EXISTS Degree_CAST_INFO_MOVIE_ID_ID_TITLE_PRODUCTION_YEAR_MOVIE_ID;

-- Create aggregator table Degree_CAST_INFO_MOVIE_ID_ID_TITLE_PRODUCTION_YEAR_MOVIE_ID for Histogram CAST_INFO_MOVIE_ID_ID_TITLE.TITLE_PRODUCTION_YEAR -- FKPK_RANGE
CREATE TEMP TABLE Degree_CAST_INFO_MOVIE_ID_ID_TITLE_PRODUCTION_YEAR_MOVIE_ID AS
SELECT
    CAST_INFO_MOVIE_ID_ID_TITLE.TITLE_PRODUCTION_YEAR AS PRODUCTION_YEAR,
    CAST_INFO_MOVIE_ID_ID_TITLE.MOVIE_ID AS MOVIE_ID,
    COUNT(*) AS deg
FROM CAST_INFO_MOVIE_ID_ID_TITLE

WHERE CAST_INFO_MOVIE_ID_ID_TITLE.MOVIE_ID IS NOT NULL
GROUP BY
    CAST_INFO_MOVIE_ID_ID_TITLE.TITLE_PRODUCTION_YEAR, CAST_INFO_MOVIE_ID_ID_TITLE.MOVIE_ID
;

INSERT INTO norms(

    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    fk_join_var_name,
    pk_relation_name,
    pk_join_var_name,
    
pred_value_id,
l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'CAST_INFO' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'RANGE' AS aggregator_name,
    'PRODUCTION_YEAR' AS pred_col_name,
    'MOVIE_ID' AS fk_join_var_name,
    'TITLE' AS pk_relation_name,
    'ID' AS pk_join_var_name,
    
    bucket_id     AS pred_value,
    COUNT(*) AS l0,
    SUM(POWER(deg, 1)) AS l1,
    SUM(POWER(deg, 2)) AS l2,
    SUM(POWER(deg, 3)) AS l3,
    SUM(POWER(deg, 4)) AS l4,
    SUM(POWER(deg, 5)) AS l5,
    SUM(POWER(deg, 6)) AS l6,
    SUM(POWER(deg, 7)) AS l7,
    SUM(POWER(deg, 8)) AS l8,
    SUM(POWER(deg, 9)) AS l9,
    SUM(POWER(deg, 10)) AS l10,
    MAX(deg) AS l_inf
FROM 
(
  SELECT
        MOVIE_ID, bucket_id, SUM(deg) as deg
        FROM Degree_CAST_INFO_MOVIE_ID_ID_TITLE_PRODUCTION_YEAR_MOVIE_ID d
        JOIN TITLE_PRODUCTION_YEAR_histogram h
        ON (h.lower_bound IS NULL OR d.PRODUCTION_YEAR >= h.lower_bound)
        AND (h.upper_bound IS NULL OR d.PRODUCTION_YEAR <= h.upper_bound)
    WHERE MOVIE_ID IS NOT NULL AND bucket_id IS NOT NULL
    GROUP BY MOVIE_ID, bucket_id
)
GROUP BY bucket_id
ORDER BY bucket_id
;

----------------------------------
-- START: generate all lpnorm queries for MOVIE_COMPANIES
----------------------------------


DROP TABLE IF EXISTS MOVIE_COMPANIES_MOVIE_ID_ID_TITLE;

-- Create temp table MOVIE_COMPANIES_MOVIE_ID_ID_TITLE (FK->PK join)
CREATE TEMP TABLE MOVIE_COMPANIES_MOVIE_ID_ID_TITLE AS
SELECT
    MOVIE_COMPANIES.MOVIE_ID AS MOVIE_ID,
    TITLE.PRODUCTION_YEAR AS TITLE_PRODUCTION_YEAR, TITLE.KIND_ID AS TITLE_KIND_ID
FROM MOVIE_COMPANIES
JOIN TITLE
  ON MOVIE_COMPANIES.MOVIE_ID = TITLE.ID
;

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

DROP TABLE IF EXISTS Degree_MOVIE_COMPANIES_COMPANY_ID_MOVIE_ID;

-- Create aggregator table Degree_MOVIE_COMPANIES_COMPANY_ID_MOVIE_ID for MCV MOVIE_COMPANIES.COMPANY_ID -- MCV
CREATE TEMP TABLE Degree_MOVIE_COMPANIES_COMPANY_ID_MOVIE_ID AS
SELECT
    mcvt.mcv_id AS mcv_id,
    MOVIE_COMPANIES.MOVIE_ID AS MOVIE_ID,
    COUNT(*) AS deg
FROM MOVIE_COMPANIES
JOIN MOVIE_COMPANIES_COMPANY_ID_mcv mcvt ON MOVIE_COMPANIES.COMPANY_ID = mcvt.COMPANY_ID
WHERE MOVIE_COMPANIES.MOVIE_ID IS NOT NULL
GROUP BY
    mcvt.mcv_id, MOVIE_COMPANIES.MOVIE_ID
;

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    fk_join_var_name,
    pk_relation_name,
    pk_join_var_name,
    
  pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_COMPANIES' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'MCV' AS aggregator_name,
    'COMPANY_ID' AS pred_col_name,
    NULL AS fk_join_var_name,
    NULL AS pk_relation_name,
    NULL AS pk_join_var_name,
    
    mcv_id AS pred_value,
    COUNT(*) AS l0,
    SUM(POWER(deg, 1)) AS l1,
    SUM(POWER(deg, 2)) AS l2,
    SUM(POWER(deg, 3)) AS l3,
    SUM(POWER(deg, 4)) AS l4,
    SUM(POWER(deg, 5)) AS l5,
    SUM(POWER(deg, 6)) AS l6,
    SUM(POWER(deg, 7)) AS l7,
    SUM(POWER(deg, 8)) AS l8,
    SUM(POWER(deg, 9)) AS l9,
    SUM(POWER(deg, 10)) AS l10,
    MAX(deg) AS l_inf
FROM Degree_MOVIE_COMPANIES_COMPANY_ID_MOVIE_ID
GROUP BY mcv_id
;

DROP TABLE IF EXISTS Degree_MOVIE_COMPANIES_COMPANY_ID_MOVIE_ID;

-- Create aggregator table Degree_MOVIE_COMPANIES_COMPANY_ID_MOVIE_ID for NONMCV MOVIE_COMPANIES.COMPANY_ID -- MCV
CREATE TEMP TABLE Degree_MOVIE_COMPANIES_COMPANY_ID_MOVIE_ID AS
SELECT
    MOVIE_COMPANIES.COMPANY_ID AS COMPANY_ID,
    MOVIE_COMPANIES.MOVIE_ID AS MOVIE_ID,
    COUNT(*) AS deg
FROM MOVIE_COMPANIES
LEFT JOIN MOVIE_COMPANIES_COMPANY_ID_mcv mcvt ON MOVIE_COMPANIES.COMPANY_ID = mcvt.COMPANY_ID
WHERE MOVIE_COMPANIES.MOVIE_ID IS NOT NULL AND mcvt.mcv_id IS NULL 
GROUP BY
    MOVIE_COMPANIES.COMPANY_ID, MOVIE_COMPANIES.MOVIE_ID
;

-- Insert into norms for Non-MCV MOVIE_COMPANIES COMPANY_ID MOVIE_ID NONMCV
-- Among all non-MCV values in Degree_MOVIE_COMPANIES_COMPANY_ID_MOVIE_ID, find max of each Lp
WITH perValue AS (
  SELECT
    COMPANY_ID,
    COUNT(*) AS l0, SUM(POWER(deg, 1)) AS l1, SUM(POWER(deg, 2)) AS l2, SUM(POWER(deg, 3)) AS l3, SUM(POWER(deg, 4)) AS l4, SUM(POWER(deg, 5)) AS l5, SUM(POWER(deg, 6)) AS l6, SUM(POWER(deg, 7)) AS l7, SUM(POWER(deg, 8)) AS l8, SUM(POWER(deg, 9)) AS l9, SUM(POWER(deg, 10)) AS l10, MAX(deg) AS l_inf
  FROM Degree_MOVIE_COMPANIES_COMPANY_ID_MOVIE_ID
  GROUP BY COMPANY_ID
  HAVING COMPANY_ID IS NOT NULL
)
INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    fk_join_var_name,
    pk_relation_name,
    pk_join_var_name,
    
  pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_COMPANIES' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NONMCV' AS aggregator_name,
    'COMPANY_ID' AS pred_col_name,
    NULL AS fk_join_var_name,
    NULL AS pk_relation_name,
    NULL AS pk_join_var_name,
    
  NULL              AS pred_value_id,
  MAX(l0) AS l0, MAX(l1) AS l1, MAX(l2) AS l2, MAX(l3) AS l3, MAX(l4) AS l4, MAX(l5) AS l5, MAX(l6) AS l6, MAX(l7) AS l7, MAX(l8) AS l8, MAX(l9) AS l9, MAX(l10) AS l10, MAX(l_inf) AS l_inf
FROM perValue
HAVING COUNT(*) > 0  -- If you need to ensure some rows are returned
;

DROP TABLE IF EXISTS Degree_MOVIE_COMPANIES_COMPANY_TYPE_ID_MOVIE_ID;

-- Create aggregator table Degree_MOVIE_COMPANIES_COMPANY_TYPE_ID_MOVIE_ID for MCV MOVIE_COMPANIES.COMPANY_TYPE_ID -- MCV
CREATE TEMP TABLE Degree_MOVIE_COMPANIES_COMPANY_TYPE_ID_MOVIE_ID AS
SELECT
    mcvt.mcv_id AS mcv_id,
    MOVIE_COMPANIES.MOVIE_ID AS MOVIE_ID,
    COUNT(*) AS deg
FROM MOVIE_COMPANIES
JOIN MOVIE_COMPANIES_COMPANY_TYPE_ID_mcv mcvt ON MOVIE_COMPANIES.COMPANY_TYPE_ID = mcvt.COMPANY_TYPE_ID
WHERE MOVIE_COMPANIES.MOVIE_ID IS NOT NULL
GROUP BY
    mcvt.mcv_id, MOVIE_COMPANIES.MOVIE_ID
;

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    fk_join_var_name,
    pk_relation_name,
    pk_join_var_name,
    
  pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_COMPANIES' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'MCV' AS aggregator_name,
    'COMPANY_TYPE_ID' AS pred_col_name,
    NULL AS fk_join_var_name,
    NULL AS pk_relation_name,
    NULL AS pk_join_var_name,
    
    mcv_id AS pred_value,
    COUNT(*) AS l0,
    SUM(POWER(deg, 1)) AS l1,
    SUM(POWER(deg, 2)) AS l2,
    SUM(POWER(deg, 3)) AS l3,
    SUM(POWER(deg, 4)) AS l4,
    SUM(POWER(deg, 5)) AS l5,
    SUM(POWER(deg, 6)) AS l6,
    SUM(POWER(deg, 7)) AS l7,
    SUM(POWER(deg, 8)) AS l8,
    SUM(POWER(deg, 9)) AS l9,
    SUM(POWER(deg, 10)) AS l10,
    MAX(deg) AS l_inf
FROM Degree_MOVIE_COMPANIES_COMPANY_TYPE_ID_MOVIE_ID
GROUP BY mcv_id
;

DROP TABLE IF EXISTS Degree_MOVIE_COMPANIES_COMPANY_TYPE_ID_MOVIE_ID;

-- Create aggregator table Degree_MOVIE_COMPANIES_COMPANY_TYPE_ID_MOVIE_ID for NONMCV MOVIE_COMPANIES.COMPANY_TYPE_ID -- MCV
CREATE TEMP TABLE Degree_MOVIE_COMPANIES_COMPANY_TYPE_ID_MOVIE_ID AS
SELECT
    MOVIE_COMPANIES.COMPANY_TYPE_ID AS COMPANY_TYPE_ID,
    MOVIE_COMPANIES.MOVIE_ID AS MOVIE_ID,
    COUNT(*) AS deg
FROM MOVIE_COMPANIES
LEFT JOIN MOVIE_COMPANIES_COMPANY_TYPE_ID_mcv mcvt ON MOVIE_COMPANIES.COMPANY_TYPE_ID = mcvt.COMPANY_TYPE_ID
WHERE MOVIE_COMPANIES.MOVIE_ID IS NOT NULL AND mcvt.mcv_id IS NULL 
GROUP BY
    MOVIE_COMPANIES.COMPANY_TYPE_ID, MOVIE_COMPANIES.MOVIE_ID
;

-- Insert into norms for Non-MCV MOVIE_COMPANIES COMPANY_TYPE_ID MOVIE_ID NONMCV
-- Among all non-MCV values in Degree_MOVIE_COMPANIES_COMPANY_TYPE_ID_MOVIE_ID, find max of each Lp
WITH perValue AS (
  SELECT
    COMPANY_TYPE_ID,
    COUNT(*) AS l0, SUM(POWER(deg, 1)) AS l1, SUM(POWER(deg, 2)) AS l2, SUM(POWER(deg, 3)) AS l3, SUM(POWER(deg, 4)) AS l4, SUM(POWER(deg, 5)) AS l5, SUM(POWER(deg, 6)) AS l6, SUM(POWER(deg, 7)) AS l7, SUM(POWER(deg, 8)) AS l8, SUM(POWER(deg, 9)) AS l9, SUM(POWER(deg, 10)) AS l10, MAX(deg) AS l_inf
  FROM Degree_MOVIE_COMPANIES_COMPANY_TYPE_ID_MOVIE_ID
  GROUP BY COMPANY_TYPE_ID
  HAVING COMPANY_TYPE_ID IS NOT NULL
)
INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    fk_join_var_name,
    pk_relation_name,
    pk_join_var_name,
    
  pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_COMPANIES' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NONMCV' AS aggregator_name,
    'COMPANY_TYPE_ID' AS pred_col_name,
    NULL AS fk_join_var_name,
    NULL AS pk_relation_name,
    NULL AS pk_join_var_name,
    
  NULL              AS pred_value_id,
  MAX(l0) AS l0, MAX(l1) AS l1, MAX(l2) AS l2, MAX(l3) AS l3, MAX(l4) AS l4, MAX(l5) AS l5, MAX(l6) AS l6, MAX(l7) AS l7, MAX(l8) AS l8, MAX(l9) AS l9, MAX(l10) AS l10, MAX(l_inf) AS l_inf
FROM perValue
HAVING COUNT(*) > 0  -- If you need to ensure some rows are returned
;

DROP TABLE IF EXISTS Degree_MOVIE_COMPANIES_MOVIE_ID_ID_TITLE_KIND_ID_MOVIE_ID;

-- Create aggregator table Degree_MOVIE_COMPANIES_MOVIE_ID_ID_TITLE_KIND_ID_MOVIE_ID for MCV MOVIE_COMPANIES_MOVIE_ID_ID_TITLE.TITLE_KIND_ID -- FKPK_MCV
CREATE TEMP TABLE Degree_MOVIE_COMPANIES_MOVIE_ID_ID_TITLE_KIND_ID_MOVIE_ID AS
SELECT
    mcvt.mcv_id AS mcv_id,
    MOVIE_COMPANIES_MOVIE_ID_ID_TITLE.MOVIE_ID AS MOVIE_ID,
    COUNT(*) AS deg
FROM MOVIE_COMPANIES_MOVIE_ID_ID_TITLE
JOIN TITLE_KIND_ID_mcv mcvt ON MOVIE_COMPANIES_MOVIE_ID_ID_TITLE.TITLE_KIND_ID = mcvt.KIND_ID
WHERE MOVIE_COMPANIES_MOVIE_ID_ID_TITLE.MOVIE_ID IS NOT NULL
GROUP BY
    mcvt.mcv_id, MOVIE_COMPANIES_MOVIE_ID_ID_TITLE.MOVIE_ID
;

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    fk_join_var_name,
    pk_relation_name,
    pk_join_var_name,
    
  pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_COMPANIES' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'MCV' AS aggregator_name,
    'KIND_ID' AS pred_col_name,
    'MOVIE_ID' AS fk_join_var_name,
    'TITLE' AS pk_relation_name,
    'ID' AS pk_join_var_name,
    
    mcv_id AS pred_value,
    COUNT(*) AS l0,
    SUM(POWER(deg, 1)) AS l1,
    SUM(POWER(deg, 2)) AS l2,
    SUM(POWER(deg, 3)) AS l3,
    SUM(POWER(deg, 4)) AS l4,
    SUM(POWER(deg, 5)) AS l5,
    SUM(POWER(deg, 6)) AS l6,
    SUM(POWER(deg, 7)) AS l7,
    SUM(POWER(deg, 8)) AS l8,
    SUM(POWER(deg, 9)) AS l9,
    SUM(POWER(deg, 10)) AS l10,
    MAX(deg) AS l_inf
FROM Degree_MOVIE_COMPANIES_MOVIE_ID_ID_TITLE_KIND_ID_MOVIE_ID
GROUP BY mcv_id
;

DROP TABLE IF EXISTS Degree_MOVIE_COMPANIES_MOVIE_ID_ID_TITLE_KIND_ID_MOVIE_ID;

-- Create aggregator table Degree_MOVIE_COMPANIES_MOVIE_ID_ID_TITLE_KIND_ID_MOVIE_ID for NONMCV MOVIE_COMPANIES_MOVIE_ID_ID_TITLE.TITLE_KIND_ID -- FKPK_MCV
CREATE TEMP TABLE Degree_MOVIE_COMPANIES_MOVIE_ID_ID_TITLE_KIND_ID_MOVIE_ID AS
SELECT
    MOVIE_COMPANIES_MOVIE_ID_ID_TITLE.TITLE_KIND_ID AS KIND_ID,
    MOVIE_COMPANIES_MOVIE_ID_ID_TITLE.MOVIE_ID AS MOVIE_ID,
    COUNT(*) AS deg
FROM MOVIE_COMPANIES_MOVIE_ID_ID_TITLE
LEFT JOIN TITLE_KIND_ID_mcv mcvt ON MOVIE_COMPANIES_MOVIE_ID_ID_TITLE.TITLE_KIND_ID = mcvt.KIND_ID
WHERE MOVIE_COMPANIES_MOVIE_ID_ID_TITLE.MOVIE_ID IS NOT NULL AND mcvt.mcv_id IS NULL 
GROUP BY
    MOVIE_COMPANIES_MOVIE_ID_ID_TITLE.TITLE_KIND_ID, MOVIE_COMPANIES_MOVIE_ID_ID_TITLE.MOVIE_ID
;

-- Insert into norms for Non-MCV MOVIE_COMPANIES_MOVIE_ID_ID_TITLE KIND_ID MOVIE_ID NONMCV
-- Among all non-MCV values in Degree_MOVIE_COMPANIES_MOVIE_ID_ID_TITLE_KIND_ID_MOVIE_ID, find max of each Lp
WITH perValue AS (
  SELECT
    KIND_ID,
    COUNT(*) AS l0, SUM(POWER(deg, 1)) AS l1, SUM(POWER(deg, 2)) AS l2, SUM(POWER(deg, 3)) AS l3, SUM(POWER(deg, 4)) AS l4, SUM(POWER(deg, 5)) AS l5, SUM(POWER(deg, 6)) AS l6, SUM(POWER(deg, 7)) AS l7, SUM(POWER(deg, 8)) AS l8, SUM(POWER(deg, 9)) AS l9, SUM(POWER(deg, 10)) AS l10, MAX(deg) AS l_inf
  FROM Degree_MOVIE_COMPANIES_MOVIE_ID_ID_TITLE_KIND_ID_MOVIE_ID
  GROUP BY KIND_ID
  HAVING KIND_ID IS NOT NULL
)
INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    fk_join_var_name,
    pk_relation_name,
    pk_join_var_name,
    
  pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_COMPANIES' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NONMCV' AS aggregator_name,
    'KIND_ID' AS pred_col_name,
    'MOVIE_ID' AS fk_join_var_name,
    'TITLE' AS pk_relation_name,
    'ID' AS pk_join_var_name,
    
  NULL              AS pred_value_id,
  MAX(l0) AS l0, MAX(l1) AS l1, MAX(l2) AS l2, MAX(l3) AS l3, MAX(l4) AS l4, MAX(l5) AS l5, MAX(l6) AS l6, MAX(l7) AS l7, MAX(l8) AS l8, MAX(l9) AS l9, MAX(l10) AS l10, MAX(l_inf) AS l_inf
FROM perValue
HAVING COUNT(*) > 0  -- If you need to ensure some rows are returned
;

DROP TABLE IF EXISTS Degree_MOVIE_COMPANIES_MOVIE_ID_ID_TITLE_PRODUCTION_YEAR_MOVIE_ID;

-- Create aggregator table Degree_MOVIE_COMPANIES_MOVIE_ID_ID_TITLE_PRODUCTION_YEAR_MOVIE_ID for MCV MOVIE_COMPANIES_MOVIE_ID_ID_TITLE.TITLE_PRODUCTION_YEAR -- FKPK_MCV
CREATE TEMP TABLE Degree_MOVIE_COMPANIES_MOVIE_ID_ID_TITLE_PRODUCTION_YEAR_MOVIE_ID AS
SELECT
    mcvt.mcv_id AS mcv_id,
    MOVIE_COMPANIES_MOVIE_ID_ID_TITLE.MOVIE_ID AS MOVIE_ID,
    COUNT(*) AS deg
FROM MOVIE_COMPANIES_MOVIE_ID_ID_TITLE
JOIN TITLE_PRODUCTION_YEAR_mcv mcvt ON MOVIE_COMPANIES_MOVIE_ID_ID_TITLE.TITLE_PRODUCTION_YEAR = mcvt.PRODUCTION_YEAR
WHERE MOVIE_COMPANIES_MOVIE_ID_ID_TITLE.MOVIE_ID IS NOT NULL
GROUP BY
    mcvt.mcv_id, MOVIE_COMPANIES_MOVIE_ID_ID_TITLE.MOVIE_ID
;

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    fk_join_var_name,
    pk_relation_name,
    pk_join_var_name,
    
  pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_COMPANIES' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'MCV' AS aggregator_name,
    'PRODUCTION_YEAR' AS pred_col_name,
    'MOVIE_ID' AS fk_join_var_name,
    'TITLE' AS pk_relation_name,
    'ID' AS pk_join_var_name,
    
    mcv_id AS pred_value,
    COUNT(*) AS l0,
    SUM(POWER(deg, 1)) AS l1,
    SUM(POWER(deg, 2)) AS l2,
    SUM(POWER(deg, 3)) AS l3,
    SUM(POWER(deg, 4)) AS l4,
    SUM(POWER(deg, 5)) AS l5,
    SUM(POWER(deg, 6)) AS l6,
    SUM(POWER(deg, 7)) AS l7,
    SUM(POWER(deg, 8)) AS l8,
    SUM(POWER(deg, 9)) AS l9,
    SUM(POWER(deg, 10)) AS l10,
    MAX(deg) AS l_inf
FROM Degree_MOVIE_COMPANIES_MOVIE_ID_ID_TITLE_PRODUCTION_YEAR_MOVIE_ID
GROUP BY mcv_id
;

DROP TABLE IF EXISTS Degree_MOVIE_COMPANIES_MOVIE_ID_ID_TITLE_PRODUCTION_YEAR_MOVIE_ID;

-- Create aggregator table Degree_MOVIE_COMPANIES_MOVIE_ID_ID_TITLE_PRODUCTION_YEAR_MOVIE_ID for NONMCV MOVIE_COMPANIES_MOVIE_ID_ID_TITLE.TITLE_PRODUCTION_YEAR -- FKPK_MCV
CREATE TEMP TABLE Degree_MOVIE_COMPANIES_MOVIE_ID_ID_TITLE_PRODUCTION_YEAR_MOVIE_ID AS
SELECT
    MOVIE_COMPANIES_MOVIE_ID_ID_TITLE.TITLE_PRODUCTION_YEAR AS PRODUCTION_YEAR,
    MOVIE_COMPANIES_MOVIE_ID_ID_TITLE.MOVIE_ID AS MOVIE_ID,
    COUNT(*) AS deg
FROM MOVIE_COMPANIES_MOVIE_ID_ID_TITLE
LEFT JOIN TITLE_PRODUCTION_YEAR_mcv mcvt ON MOVIE_COMPANIES_MOVIE_ID_ID_TITLE.TITLE_PRODUCTION_YEAR = mcvt.PRODUCTION_YEAR
WHERE MOVIE_COMPANIES_MOVIE_ID_ID_TITLE.MOVIE_ID IS NOT NULL AND mcvt.mcv_id IS NULL 
GROUP BY
    MOVIE_COMPANIES_MOVIE_ID_ID_TITLE.TITLE_PRODUCTION_YEAR, MOVIE_COMPANIES_MOVIE_ID_ID_TITLE.MOVIE_ID
;

-- Insert into norms for Non-MCV MOVIE_COMPANIES_MOVIE_ID_ID_TITLE PRODUCTION_YEAR MOVIE_ID NONMCV
-- Among all non-MCV values in Degree_MOVIE_COMPANIES_MOVIE_ID_ID_TITLE_PRODUCTION_YEAR_MOVIE_ID, find max of each Lp
WITH perValue AS (
  SELECT
    PRODUCTION_YEAR,
    COUNT(*) AS l0, SUM(POWER(deg, 1)) AS l1, SUM(POWER(deg, 2)) AS l2, SUM(POWER(deg, 3)) AS l3, SUM(POWER(deg, 4)) AS l4, SUM(POWER(deg, 5)) AS l5, SUM(POWER(deg, 6)) AS l6, SUM(POWER(deg, 7)) AS l7, SUM(POWER(deg, 8)) AS l8, SUM(POWER(deg, 9)) AS l9, SUM(POWER(deg, 10)) AS l10, MAX(deg) AS l_inf
  FROM Degree_MOVIE_COMPANIES_MOVIE_ID_ID_TITLE_PRODUCTION_YEAR_MOVIE_ID
  GROUP BY PRODUCTION_YEAR
  HAVING PRODUCTION_YEAR IS NOT NULL
)
INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    fk_join_var_name,
    pk_relation_name,
    pk_join_var_name,
    
  pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_COMPANIES' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NONMCV' AS aggregator_name,
    'PRODUCTION_YEAR' AS pred_col_name,
    'MOVIE_ID' AS fk_join_var_name,
    'TITLE' AS pk_relation_name,
    'ID' AS pk_join_var_name,
    
  NULL              AS pred_value_id,
  MAX(l0) AS l0, MAX(l1) AS l1, MAX(l2) AS l2, MAX(l3) AS l3, MAX(l4) AS l4, MAX(l5) AS l5, MAX(l6) AS l6, MAX(l7) AS l7, MAX(l8) AS l8, MAX(l9) AS l9, MAX(l10) AS l10, MAX(l_inf) AS l_inf
FROM perValue
HAVING COUNT(*) > 0  -- If you need to ensure some rows are returned
;

DROP TABLE IF EXISTS Degree_MOVIE_COMPANIES_MOVIE_ID_ID_TITLE_PRODUCTION_YEAR_MOVIE_ID;

-- Create aggregator table Degree_MOVIE_COMPANIES_MOVIE_ID_ID_TITLE_PRODUCTION_YEAR_MOVIE_ID for Histogram MOVIE_COMPANIES_MOVIE_ID_ID_TITLE.TITLE_PRODUCTION_YEAR -- FKPK_RANGE
CREATE TEMP TABLE Degree_MOVIE_COMPANIES_MOVIE_ID_ID_TITLE_PRODUCTION_YEAR_MOVIE_ID AS
SELECT
    MOVIE_COMPANIES_MOVIE_ID_ID_TITLE.TITLE_PRODUCTION_YEAR AS PRODUCTION_YEAR,
    MOVIE_COMPANIES_MOVIE_ID_ID_TITLE.MOVIE_ID AS MOVIE_ID,
    COUNT(*) AS deg
FROM MOVIE_COMPANIES_MOVIE_ID_ID_TITLE

WHERE MOVIE_COMPANIES_MOVIE_ID_ID_TITLE.MOVIE_ID IS NOT NULL
GROUP BY
    MOVIE_COMPANIES_MOVIE_ID_ID_TITLE.TITLE_PRODUCTION_YEAR, MOVIE_COMPANIES_MOVIE_ID_ID_TITLE.MOVIE_ID
;

INSERT INTO norms(

    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    fk_join_var_name,
    pk_relation_name,
    pk_join_var_name,
    
pred_value_id,
l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_COMPANIES' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'RANGE' AS aggregator_name,
    'PRODUCTION_YEAR' AS pred_col_name,
    'MOVIE_ID' AS fk_join_var_name,
    'TITLE' AS pk_relation_name,
    'ID' AS pk_join_var_name,
    
    bucket_id     AS pred_value,
    COUNT(*) AS l0,
    SUM(POWER(deg, 1)) AS l1,
    SUM(POWER(deg, 2)) AS l2,
    SUM(POWER(deg, 3)) AS l3,
    SUM(POWER(deg, 4)) AS l4,
    SUM(POWER(deg, 5)) AS l5,
    SUM(POWER(deg, 6)) AS l6,
    SUM(POWER(deg, 7)) AS l7,
    SUM(POWER(deg, 8)) AS l8,
    SUM(POWER(deg, 9)) AS l9,
    SUM(POWER(deg, 10)) AS l10,
    MAX(deg) AS l_inf
FROM 
(
  SELECT
        MOVIE_ID, bucket_id, SUM(deg) as deg
        FROM Degree_MOVIE_COMPANIES_MOVIE_ID_ID_TITLE_PRODUCTION_YEAR_MOVIE_ID d
        JOIN TITLE_PRODUCTION_YEAR_histogram h
        ON (h.lower_bound IS NULL OR d.PRODUCTION_YEAR >= h.lower_bound)
        AND (h.upper_bound IS NULL OR d.PRODUCTION_YEAR <= h.upper_bound)
    WHERE MOVIE_ID IS NOT NULL AND bucket_id IS NOT NULL
    GROUP BY MOVIE_ID, bucket_id
)
GROUP BY bucket_id
ORDER BY bucket_id
;

----------------------------------
-- START: generate all lpnorm queries for MOVIE_INFO_IDX
----------------------------------


DROP TABLE IF EXISTS MOVIE_INFO_IDX_MOVIE_ID_ID_TITLE;

-- Create temp table MOVIE_INFO_IDX_MOVIE_ID_ID_TITLE (FK->PK join)
CREATE TEMP TABLE MOVIE_INFO_IDX_MOVIE_ID_ID_TITLE AS
SELECT
    MOVIE_INFO_IDX.MOVIE_ID AS MOVIE_ID,
    TITLE.PRODUCTION_YEAR AS TITLE_PRODUCTION_YEAR, TITLE.KIND_ID AS TITLE_KIND_ID
FROM MOVIE_INFO_IDX
JOIN TITLE
  ON MOVIE_INFO_IDX.MOVIE_ID = TITLE.ID
;

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

DROP TABLE IF EXISTS Degree_MOVIE_INFO_IDX_INFO_TYPE_ID_MOVIE_ID;

-- Create aggregator table Degree_MOVIE_INFO_IDX_INFO_TYPE_ID_MOVIE_ID for MCV MOVIE_INFO_IDX.INFO_TYPE_ID -- MCV
CREATE TEMP TABLE Degree_MOVIE_INFO_IDX_INFO_TYPE_ID_MOVIE_ID AS
SELECT
    mcvt.mcv_id AS mcv_id,
    MOVIE_INFO_IDX.MOVIE_ID AS MOVIE_ID,
    COUNT(*) AS deg
FROM MOVIE_INFO_IDX
JOIN MOVIE_INFO_IDX_INFO_TYPE_ID_mcv mcvt ON MOVIE_INFO_IDX.INFO_TYPE_ID = mcvt.INFO_TYPE_ID
WHERE MOVIE_INFO_IDX.MOVIE_ID IS NOT NULL
GROUP BY
    mcvt.mcv_id, MOVIE_INFO_IDX.MOVIE_ID
;

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    fk_join_var_name,
    pk_relation_name,
    pk_join_var_name,
    
  pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_INFO_IDX' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'MCV' AS aggregator_name,
    'INFO_TYPE_ID' AS pred_col_name,
    NULL AS fk_join_var_name,
    NULL AS pk_relation_name,
    NULL AS pk_join_var_name,
    
    mcv_id AS pred_value,
    COUNT(*) AS l0,
    SUM(POWER(deg, 1)) AS l1,
    SUM(POWER(deg, 2)) AS l2,
    SUM(POWER(deg, 3)) AS l3,
    SUM(POWER(deg, 4)) AS l4,
    SUM(POWER(deg, 5)) AS l5,
    SUM(POWER(deg, 6)) AS l6,
    SUM(POWER(deg, 7)) AS l7,
    SUM(POWER(deg, 8)) AS l8,
    SUM(POWER(deg, 9)) AS l9,
    SUM(POWER(deg, 10)) AS l10,
    MAX(deg) AS l_inf
FROM Degree_MOVIE_INFO_IDX_INFO_TYPE_ID_MOVIE_ID
GROUP BY mcv_id
;

DROP TABLE IF EXISTS Degree_MOVIE_INFO_IDX_INFO_TYPE_ID_MOVIE_ID;

-- Create aggregator table Degree_MOVIE_INFO_IDX_INFO_TYPE_ID_MOVIE_ID for NONMCV MOVIE_INFO_IDX.INFO_TYPE_ID -- MCV
CREATE TEMP TABLE Degree_MOVIE_INFO_IDX_INFO_TYPE_ID_MOVIE_ID AS
SELECT
    MOVIE_INFO_IDX.INFO_TYPE_ID AS INFO_TYPE_ID,
    MOVIE_INFO_IDX.MOVIE_ID AS MOVIE_ID,
    COUNT(*) AS deg
FROM MOVIE_INFO_IDX
LEFT JOIN MOVIE_INFO_IDX_INFO_TYPE_ID_mcv mcvt ON MOVIE_INFO_IDX.INFO_TYPE_ID = mcvt.INFO_TYPE_ID
WHERE MOVIE_INFO_IDX.MOVIE_ID IS NOT NULL AND mcvt.mcv_id IS NULL 
GROUP BY
    MOVIE_INFO_IDX.INFO_TYPE_ID, MOVIE_INFO_IDX.MOVIE_ID
;

-- Insert into norms for Non-MCV MOVIE_INFO_IDX INFO_TYPE_ID MOVIE_ID NONMCV
-- Among all non-MCV values in Degree_MOVIE_INFO_IDX_INFO_TYPE_ID_MOVIE_ID, find max of each Lp
WITH perValue AS (
  SELECT
    INFO_TYPE_ID,
    COUNT(*) AS l0, SUM(POWER(deg, 1)) AS l1, SUM(POWER(deg, 2)) AS l2, SUM(POWER(deg, 3)) AS l3, SUM(POWER(deg, 4)) AS l4, SUM(POWER(deg, 5)) AS l5, SUM(POWER(deg, 6)) AS l6, SUM(POWER(deg, 7)) AS l7, SUM(POWER(deg, 8)) AS l8, SUM(POWER(deg, 9)) AS l9, SUM(POWER(deg, 10)) AS l10, MAX(deg) AS l_inf
  FROM Degree_MOVIE_INFO_IDX_INFO_TYPE_ID_MOVIE_ID
  GROUP BY INFO_TYPE_ID
  HAVING INFO_TYPE_ID IS NOT NULL
)
INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    fk_join_var_name,
    pk_relation_name,
    pk_join_var_name,
    
  pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_INFO_IDX' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NONMCV' AS aggregator_name,
    'INFO_TYPE_ID' AS pred_col_name,
    NULL AS fk_join_var_name,
    NULL AS pk_relation_name,
    NULL AS pk_join_var_name,
    
  NULL              AS pred_value_id,
  MAX(l0) AS l0, MAX(l1) AS l1, MAX(l2) AS l2, MAX(l3) AS l3, MAX(l4) AS l4, MAX(l5) AS l5, MAX(l6) AS l6, MAX(l7) AS l7, MAX(l8) AS l8, MAX(l9) AS l9, MAX(l10) AS l10, MAX(l_inf) AS l_inf
FROM perValue
HAVING COUNT(*) > 0  -- If you need to ensure some rows are returned
;

DROP TABLE IF EXISTS Degree_MOVIE_INFO_IDX_MOVIE_ID_ID_TITLE_KIND_ID_MOVIE_ID;

-- Create aggregator table Degree_MOVIE_INFO_IDX_MOVIE_ID_ID_TITLE_KIND_ID_MOVIE_ID for MCV MOVIE_INFO_IDX_MOVIE_ID_ID_TITLE.TITLE_KIND_ID -- FKPK_MCV
CREATE TEMP TABLE Degree_MOVIE_INFO_IDX_MOVIE_ID_ID_TITLE_KIND_ID_MOVIE_ID AS
SELECT
    mcvt.mcv_id AS mcv_id,
    MOVIE_INFO_IDX_MOVIE_ID_ID_TITLE.MOVIE_ID AS MOVIE_ID,
    COUNT(*) AS deg
FROM MOVIE_INFO_IDX_MOVIE_ID_ID_TITLE
JOIN TITLE_KIND_ID_mcv mcvt ON MOVIE_INFO_IDX_MOVIE_ID_ID_TITLE.TITLE_KIND_ID = mcvt.KIND_ID
WHERE MOVIE_INFO_IDX_MOVIE_ID_ID_TITLE.MOVIE_ID IS NOT NULL
GROUP BY
    mcvt.mcv_id, MOVIE_INFO_IDX_MOVIE_ID_ID_TITLE.MOVIE_ID
;

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    fk_join_var_name,
    pk_relation_name,
    pk_join_var_name,
    
  pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_INFO_IDX' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'MCV' AS aggregator_name,
    'KIND_ID' AS pred_col_name,
    'MOVIE_ID' AS fk_join_var_name,
    'TITLE' AS pk_relation_name,
    'ID' AS pk_join_var_name,
    
    mcv_id AS pred_value,
    COUNT(*) AS l0,
    SUM(POWER(deg, 1)) AS l1,
    SUM(POWER(deg, 2)) AS l2,
    SUM(POWER(deg, 3)) AS l3,
    SUM(POWER(deg, 4)) AS l4,
    SUM(POWER(deg, 5)) AS l5,
    SUM(POWER(deg, 6)) AS l6,
    SUM(POWER(deg, 7)) AS l7,
    SUM(POWER(deg, 8)) AS l8,
    SUM(POWER(deg, 9)) AS l9,
    SUM(POWER(deg, 10)) AS l10,
    MAX(deg) AS l_inf
FROM Degree_MOVIE_INFO_IDX_MOVIE_ID_ID_TITLE_KIND_ID_MOVIE_ID
GROUP BY mcv_id
;

DROP TABLE IF EXISTS Degree_MOVIE_INFO_IDX_MOVIE_ID_ID_TITLE_KIND_ID_MOVIE_ID;

-- Create aggregator table Degree_MOVIE_INFO_IDX_MOVIE_ID_ID_TITLE_KIND_ID_MOVIE_ID for NONMCV MOVIE_INFO_IDX_MOVIE_ID_ID_TITLE.TITLE_KIND_ID -- FKPK_MCV
CREATE TEMP TABLE Degree_MOVIE_INFO_IDX_MOVIE_ID_ID_TITLE_KIND_ID_MOVIE_ID AS
SELECT
    MOVIE_INFO_IDX_MOVIE_ID_ID_TITLE.TITLE_KIND_ID AS KIND_ID,
    MOVIE_INFO_IDX_MOVIE_ID_ID_TITLE.MOVIE_ID AS MOVIE_ID,
    COUNT(*) AS deg
FROM MOVIE_INFO_IDX_MOVIE_ID_ID_TITLE
LEFT JOIN TITLE_KIND_ID_mcv mcvt ON MOVIE_INFO_IDX_MOVIE_ID_ID_TITLE.TITLE_KIND_ID = mcvt.KIND_ID
WHERE MOVIE_INFO_IDX_MOVIE_ID_ID_TITLE.MOVIE_ID IS NOT NULL AND mcvt.mcv_id IS NULL 
GROUP BY
    MOVIE_INFO_IDX_MOVIE_ID_ID_TITLE.TITLE_KIND_ID, MOVIE_INFO_IDX_MOVIE_ID_ID_TITLE.MOVIE_ID
;

-- Insert into norms for Non-MCV MOVIE_INFO_IDX_MOVIE_ID_ID_TITLE KIND_ID MOVIE_ID NONMCV
-- Among all non-MCV values in Degree_MOVIE_INFO_IDX_MOVIE_ID_ID_TITLE_KIND_ID_MOVIE_ID, find max of each Lp
WITH perValue AS (
  SELECT
    KIND_ID,
    COUNT(*) AS l0, SUM(POWER(deg, 1)) AS l1, SUM(POWER(deg, 2)) AS l2, SUM(POWER(deg, 3)) AS l3, SUM(POWER(deg, 4)) AS l4, SUM(POWER(deg, 5)) AS l5, SUM(POWER(deg, 6)) AS l6, SUM(POWER(deg, 7)) AS l7, SUM(POWER(deg, 8)) AS l8, SUM(POWER(deg, 9)) AS l9, SUM(POWER(deg, 10)) AS l10, MAX(deg) AS l_inf
  FROM Degree_MOVIE_INFO_IDX_MOVIE_ID_ID_TITLE_KIND_ID_MOVIE_ID
  GROUP BY KIND_ID
  HAVING KIND_ID IS NOT NULL
)
INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    fk_join_var_name,
    pk_relation_name,
    pk_join_var_name,
    
  pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_INFO_IDX' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NONMCV' AS aggregator_name,
    'KIND_ID' AS pred_col_name,
    'MOVIE_ID' AS fk_join_var_name,
    'TITLE' AS pk_relation_name,
    'ID' AS pk_join_var_name,
    
  NULL              AS pred_value_id,
  MAX(l0) AS l0, MAX(l1) AS l1, MAX(l2) AS l2, MAX(l3) AS l3, MAX(l4) AS l4, MAX(l5) AS l5, MAX(l6) AS l6, MAX(l7) AS l7, MAX(l8) AS l8, MAX(l9) AS l9, MAX(l10) AS l10, MAX(l_inf) AS l_inf
FROM perValue
HAVING COUNT(*) > 0  -- If you need to ensure some rows are returned
;

DROP TABLE IF EXISTS Degree_MOVIE_INFO_IDX_MOVIE_ID_ID_TITLE_PRODUCTION_YEAR_MOVIE_ID;

-- Create aggregator table Degree_MOVIE_INFO_IDX_MOVIE_ID_ID_TITLE_PRODUCTION_YEAR_MOVIE_ID for MCV MOVIE_INFO_IDX_MOVIE_ID_ID_TITLE.TITLE_PRODUCTION_YEAR -- FKPK_MCV
CREATE TEMP TABLE Degree_MOVIE_INFO_IDX_MOVIE_ID_ID_TITLE_PRODUCTION_YEAR_MOVIE_ID AS
SELECT
    mcvt.mcv_id AS mcv_id,
    MOVIE_INFO_IDX_MOVIE_ID_ID_TITLE.MOVIE_ID AS MOVIE_ID,
    COUNT(*) AS deg
FROM MOVIE_INFO_IDX_MOVIE_ID_ID_TITLE
JOIN TITLE_PRODUCTION_YEAR_mcv mcvt ON MOVIE_INFO_IDX_MOVIE_ID_ID_TITLE.TITLE_PRODUCTION_YEAR = mcvt.PRODUCTION_YEAR
WHERE MOVIE_INFO_IDX_MOVIE_ID_ID_TITLE.MOVIE_ID IS NOT NULL
GROUP BY
    mcvt.mcv_id, MOVIE_INFO_IDX_MOVIE_ID_ID_TITLE.MOVIE_ID
;

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    fk_join_var_name,
    pk_relation_name,
    pk_join_var_name,
    
  pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_INFO_IDX' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'MCV' AS aggregator_name,
    'PRODUCTION_YEAR' AS pred_col_name,
    'MOVIE_ID' AS fk_join_var_name,
    'TITLE' AS pk_relation_name,
    'ID' AS pk_join_var_name,
    
    mcv_id AS pred_value,
    COUNT(*) AS l0,
    SUM(POWER(deg, 1)) AS l1,
    SUM(POWER(deg, 2)) AS l2,
    SUM(POWER(deg, 3)) AS l3,
    SUM(POWER(deg, 4)) AS l4,
    SUM(POWER(deg, 5)) AS l5,
    SUM(POWER(deg, 6)) AS l6,
    SUM(POWER(deg, 7)) AS l7,
    SUM(POWER(deg, 8)) AS l8,
    SUM(POWER(deg, 9)) AS l9,
    SUM(POWER(deg, 10)) AS l10,
    MAX(deg) AS l_inf
FROM Degree_MOVIE_INFO_IDX_MOVIE_ID_ID_TITLE_PRODUCTION_YEAR_MOVIE_ID
GROUP BY mcv_id
;

DROP TABLE IF EXISTS Degree_MOVIE_INFO_IDX_MOVIE_ID_ID_TITLE_PRODUCTION_YEAR_MOVIE_ID;

-- Create aggregator table Degree_MOVIE_INFO_IDX_MOVIE_ID_ID_TITLE_PRODUCTION_YEAR_MOVIE_ID for NONMCV MOVIE_INFO_IDX_MOVIE_ID_ID_TITLE.TITLE_PRODUCTION_YEAR -- FKPK_MCV
CREATE TEMP TABLE Degree_MOVIE_INFO_IDX_MOVIE_ID_ID_TITLE_PRODUCTION_YEAR_MOVIE_ID AS
SELECT
    MOVIE_INFO_IDX_MOVIE_ID_ID_TITLE.TITLE_PRODUCTION_YEAR AS PRODUCTION_YEAR,
    MOVIE_INFO_IDX_MOVIE_ID_ID_TITLE.MOVIE_ID AS MOVIE_ID,
    COUNT(*) AS deg
FROM MOVIE_INFO_IDX_MOVIE_ID_ID_TITLE
LEFT JOIN TITLE_PRODUCTION_YEAR_mcv mcvt ON MOVIE_INFO_IDX_MOVIE_ID_ID_TITLE.TITLE_PRODUCTION_YEAR = mcvt.PRODUCTION_YEAR
WHERE MOVIE_INFO_IDX_MOVIE_ID_ID_TITLE.MOVIE_ID IS NOT NULL AND mcvt.mcv_id IS NULL 
GROUP BY
    MOVIE_INFO_IDX_MOVIE_ID_ID_TITLE.TITLE_PRODUCTION_YEAR, MOVIE_INFO_IDX_MOVIE_ID_ID_TITLE.MOVIE_ID
;

-- Insert into norms for Non-MCV MOVIE_INFO_IDX_MOVIE_ID_ID_TITLE PRODUCTION_YEAR MOVIE_ID NONMCV
-- Among all non-MCV values in Degree_MOVIE_INFO_IDX_MOVIE_ID_ID_TITLE_PRODUCTION_YEAR_MOVIE_ID, find max of each Lp
WITH perValue AS (
  SELECT
    PRODUCTION_YEAR,
    COUNT(*) AS l0, SUM(POWER(deg, 1)) AS l1, SUM(POWER(deg, 2)) AS l2, SUM(POWER(deg, 3)) AS l3, SUM(POWER(deg, 4)) AS l4, SUM(POWER(deg, 5)) AS l5, SUM(POWER(deg, 6)) AS l6, SUM(POWER(deg, 7)) AS l7, SUM(POWER(deg, 8)) AS l8, SUM(POWER(deg, 9)) AS l9, SUM(POWER(deg, 10)) AS l10, MAX(deg) AS l_inf
  FROM Degree_MOVIE_INFO_IDX_MOVIE_ID_ID_TITLE_PRODUCTION_YEAR_MOVIE_ID
  GROUP BY PRODUCTION_YEAR
  HAVING PRODUCTION_YEAR IS NOT NULL
)
INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    fk_join_var_name,
    pk_relation_name,
    pk_join_var_name,
    
  pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_INFO_IDX' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NONMCV' AS aggregator_name,
    'PRODUCTION_YEAR' AS pred_col_name,
    'MOVIE_ID' AS fk_join_var_name,
    'TITLE' AS pk_relation_name,
    'ID' AS pk_join_var_name,
    
  NULL              AS pred_value_id,
  MAX(l0) AS l0, MAX(l1) AS l1, MAX(l2) AS l2, MAX(l3) AS l3, MAX(l4) AS l4, MAX(l5) AS l5, MAX(l6) AS l6, MAX(l7) AS l7, MAX(l8) AS l8, MAX(l9) AS l9, MAX(l10) AS l10, MAX(l_inf) AS l_inf
FROM perValue
HAVING COUNT(*) > 0  -- If you need to ensure some rows are returned
;

DROP TABLE IF EXISTS Degree_MOVIE_INFO_IDX_MOVIE_ID_ID_TITLE_PRODUCTION_YEAR_MOVIE_ID;

-- Create aggregator table Degree_MOVIE_INFO_IDX_MOVIE_ID_ID_TITLE_PRODUCTION_YEAR_MOVIE_ID for Histogram MOVIE_INFO_IDX_MOVIE_ID_ID_TITLE.TITLE_PRODUCTION_YEAR -- FKPK_RANGE
CREATE TEMP TABLE Degree_MOVIE_INFO_IDX_MOVIE_ID_ID_TITLE_PRODUCTION_YEAR_MOVIE_ID AS
SELECT
    MOVIE_INFO_IDX_MOVIE_ID_ID_TITLE.TITLE_PRODUCTION_YEAR AS PRODUCTION_YEAR,
    MOVIE_INFO_IDX_MOVIE_ID_ID_TITLE.MOVIE_ID AS MOVIE_ID,
    COUNT(*) AS deg
FROM MOVIE_INFO_IDX_MOVIE_ID_ID_TITLE

WHERE MOVIE_INFO_IDX_MOVIE_ID_ID_TITLE.MOVIE_ID IS NOT NULL
GROUP BY
    MOVIE_INFO_IDX_MOVIE_ID_ID_TITLE.TITLE_PRODUCTION_YEAR, MOVIE_INFO_IDX_MOVIE_ID_ID_TITLE.MOVIE_ID
;

INSERT INTO norms(

    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    fk_join_var_name,
    pk_relation_name,
    pk_join_var_name,
    
pred_value_id,
l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_INFO_IDX' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'RANGE' AS aggregator_name,
    'PRODUCTION_YEAR' AS pred_col_name,
    'MOVIE_ID' AS fk_join_var_name,
    'TITLE' AS pk_relation_name,
    'ID' AS pk_join_var_name,
    
    bucket_id     AS pred_value,
    COUNT(*) AS l0,
    SUM(POWER(deg, 1)) AS l1,
    SUM(POWER(deg, 2)) AS l2,
    SUM(POWER(deg, 3)) AS l3,
    SUM(POWER(deg, 4)) AS l4,
    SUM(POWER(deg, 5)) AS l5,
    SUM(POWER(deg, 6)) AS l6,
    SUM(POWER(deg, 7)) AS l7,
    SUM(POWER(deg, 8)) AS l8,
    SUM(POWER(deg, 9)) AS l9,
    SUM(POWER(deg, 10)) AS l10,
    MAX(deg) AS l_inf
FROM 
(
  SELECT
        MOVIE_ID, bucket_id, SUM(deg) as deg
        FROM Degree_MOVIE_INFO_IDX_MOVIE_ID_ID_TITLE_PRODUCTION_YEAR_MOVIE_ID d
        JOIN TITLE_PRODUCTION_YEAR_histogram h
        ON (h.lower_bound IS NULL OR d.PRODUCTION_YEAR >= h.lower_bound)
        AND (h.upper_bound IS NULL OR d.PRODUCTION_YEAR <= h.upper_bound)
    WHERE MOVIE_ID IS NOT NULL AND bucket_id IS NOT NULL
    GROUP BY MOVIE_ID, bucket_id
)
GROUP BY bucket_id
ORDER BY bucket_id
;

----------------------------------
-- START: generate all lpnorm queries for MOVIE_KEYWORD
----------------------------------


DROP TABLE IF EXISTS MOVIE_KEYWORD_MOVIE_ID_ID_TITLE;

-- Create temp table MOVIE_KEYWORD_MOVIE_ID_ID_TITLE (FK->PK join)
CREATE TEMP TABLE MOVIE_KEYWORD_MOVIE_ID_ID_TITLE AS
SELECT
    MOVIE_KEYWORD.MOVIE_ID AS MOVIE_ID,
    TITLE.PRODUCTION_YEAR AS TITLE_PRODUCTION_YEAR, TITLE.KIND_ID AS TITLE_KIND_ID
FROM MOVIE_KEYWORD
JOIN TITLE
  ON MOVIE_KEYWORD.MOVIE_ID = TITLE.ID
;

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

DROP TABLE IF EXISTS Degree_MOVIE_KEYWORD_KEYWORD_ID_MOVIE_ID;

-- Create aggregator table Degree_MOVIE_KEYWORD_KEYWORD_ID_MOVIE_ID for MCV MOVIE_KEYWORD.KEYWORD_ID -- MCV
CREATE TEMP TABLE Degree_MOVIE_KEYWORD_KEYWORD_ID_MOVIE_ID AS
SELECT
    mcvt.mcv_id AS mcv_id,
    MOVIE_KEYWORD.MOVIE_ID AS MOVIE_ID,
    COUNT(*) AS deg
FROM MOVIE_KEYWORD
JOIN MOVIE_KEYWORD_KEYWORD_ID_mcv mcvt ON MOVIE_KEYWORD.KEYWORD_ID = mcvt.KEYWORD_ID
WHERE MOVIE_KEYWORD.MOVIE_ID IS NOT NULL
GROUP BY
    mcvt.mcv_id, MOVIE_KEYWORD.MOVIE_ID
;

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    fk_join_var_name,
    pk_relation_name,
    pk_join_var_name,
    
  pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_KEYWORD' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'MCV' AS aggregator_name,
    'KEYWORD_ID' AS pred_col_name,
    NULL AS fk_join_var_name,
    NULL AS pk_relation_name,
    NULL AS pk_join_var_name,
    
    mcv_id AS pred_value,
    COUNT(*) AS l0,
    SUM(POWER(deg, 1)) AS l1,
    SUM(POWER(deg, 2)) AS l2,
    SUM(POWER(deg, 3)) AS l3,
    SUM(POWER(deg, 4)) AS l4,
    SUM(POWER(deg, 5)) AS l5,
    SUM(POWER(deg, 6)) AS l6,
    SUM(POWER(deg, 7)) AS l7,
    SUM(POWER(deg, 8)) AS l8,
    SUM(POWER(deg, 9)) AS l9,
    SUM(POWER(deg, 10)) AS l10,
    MAX(deg) AS l_inf
FROM Degree_MOVIE_KEYWORD_KEYWORD_ID_MOVIE_ID
GROUP BY mcv_id
;

DROP TABLE IF EXISTS Degree_MOVIE_KEYWORD_KEYWORD_ID_MOVIE_ID;

-- Create aggregator table Degree_MOVIE_KEYWORD_KEYWORD_ID_MOVIE_ID for NONMCV MOVIE_KEYWORD.KEYWORD_ID -- MCV
CREATE TEMP TABLE Degree_MOVIE_KEYWORD_KEYWORD_ID_MOVIE_ID AS
SELECT
    MOVIE_KEYWORD.KEYWORD_ID AS KEYWORD_ID,
    MOVIE_KEYWORD.MOVIE_ID AS MOVIE_ID,
    COUNT(*) AS deg
FROM MOVIE_KEYWORD
LEFT JOIN MOVIE_KEYWORD_KEYWORD_ID_mcv mcvt ON MOVIE_KEYWORD.KEYWORD_ID = mcvt.KEYWORD_ID
WHERE MOVIE_KEYWORD.MOVIE_ID IS NOT NULL AND mcvt.mcv_id IS NULL 
GROUP BY
    MOVIE_KEYWORD.KEYWORD_ID, MOVIE_KEYWORD.MOVIE_ID
;

-- Insert into norms for Non-MCV MOVIE_KEYWORD KEYWORD_ID MOVIE_ID NONMCV
-- Among all non-MCV values in Degree_MOVIE_KEYWORD_KEYWORD_ID_MOVIE_ID, find max of each Lp
WITH perValue AS (
  SELECT
    KEYWORD_ID,
    COUNT(*) AS l0, SUM(POWER(deg, 1)) AS l1, SUM(POWER(deg, 2)) AS l2, SUM(POWER(deg, 3)) AS l3, SUM(POWER(deg, 4)) AS l4, SUM(POWER(deg, 5)) AS l5, SUM(POWER(deg, 6)) AS l6, SUM(POWER(deg, 7)) AS l7, SUM(POWER(deg, 8)) AS l8, SUM(POWER(deg, 9)) AS l9, SUM(POWER(deg, 10)) AS l10, MAX(deg) AS l_inf
  FROM Degree_MOVIE_KEYWORD_KEYWORD_ID_MOVIE_ID
  GROUP BY KEYWORD_ID
  HAVING KEYWORD_ID IS NOT NULL
)
INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    fk_join_var_name,
    pk_relation_name,
    pk_join_var_name,
    
  pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_KEYWORD' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NONMCV' AS aggregator_name,
    'KEYWORD_ID' AS pred_col_name,
    NULL AS fk_join_var_name,
    NULL AS pk_relation_name,
    NULL AS pk_join_var_name,
    
  NULL              AS pred_value_id,
  MAX(l0) AS l0, MAX(l1) AS l1, MAX(l2) AS l2, MAX(l3) AS l3, MAX(l4) AS l4, MAX(l5) AS l5, MAX(l6) AS l6, MAX(l7) AS l7, MAX(l8) AS l8, MAX(l9) AS l9, MAX(l10) AS l10, MAX(l_inf) AS l_inf
FROM perValue
HAVING COUNT(*) > 0  -- If you need to ensure some rows are returned
;

DROP TABLE IF EXISTS Degree_MOVIE_KEYWORD_MOVIE_ID_ID_TITLE_KIND_ID_MOVIE_ID;

-- Create aggregator table Degree_MOVIE_KEYWORD_MOVIE_ID_ID_TITLE_KIND_ID_MOVIE_ID for MCV MOVIE_KEYWORD_MOVIE_ID_ID_TITLE.TITLE_KIND_ID -- FKPK_MCV
CREATE TEMP TABLE Degree_MOVIE_KEYWORD_MOVIE_ID_ID_TITLE_KIND_ID_MOVIE_ID AS
SELECT
    mcvt.mcv_id AS mcv_id,
    MOVIE_KEYWORD_MOVIE_ID_ID_TITLE.MOVIE_ID AS MOVIE_ID,
    COUNT(*) AS deg
FROM MOVIE_KEYWORD_MOVIE_ID_ID_TITLE
JOIN TITLE_KIND_ID_mcv mcvt ON MOVIE_KEYWORD_MOVIE_ID_ID_TITLE.TITLE_KIND_ID = mcvt.KIND_ID
WHERE MOVIE_KEYWORD_MOVIE_ID_ID_TITLE.MOVIE_ID IS NOT NULL
GROUP BY
    mcvt.mcv_id, MOVIE_KEYWORD_MOVIE_ID_ID_TITLE.MOVIE_ID
;

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    fk_join_var_name,
    pk_relation_name,
    pk_join_var_name,
    
  pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_KEYWORD' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'MCV' AS aggregator_name,
    'KIND_ID' AS pred_col_name,
    'MOVIE_ID' AS fk_join_var_name,
    'TITLE' AS pk_relation_name,
    'ID' AS pk_join_var_name,
    
    mcv_id AS pred_value,
    COUNT(*) AS l0,
    SUM(POWER(deg, 1)) AS l1,
    SUM(POWER(deg, 2)) AS l2,
    SUM(POWER(deg, 3)) AS l3,
    SUM(POWER(deg, 4)) AS l4,
    SUM(POWER(deg, 5)) AS l5,
    SUM(POWER(deg, 6)) AS l6,
    SUM(POWER(deg, 7)) AS l7,
    SUM(POWER(deg, 8)) AS l8,
    SUM(POWER(deg, 9)) AS l9,
    SUM(POWER(deg, 10)) AS l10,
    MAX(deg) AS l_inf
FROM Degree_MOVIE_KEYWORD_MOVIE_ID_ID_TITLE_KIND_ID_MOVIE_ID
GROUP BY mcv_id
;

DROP TABLE IF EXISTS Degree_MOVIE_KEYWORD_MOVIE_ID_ID_TITLE_KIND_ID_MOVIE_ID;

-- Create aggregator table Degree_MOVIE_KEYWORD_MOVIE_ID_ID_TITLE_KIND_ID_MOVIE_ID for NONMCV MOVIE_KEYWORD_MOVIE_ID_ID_TITLE.TITLE_KIND_ID -- FKPK_MCV
CREATE TEMP TABLE Degree_MOVIE_KEYWORD_MOVIE_ID_ID_TITLE_KIND_ID_MOVIE_ID AS
SELECT
    MOVIE_KEYWORD_MOVIE_ID_ID_TITLE.TITLE_KIND_ID AS KIND_ID,
    MOVIE_KEYWORD_MOVIE_ID_ID_TITLE.MOVIE_ID AS MOVIE_ID,
    COUNT(*) AS deg
FROM MOVIE_KEYWORD_MOVIE_ID_ID_TITLE
LEFT JOIN TITLE_KIND_ID_mcv mcvt ON MOVIE_KEYWORD_MOVIE_ID_ID_TITLE.TITLE_KIND_ID = mcvt.KIND_ID
WHERE MOVIE_KEYWORD_MOVIE_ID_ID_TITLE.MOVIE_ID IS NOT NULL AND mcvt.mcv_id IS NULL 
GROUP BY
    MOVIE_KEYWORD_MOVIE_ID_ID_TITLE.TITLE_KIND_ID, MOVIE_KEYWORD_MOVIE_ID_ID_TITLE.MOVIE_ID
;

-- Insert into norms for Non-MCV MOVIE_KEYWORD_MOVIE_ID_ID_TITLE KIND_ID MOVIE_ID NONMCV
-- Among all non-MCV values in Degree_MOVIE_KEYWORD_MOVIE_ID_ID_TITLE_KIND_ID_MOVIE_ID, find max of each Lp
WITH perValue AS (
  SELECT
    KIND_ID,
    COUNT(*) AS l0, SUM(POWER(deg, 1)) AS l1, SUM(POWER(deg, 2)) AS l2, SUM(POWER(deg, 3)) AS l3, SUM(POWER(deg, 4)) AS l4, SUM(POWER(deg, 5)) AS l5, SUM(POWER(deg, 6)) AS l6, SUM(POWER(deg, 7)) AS l7, SUM(POWER(deg, 8)) AS l8, SUM(POWER(deg, 9)) AS l9, SUM(POWER(deg, 10)) AS l10, MAX(deg) AS l_inf
  FROM Degree_MOVIE_KEYWORD_MOVIE_ID_ID_TITLE_KIND_ID_MOVIE_ID
  GROUP BY KIND_ID
  HAVING KIND_ID IS NOT NULL
)
INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    fk_join_var_name,
    pk_relation_name,
    pk_join_var_name,
    
  pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_KEYWORD' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NONMCV' AS aggregator_name,
    'KIND_ID' AS pred_col_name,
    'MOVIE_ID' AS fk_join_var_name,
    'TITLE' AS pk_relation_name,
    'ID' AS pk_join_var_name,
    
  NULL              AS pred_value_id,
  MAX(l0) AS l0, MAX(l1) AS l1, MAX(l2) AS l2, MAX(l3) AS l3, MAX(l4) AS l4, MAX(l5) AS l5, MAX(l6) AS l6, MAX(l7) AS l7, MAX(l8) AS l8, MAX(l9) AS l9, MAX(l10) AS l10, MAX(l_inf) AS l_inf
FROM perValue
HAVING COUNT(*) > 0  -- If you need to ensure some rows are returned
;

DROP TABLE IF EXISTS Degree_MOVIE_KEYWORD_MOVIE_ID_ID_TITLE_PRODUCTION_YEAR_MOVIE_ID;

-- Create aggregator table Degree_MOVIE_KEYWORD_MOVIE_ID_ID_TITLE_PRODUCTION_YEAR_MOVIE_ID for MCV MOVIE_KEYWORD_MOVIE_ID_ID_TITLE.TITLE_PRODUCTION_YEAR -- FKPK_MCV
CREATE TEMP TABLE Degree_MOVIE_KEYWORD_MOVIE_ID_ID_TITLE_PRODUCTION_YEAR_MOVIE_ID AS
SELECT
    mcvt.mcv_id AS mcv_id,
    MOVIE_KEYWORD_MOVIE_ID_ID_TITLE.MOVIE_ID AS MOVIE_ID,
    COUNT(*) AS deg
FROM MOVIE_KEYWORD_MOVIE_ID_ID_TITLE
JOIN TITLE_PRODUCTION_YEAR_mcv mcvt ON MOVIE_KEYWORD_MOVIE_ID_ID_TITLE.TITLE_PRODUCTION_YEAR = mcvt.PRODUCTION_YEAR
WHERE MOVIE_KEYWORD_MOVIE_ID_ID_TITLE.MOVIE_ID IS NOT NULL
GROUP BY
    mcvt.mcv_id, MOVIE_KEYWORD_MOVIE_ID_ID_TITLE.MOVIE_ID
;

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    fk_join_var_name,
    pk_relation_name,
    pk_join_var_name,
    
  pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_KEYWORD' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'MCV' AS aggregator_name,
    'PRODUCTION_YEAR' AS pred_col_name,
    'MOVIE_ID' AS fk_join_var_name,
    'TITLE' AS pk_relation_name,
    'ID' AS pk_join_var_name,
    
    mcv_id AS pred_value,
    COUNT(*) AS l0,
    SUM(POWER(deg, 1)) AS l1,
    SUM(POWER(deg, 2)) AS l2,
    SUM(POWER(deg, 3)) AS l3,
    SUM(POWER(deg, 4)) AS l4,
    SUM(POWER(deg, 5)) AS l5,
    SUM(POWER(deg, 6)) AS l6,
    SUM(POWER(deg, 7)) AS l7,
    SUM(POWER(deg, 8)) AS l8,
    SUM(POWER(deg, 9)) AS l9,
    SUM(POWER(deg, 10)) AS l10,
    MAX(deg) AS l_inf
FROM Degree_MOVIE_KEYWORD_MOVIE_ID_ID_TITLE_PRODUCTION_YEAR_MOVIE_ID
GROUP BY mcv_id
;

DROP TABLE IF EXISTS Degree_MOVIE_KEYWORD_MOVIE_ID_ID_TITLE_PRODUCTION_YEAR_MOVIE_ID;

-- Create aggregator table Degree_MOVIE_KEYWORD_MOVIE_ID_ID_TITLE_PRODUCTION_YEAR_MOVIE_ID for NONMCV MOVIE_KEYWORD_MOVIE_ID_ID_TITLE.TITLE_PRODUCTION_YEAR -- FKPK_MCV
CREATE TEMP TABLE Degree_MOVIE_KEYWORD_MOVIE_ID_ID_TITLE_PRODUCTION_YEAR_MOVIE_ID AS
SELECT
    MOVIE_KEYWORD_MOVIE_ID_ID_TITLE.TITLE_PRODUCTION_YEAR AS PRODUCTION_YEAR,
    MOVIE_KEYWORD_MOVIE_ID_ID_TITLE.MOVIE_ID AS MOVIE_ID,
    COUNT(*) AS deg
FROM MOVIE_KEYWORD_MOVIE_ID_ID_TITLE
LEFT JOIN TITLE_PRODUCTION_YEAR_mcv mcvt ON MOVIE_KEYWORD_MOVIE_ID_ID_TITLE.TITLE_PRODUCTION_YEAR = mcvt.PRODUCTION_YEAR
WHERE MOVIE_KEYWORD_MOVIE_ID_ID_TITLE.MOVIE_ID IS NOT NULL AND mcvt.mcv_id IS NULL 
GROUP BY
    MOVIE_KEYWORD_MOVIE_ID_ID_TITLE.TITLE_PRODUCTION_YEAR, MOVIE_KEYWORD_MOVIE_ID_ID_TITLE.MOVIE_ID
;

-- Insert into norms for Non-MCV MOVIE_KEYWORD_MOVIE_ID_ID_TITLE PRODUCTION_YEAR MOVIE_ID NONMCV
-- Among all non-MCV values in Degree_MOVIE_KEYWORD_MOVIE_ID_ID_TITLE_PRODUCTION_YEAR_MOVIE_ID, find max of each Lp
WITH perValue AS (
  SELECT
    PRODUCTION_YEAR,
    COUNT(*) AS l0, SUM(POWER(deg, 1)) AS l1, SUM(POWER(deg, 2)) AS l2, SUM(POWER(deg, 3)) AS l3, SUM(POWER(deg, 4)) AS l4, SUM(POWER(deg, 5)) AS l5, SUM(POWER(deg, 6)) AS l6, SUM(POWER(deg, 7)) AS l7, SUM(POWER(deg, 8)) AS l8, SUM(POWER(deg, 9)) AS l9, SUM(POWER(deg, 10)) AS l10, MAX(deg) AS l_inf
  FROM Degree_MOVIE_KEYWORD_MOVIE_ID_ID_TITLE_PRODUCTION_YEAR_MOVIE_ID
  GROUP BY PRODUCTION_YEAR
  HAVING PRODUCTION_YEAR IS NOT NULL
)
INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    fk_join_var_name,
    pk_relation_name,
    pk_join_var_name,
    
  pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_KEYWORD' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NONMCV' AS aggregator_name,
    'PRODUCTION_YEAR' AS pred_col_name,
    'MOVIE_ID' AS fk_join_var_name,
    'TITLE' AS pk_relation_name,
    'ID' AS pk_join_var_name,
    
  NULL              AS pred_value_id,
  MAX(l0) AS l0, MAX(l1) AS l1, MAX(l2) AS l2, MAX(l3) AS l3, MAX(l4) AS l4, MAX(l5) AS l5, MAX(l6) AS l6, MAX(l7) AS l7, MAX(l8) AS l8, MAX(l9) AS l9, MAX(l10) AS l10, MAX(l_inf) AS l_inf
FROM perValue
HAVING COUNT(*) > 0  -- If you need to ensure some rows are returned
;

DROP TABLE IF EXISTS Degree_MOVIE_KEYWORD_MOVIE_ID_ID_TITLE_PRODUCTION_YEAR_MOVIE_ID;

-- Create aggregator table Degree_MOVIE_KEYWORD_MOVIE_ID_ID_TITLE_PRODUCTION_YEAR_MOVIE_ID for Histogram MOVIE_KEYWORD_MOVIE_ID_ID_TITLE.TITLE_PRODUCTION_YEAR -- FKPK_RANGE
CREATE TEMP TABLE Degree_MOVIE_KEYWORD_MOVIE_ID_ID_TITLE_PRODUCTION_YEAR_MOVIE_ID AS
SELECT
    MOVIE_KEYWORD_MOVIE_ID_ID_TITLE.TITLE_PRODUCTION_YEAR AS PRODUCTION_YEAR,
    MOVIE_KEYWORD_MOVIE_ID_ID_TITLE.MOVIE_ID AS MOVIE_ID,
    COUNT(*) AS deg
FROM MOVIE_KEYWORD_MOVIE_ID_ID_TITLE

WHERE MOVIE_KEYWORD_MOVIE_ID_ID_TITLE.MOVIE_ID IS NOT NULL
GROUP BY
    MOVIE_KEYWORD_MOVIE_ID_ID_TITLE.TITLE_PRODUCTION_YEAR, MOVIE_KEYWORD_MOVIE_ID_ID_TITLE.MOVIE_ID
;

INSERT INTO norms(

    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    fk_join_var_name,
    pk_relation_name,
    pk_join_var_name,
    
pred_value_id,
l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_KEYWORD' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'RANGE' AS aggregator_name,
    'PRODUCTION_YEAR' AS pred_col_name,
    'MOVIE_ID' AS fk_join_var_name,
    'TITLE' AS pk_relation_name,
    'ID' AS pk_join_var_name,
    
    bucket_id     AS pred_value,
    COUNT(*) AS l0,
    SUM(POWER(deg, 1)) AS l1,
    SUM(POWER(deg, 2)) AS l2,
    SUM(POWER(deg, 3)) AS l3,
    SUM(POWER(deg, 4)) AS l4,
    SUM(POWER(deg, 5)) AS l5,
    SUM(POWER(deg, 6)) AS l6,
    SUM(POWER(deg, 7)) AS l7,
    SUM(POWER(deg, 8)) AS l8,
    SUM(POWER(deg, 9)) AS l9,
    SUM(POWER(deg, 10)) AS l10,
    MAX(deg) AS l_inf
FROM 
(
  SELECT
        MOVIE_ID, bucket_id, SUM(deg) as deg
        FROM Degree_MOVIE_KEYWORD_MOVIE_ID_ID_TITLE_PRODUCTION_YEAR_MOVIE_ID d
        JOIN TITLE_PRODUCTION_YEAR_histogram h
        ON (h.lower_bound IS NULL OR d.PRODUCTION_YEAR >= h.lower_bound)
        AND (h.upper_bound IS NULL OR d.PRODUCTION_YEAR <= h.upper_bound)
    WHERE MOVIE_ID IS NOT NULL AND bucket_id IS NOT NULL
    GROUP BY MOVIE_ID, bucket_id
)
GROUP BY bucket_id
ORDER BY bucket_id
;

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

DROP TABLE IF EXISTS Degree_TITLE_KIND_ID_ID;

-- Create aggregator table Degree_TITLE_KIND_ID_ID for MCV TITLE.KIND_ID -- MCV
CREATE TEMP TABLE Degree_TITLE_KIND_ID_ID AS
SELECT
    mcvt.mcv_id AS mcv_id,
    TITLE.ID AS ID,
    COUNT(*) AS deg
FROM TITLE
JOIN TITLE_KIND_ID_mcv mcvt ON TITLE.KIND_ID = mcvt.KIND_ID
WHERE TITLE.ID IS NOT NULL
GROUP BY
    mcvt.mcv_id, TITLE.ID
;

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    fk_join_var_name,
    pk_relation_name,
    pk_join_var_name,
    
  pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'TITLE' AS relation_name,
    'ID' AS join_var_name,
    'MCV' AS aggregator_name,
    'KIND_ID' AS pred_col_name,
    NULL AS fk_join_var_name,
    NULL AS pk_relation_name,
    NULL AS pk_join_var_name,
    
    mcv_id AS pred_value,
    COUNT(*) AS l0,
    SUM(POWER(deg, 1)) AS l1,
    SUM(POWER(deg, 2)) AS l2,
    SUM(POWER(deg, 3)) AS l3,
    SUM(POWER(deg, 4)) AS l4,
    SUM(POWER(deg, 5)) AS l5,
    SUM(POWER(deg, 6)) AS l6,
    SUM(POWER(deg, 7)) AS l7,
    SUM(POWER(deg, 8)) AS l8,
    SUM(POWER(deg, 9)) AS l9,
    SUM(POWER(deg, 10)) AS l10,
    MAX(deg) AS l_inf
FROM Degree_TITLE_KIND_ID_ID
GROUP BY mcv_id
;

DROP TABLE IF EXISTS Degree_TITLE_KIND_ID_ID;

-- Create aggregator table Degree_TITLE_KIND_ID_ID for NONMCV TITLE.KIND_ID -- MCV
CREATE TEMP TABLE Degree_TITLE_KIND_ID_ID AS
SELECT
    TITLE.KIND_ID AS KIND_ID,
    TITLE.ID AS ID,
    COUNT(*) AS deg
FROM TITLE
LEFT JOIN TITLE_KIND_ID_mcv mcvt ON TITLE.KIND_ID = mcvt.KIND_ID
WHERE TITLE.ID IS NOT NULL AND mcvt.mcv_id IS NULL 
GROUP BY
    TITLE.KIND_ID, TITLE.ID
;

-- Insert into norms for Non-MCV TITLE KIND_ID ID NONMCV
-- Among all non-MCV values in Degree_TITLE_KIND_ID_ID, find max of each Lp
WITH perValue AS (
  SELECT
    KIND_ID,
    COUNT(*) AS l0, SUM(POWER(deg, 1)) AS l1, SUM(POWER(deg, 2)) AS l2, SUM(POWER(deg, 3)) AS l3, SUM(POWER(deg, 4)) AS l4, SUM(POWER(deg, 5)) AS l5, SUM(POWER(deg, 6)) AS l6, SUM(POWER(deg, 7)) AS l7, SUM(POWER(deg, 8)) AS l8, SUM(POWER(deg, 9)) AS l9, SUM(POWER(deg, 10)) AS l10, MAX(deg) AS l_inf
  FROM Degree_TITLE_KIND_ID_ID
  GROUP BY KIND_ID
  HAVING KIND_ID IS NOT NULL
)
INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    fk_join_var_name,
    pk_relation_name,
    pk_join_var_name,
    
  pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'TITLE' AS relation_name,
    'ID' AS join_var_name,
    'NONMCV' AS aggregator_name,
    'KIND_ID' AS pred_col_name,
    NULL AS fk_join_var_name,
    NULL AS pk_relation_name,
    NULL AS pk_join_var_name,
    
  NULL              AS pred_value_id,
  MAX(l0) AS l0, MAX(l1) AS l1, MAX(l2) AS l2, MAX(l3) AS l3, MAX(l4) AS l4, MAX(l5) AS l5, MAX(l6) AS l6, MAX(l7) AS l7, MAX(l8) AS l8, MAX(l9) AS l9, MAX(l10) AS l10, MAX(l_inf) AS l_inf
FROM perValue
HAVING COUNT(*) > 0  -- If you need to ensure some rows are returned
;

DROP TABLE IF EXISTS Degree_TITLE_PRODUCTION_YEAR_ID;

-- Create aggregator table Degree_TITLE_PRODUCTION_YEAR_ID for MCV TITLE.PRODUCTION_YEAR -- MCV
CREATE TEMP TABLE Degree_TITLE_PRODUCTION_YEAR_ID AS
SELECT
    mcvt.mcv_id AS mcv_id,
    TITLE.ID AS ID,
    COUNT(*) AS deg
FROM TITLE
JOIN TITLE_PRODUCTION_YEAR_mcv mcvt ON TITLE.PRODUCTION_YEAR = mcvt.PRODUCTION_YEAR
WHERE TITLE.ID IS NOT NULL
GROUP BY
    mcvt.mcv_id, TITLE.ID
;

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    fk_join_var_name,
    pk_relation_name,
    pk_join_var_name,
    
  pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'TITLE' AS relation_name,
    'ID' AS join_var_name,
    'MCV' AS aggregator_name,
    'PRODUCTION_YEAR' AS pred_col_name,
    NULL AS fk_join_var_name,
    NULL AS pk_relation_name,
    NULL AS pk_join_var_name,
    
    mcv_id AS pred_value,
    COUNT(*) AS l0,
    SUM(POWER(deg, 1)) AS l1,
    SUM(POWER(deg, 2)) AS l2,
    SUM(POWER(deg, 3)) AS l3,
    SUM(POWER(deg, 4)) AS l4,
    SUM(POWER(deg, 5)) AS l5,
    SUM(POWER(deg, 6)) AS l6,
    SUM(POWER(deg, 7)) AS l7,
    SUM(POWER(deg, 8)) AS l8,
    SUM(POWER(deg, 9)) AS l9,
    SUM(POWER(deg, 10)) AS l10,
    MAX(deg) AS l_inf
FROM Degree_TITLE_PRODUCTION_YEAR_ID
GROUP BY mcv_id
;

DROP TABLE IF EXISTS Degree_TITLE_PRODUCTION_YEAR_ID;

-- Create aggregator table Degree_TITLE_PRODUCTION_YEAR_ID for NONMCV TITLE.PRODUCTION_YEAR -- MCV
CREATE TEMP TABLE Degree_TITLE_PRODUCTION_YEAR_ID AS
SELECT
    TITLE.PRODUCTION_YEAR AS PRODUCTION_YEAR,
    TITLE.ID AS ID,
    COUNT(*) AS deg
FROM TITLE
LEFT JOIN TITLE_PRODUCTION_YEAR_mcv mcvt ON TITLE.PRODUCTION_YEAR = mcvt.PRODUCTION_YEAR
WHERE TITLE.ID IS NOT NULL AND mcvt.mcv_id IS NULL 
GROUP BY
    TITLE.PRODUCTION_YEAR, TITLE.ID
;

-- Insert into norms for Non-MCV TITLE PRODUCTION_YEAR ID NONMCV
-- Among all non-MCV values in Degree_TITLE_PRODUCTION_YEAR_ID, find max of each Lp
WITH perValue AS (
  SELECT
    PRODUCTION_YEAR,
    COUNT(*) AS l0, SUM(POWER(deg, 1)) AS l1, SUM(POWER(deg, 2)) AS l2, SUM(POWER(deg, 3)) AS l3, SUM(POWER(deg, 4)) AS l4, SUM(POWER(deg, 5)) AS l5, SUM(POWER(deg, 6)) AS l6, SUM(POWER(deg, 7)) AS l7, SUM(POWER(deg, 8)) AS l8, SUM(POWER(deg, 9)) AS l9, SUM(POWER(deg, 10)) AS l10, MAX(deg) AS l_inf
  FROM Degree_TITLE_PRODUCTION_YEAR_ID
  GROUP BY PRODUCTION_YEAR
  HAVING PRODUCTION_YEAR IS NOT NULL
)
INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    fk_join_var_name,
    pk_relation_name,
    pk_join_var_name,
    
  pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'TITLE' AS relation_name,
    'ID' AS join_var_name,
    'NONMCV' AS aggregator_name,
    'PRODUCTION_YEAR' AS pred_col_name,
    NULL AS fk_join_var_name,
    NULL AS pk_relation_name,
    NULL AS pk_join_var_name,
    
  NULL              AS pred_value_id,
  MAX(l0) AS l0, MAX(l1) AS l1, MAX(l2) AS l2, MAX(l3) AS l3, MAX(l4) AS l4, MAX(l5) AS l5, MAX(l6) AS l6, MAX(l7) AS l7, MAX(l8) AS l8, MAX(l9) AS l9, MAX(l10) AS l10, MAX(l_inf) AS l_inf
FROM perValue
HAVING COUNT(*) > 0  -- If you need to ensure some rows are returned
;

DROP TABLE IF EXISTS Degree_TITLE_PRODUCTION_YEAR_ID;

-- Create aggregator table Degree_TITLE_PRODUCTION_YEAR_ID for Histogram TITLE.PRODUCTION_YEAR -- RANGE
CREATE TEMP TABLE Degree_TITLE_PRODUCTION_YEAR_ID AS
SELECT
    TITLE.PRODUCTION_YEAR AS PRODUCTION_YEAR,
    TITLE.ID AS ID,
    COUNT(*) AS deg
FROM TITLE

WHERE TITLE.ID IS NOT NULL
GROUP BY
    TITLE.PRODUCTION_YEAR, TITLE.ID
;

INSERT INTO norms(

    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    fk_join_var_name,
    pk_relation_name,
    pk_join_var_name,
    
pred_value_id,
l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'TITLE' AS relation_name,
    'ID' AS join_var_name,
    'RANGE' AS aggregator_name,
    'PRODUCTION_YEAR' AS pred_col_name,
    NULL AS fk_join_var_name,
    NULL AS pk_relation_name,
    NULL AS pk_join_var_name,
    
    bucket_id     AS pred_value,
    COUNT(*) AS l0,
    SUM(POWER(deg, 1)) AS l1,
    SUM(POWER(deg, 2)) AS l2,
    SUM(POWER(deg, 3)) AS l3,
    SUM(POWER(deg, 4)) AS l4,
    SUM(POWER(deg, 5)) AS l5,
    SUM(POWER(deg, 6)) AS l6,
    SUM(POWER(deg, 7)) AS l7,
    SUM(POWER(deg, 8)) AS l8,
    SUM(POWER(deg, 9)) AS l9,
    SUM(POWER(deg, 10)) AS l10,
    MAX(deg) AS l_inf
FROM 
(
  SELECT
        ID, bucket_id, SUM(deg) as deg
        FROM Degree_TITLE_PRODUCTION_YEAR_ID d
        JOIN TITLE_PRODUCTION_YEAR_histogram h
        ON (h.lower_bound IS NULL OR d.PRODUCTION_YEAR >= h.lower_bound)
        AND (h.upper_bound IS NULL OR d.PRODUCTION_YEAR <= h.upper_bound)
    WHERE ID IS NOT NULL AND bucket_id IS NOT NULL
    GROUP BY ID, bucket_id
)
GROUP BY bucket_id
ORDER BY bucket_id
;

----------------------------------
-- START: generate all lpnorm queries for MOVIE_INFO
----------------------------------


DROP TABLE IF EXISTS MOVIE_INFO_MOVIE_ID_ID_TITLE;

-- Create temp table MOVIE_INFO_MOVIE_ID_ID_TITLE (FK->PK join)
CREATE TEMP TABLE MOVIE_INFO_MOVIE_ID_ID_TITLE AS
SELECT
    MOVIE_INFO.MOVIE_ID AS MOVIE_ID,
    TITLE.PRODUCTION_YEAR AS TITLE_PRODUCTION_YEAR, TITLE.KIND_ID AS TITLE_KIND_ID
FROM MOVIE_INFO
JOIN TITLE
  ON MOVIE_INFO.MOVIE_ID = TITLE.ID
;

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

DROP TABLE IF EXISTS Degree_MOVIE_INFO_INFO_TYPE_ID_MOVIE_ID;

-- Create aggregator table Degree_MOVIE_INFO_INFO_TYPE_ID_MOVIE_ID for MCV MOVIE_INFO.INFO_TYPE_ID -- MCV
CREATE TEMP TABLE Degree_MOVIE_INFO_INFO_TYPE_ID_MOVIE_ID AS
SELECT
    mcvt.mcv_id AS mcv_id,
    MOVIE_INFO.MOVIE_ID AS MOVIE_ID,
    COUNT(*) AS deg
FROM MOVIE_INFO
JOIN MOVIE_INFO_INFO_TYPE_ID_mcv mcvt ON MOVIE_INFO.INFO_TYPE_ID = mcvt.INFO_TYPE_ID
WHERE MOVIE_INFO.MOVIE_ID IS NOT NULL
GROUP BY
    mcvt.mcv_id, MOVIE_INFO.MOVIE_ID
;

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    fk_join_var_name,
    pk_relation_name,
    pk_join_var_name,
    
  pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_INFO' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'MCV' AS aggregator_name,
    'INFO_TYPE_ID' AS pred_col_name,
    NULL AS fk_join_var_name,
    NULL AS pk_relation_name,
    NULL AS pk_join_var_name,
    
    mcv_id AS pred_value,
    COUNT(*) AS l0,
    SUM(POWER(deg, 1)) AS l1,
    SUM(POWER(deg, 2)) AS l2,
    SUM(POWER(deg, 3)) AS l3,
    SUM(POWER(deg, 4)) AS l4,
    SUM(POWER(deg, 5)) AS l5,
    SUM(POWER(deg, 6)) AS l6,
    SUM(POWER(deg, 7)) AS l7,
    SUM(POWER(deg, 8)) AS l8,
    SUM(POWER(deg, 9)) AS l9,
    SUM(POWER(deg, 10)) AS l10,
    MAX(deg) AS l_inf
FROM Degree_MOVIE_INFO_INFO_TYPE_ID_MOVIE_ID
GROUP BY mcv_id
;

DROP TABLE IF EXISTS Degree_MOVIE_INFO_INFO_TYPE_ID_MOVIE_ID;

-- Create aggregator table Degree_MOVIE_INFO_INFO_TYPE_ID_MOVIE_ID for NONMCV MOVIE_INFO.INFO_TYPE_ID -- MCV
CREATE TEMP TABLE Degree_MOVIE_INFO_INFO_TYPE_ID_MOVIE_ID AS
SELECT
    MOVIE_INFO.INFO_TYPE_ID AS INFO_TYPE_ID,
    MOVIE_INFO.MOVIE_ID AS MOVIE_ID,
    COUNT(*) AS deg
FROM MOVIE_INFO
LEFT JOIN MOVIE_INFO_INFO_TYPE_ID_mcv mcvt ON MOVIE_INFO.INFO_TYPE_ID = mcvt.INFO_TYPE_ID
WHERE MOVIE_INFO.MOVIE_ID IS NOT NULL AND mcvt.mcv_id IS NULL 
GROUP BY
    MOVIE_INFO.INFO_TYPE_ID, MOVIE_INFO.MOVIE_ID
;

-- Insert into norms for Non-MCV MOVIE_INFO INFO_TYPE_ID MOVIE_ID NONMCV
-- Among all non-MCV values in Degree_MOVIE_INFO_INFO_TYPE_ID_MOVIE_ID, find max of each Lp
WITH perValue AS (
  SELECT
    INFO_TYPE_ID,
    COUNT(*) AS l0, SUM(POWER(deg, 1)) AS l1, SUM(POWER(deg, 2)) AS l2, SUM(POWER(deg, 3)) AS l3, SUM(POWER(deg, 4)) AS l4, SUM(POWER(deg, 5)) AS l5, SUM(POWER(deg, 6)) AS l6, SUM(POWER(deg, 7)) AS l7, SUM(POWER(deg, 8)) AS l8, SUM(POWER(deg, 9)) AS l9, SUM(POWER(deg, 10)) AS l10, MAX(deg) AS l_inf
  FROM Degree_MOVIE_INFO_INFO_TYPE_ID_MOVIE_ID
  GROUP BY INFO_TYPE_ID
  HAVING INFO_TYPE_ID IS NOT NULL
)
INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    fk_join_var_name,
    pk_relation_name,
    pk_join_var_name,
    
  pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_INFO' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NONMCV' AS aggregator_name,
    'INFO_TYPE_ID' AS pred_col_name,
    NULL AS fk_join_var_name,
    NULL AS pk_relation_name,
    NULL AS pk_join_var_name,
    
  NULL              AS pred_value_id,
  MAX(l0) AS l0, MAX(l1) AS l1, MAX(l2) AS l2, MAX(l3) AS l3, MAX(l4) AS l4, MAX(l5) AS l5, MAX(l6) AS l6, MAX(l7) AS l7, MAX(l8) AS l8, MAX(l9) AS l9, MAX(l10) AS l10, MAX(l_inf) AS l_inf
FROM perValue
HAVING COUNT(*) > 0  -- If you need to ensure some rows are returned
;

DROP TABLE IF EXISTS Degree_MOVIE_INFO_MOVIE_ID_ID_TITLE_KIND_ID_MOVIE_ID;

-- Create aggregator table Degree_MOVIE_INFO_MOVIE_ID_ID_TITLE_KIND_ID_MOVIE_ID for MCV MOVIE_INFO_MOVIE_ID_ID_TITLE.TITLE_KIND_ID -- FKPK_MCV
CREATE TEMP TABLE Degree_MOVIE_INFO_MOVIE_ID_ID_TITLE_KIND_ID_MOVIE_ID AS
SELECT
    mcvt.mcv_id AS mcv_id,
    MOVIE_INFO_MOVIE_ID_ID_TITLE.MOVIE_ID AS MOVIE_ID,
    COUNT(*) AS deg
FROM MOVIE_INFO_MOVIE_ID_ID_TITLE
JOIN TITLE_KIND_ID_mcv mcvt ON MOVIE_INFO_MOVIE_ID_ID_TITLE.TITLE_KIND_ID = mcvt.KIND_ID
WHERE MOVIE_INFO_MOVIE_ID_ID_TITLE.MOVIE_ID IS NOT NULL
GROUP BY
    mcvt.mcv_id, MOVIE_INFO_MOVIE_ID_ID_TITLE.MOVIE_ID
;

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    fk_join_var_name,
    pk_relation_name,
    pk_join_var_name,
    
  pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_INFO' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'MCV' AS aggregator_name,
    'KIND_ID' AS pred_col_name,
    'MOVIE_ID' AS fk_join_var_name,
    'TITLE' AS pk_relation_name,
    'ID' AS pk_join_var_name,
    
    mcv_id AS pred_value,
    COUNT(*) AS l0,
    SUM(POWER(deg, 1)) AS l1,
    SUM(POWER(deg, 2)) AS l2,
    SUM(POWER(deg, 3)) AS l3,
    SUM(POWER(deg, 4)) AS l4,
    SUM(POWER(deg, 5)) AS l5,
    SUM(POWER(deg, 6)) AS l6,
    SUM(POWER(deg, 7)) AS l7,
    SUM(POWER(deg, 8)) AS l8,
    SUM(POWER(deg, 9)) AS l9,
    SUM(POWER(deg, 10)) AS l10,
    MAX(deg) AS l_inf
FROM Degree_MOVIE_INFO_MOVIE_ID_ID_TITLE_KIND_ID_MOVIE_ID
GROUP BY mcv_id
;

DROP TABLE IF EXISTS Degree_MOVIE_INFO_MOVIE_ID_ID_TITLE_KIND_ID_MOVIE_ID;

-- Create aggregator table Degree_MOVIE_INFO_MOVIE_ID_ID_TITLE_KIND_ID_MOVIE_ID for NONMCV MOVIE_INFO_MOVIE_ID_ID_TITLE.TITLE_KIND_ID -- FKPK_MCV
CREATE TEMP TABLE Degree_MOVIE_INFO_MOVIE_ID_ID_TITLE_KIND_ID_MOVIE_ID AS
SELECT
    MOVIE_INFO_MOVIE_ID_ID_TITLE.TITLE_KIND_ID AS KIND_ID,
    MOVIE_INFO_MOVIE_ID_ID_TITLE.MOVIE_ID AS MOVIE_ID,
    COUNT(*) AS deg
FROM MOVIE_INFO_MOVIE_ID_ID_TITLE
LEFT JOIN TITLE_KIND_ID_mcv mcvt ON MOVIE_INFO_MOVIE_ID_ID_TITLE.TITLE_KIND_ID = mcvt.KIND_ID
WHERE MOVIE_INFO_MOVIE_ID_ID_TITLE.MOVIE_ID IS NOT NULL AND mcvt.mcv_id IS NULL 
GROUP BY
    MOVIE_INFO_MOVIE_ID_ID_TITLE.TITLE_KIND_ID, MOVIE_INFO_MOVIE_ID_ID_TITLE.MOVIE_ID
;

-- Insert into norms for Non-MCV MOVIE_INFO_MOVIE_ID_ID_TITLE KIND_ID MOVIE_ID NONMCV
-- Among all non-MCV values in Degree_MOVIE_INFO_MOVIE_ID_ID_TITLE_KIND_ID_MOVIE_ID, find max of each Lp
WITH perValue AS (
  SELECT
    KIND_ID,
    COUNT(*) AS l0, SUM(POWER(deg, 1)) AS l1, SUM(POWER(deg, 2)) AS l2, SUM(POWER(deg, 3)) AS l3, SUM(POWER(deg, 4)) AS l4, SUM(POWER(deg, 5)) AS l5, SUM(POWER(deg, 6)) AS l6, SUM(POWER(deg, 7)) AS l7, SUM(POWER(deg, 8)) AS l8, SUM(POWER(deg, 9)) AS l9, SUM(POWER(deg, 10)) AS l10, MAX(deg) AS l_inf
  FROM Degree_MOVIE_INFO_MOVIE_ID_ID_TITLE_KIND_ID_MOVIE_ID
  GROUP BY KIND_ID
  HAVING KIND_ID IS NOT NULL
)
INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    fk_join_var_name,
    pk_relation_name,
    pk_join_var_name,
    
  pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_INFO' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NONMCV' AS aggregator_name,
    'KIND_ID' AS pred_col_name,
    'MOVIE_ID' AS fk_join_var_name,
    'TITLE' AS pk_relation_name,
    'ID' AS pk_join_var_name,
    
  NULL              AS pred_value_id,
  MAX(l0) AS l0, MAX(l1) AS l1, MAX(l2) AS l2, MAX(l3) AS l3, MAX(l4) AS l4, MAX(l5) AS l5, MAX(l6) AS l6, MAX(l7) AS l7, MAX(l8) AS l8, MAX(l9) AS l9, MAX(l10) AS l10, MAX(l_inf) AS l_inf
FROM perValue
HAVING COUNT(*) > 0  -- If you need to ensure some rows are returned
;

DROP TABLE IF EXISTS Degree_MOVIE_INFO_MOVIE_ID_ID_TITLE_PRODUCTION_YEAR_MOVIE_ID;

-- Create aggregator table Degree_MOVIE_INFO_MOVIE_ID_ID_TITLE_PRODUCTION_YEAR_MOVIE_ID for MCV MOVIE_INFO_MOVIE_ID_ID_TITLE.TITLE_PRODUCTION_YEAR -- FKPK_MCV
CREATE TEMP TABLE Degree_MOVIE_INFO_MOVIE_ID_ID_TITLE_PRODUCTION_YEAR_MOVIE_ID AS
SELECT
    mcvt.mcv_id AS mcv_id,
    MOVIE_INFO_MOVIE_ID_ID_TITLE.MOVIE_ID AS MOVIE_ID,
    COUNT(*) AS deg
FROM MOVIE_INFO_MOVIE_ID_ID_TITLE
JOIN TITLE_PRODUCTION_YEAR_mcv mcvt ON MOVIE_INFO_MOVIE_ID_ID_TITLE.TITLE_PRODUCTION_YEAR = mcvt.PRODUCTION_YEAR
WHERE MOVIE_INFO_MOVIE_ID_ID_TITLE.MOVIE_ID IS NOT NULL
GROUP BY
    mcvt.mcv_id, MOVIE_INFO_MOVIE_ID_ID_TITLE.MOVIE_ID
;

INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    fk_join_var_name,
    pk_relation_name,
    pk_join_var_name,
    
  pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_INFO' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'MCV' AS aggregator_name,
    'PRODUCTION_YEAR' AS pred_col_name,
    'MOVIE_ID' AS fk_join_var_name,
    'TITLE' AS pk_relation_name,
    'ID' AS pk_join_var_name,
    
    mcv_id AS pred_value,
    COUNT(*) AS l0,
    SUM(POWER(deg, 1)) AS l1,
    SUM(POWER(deg, 2)) AS l2,
    SUM(POWER(deg, 3)) AS l3,
    SUM(POWER(deg, 4)) AS l4,
    SUM(POWER(deg, 5)) AS l5,
    SUM(POWER(deg, 6)) AS l6,
    SUM(POWER(deg, 7)) AS l7,
    SUM(POWER(deg, 8)) AS l8,
    SUM(POWER(deg, 9)) AS l9,
    SUM(POWER(deg, 10)) AS l10,
    MAX(deg) AS l_inf
FROM Degree_MOVIE_INFO_MOVIE_ID_ID_TITLE_PRODUCTION_YEAR_MOVIE_ID
GROUP BY mcv_id
;

DROP TABLE IF EXISTS Degree_MOVIE_INFO_MOVIE_ID_ID_TITLE_PRODUCTION_YEAR_MOVIE_ID;

-- Create aggregator table Degree_MOVIE_INFO_MOVIE_ID_ID_TITLE_PRODUCTION_YEAR_MOVIE_ID for NONMCV MOVIE_INFO_MOVIE_ID_ID_TITLE.TITLE_PRODUCTION_YEAR -- FKPK_MCV
CREATE TEMP TABLE Degree_MOVIE_INFO_MOVIE_ID_ID_TITLE_PRODUCTION_YEAR_MOVIE_ID AS
SELECT
    MOVIE_INFO_MOVIE_ID_ID_TITLE.TITLE_PRODUCTION_YEAR AS PRODUCTION_YEAR,
    MOVIE_INFO_MOVIE_ID_ID_TITLE.MOVIE_ID AS MOVIE_ID,
    COUNT(*) AS deg
FROM MOVIE_INFO_MOVIE_ID_ID_TITLE
LEFT JOIN TITLE_PRODUCTION_YEAR_mcv mcvt ON MOVIE_INFO_MOVIE_ID_ID_TITLE.TITLE_PRODUCTION_YEAR = mcvt.PRODUCTION_YEAR
WHERE MOVIE_INFO_MOVIE_ID_ID_TITLE.MOVIE_ID IS NOT NULL AND mcvt.mcv_id IS NULL 
GROUP BY
    MOVIE_INFO_MOVIE_ID_ID_TITLE.TITLE_PRODUCTION_YEAR, MOVIE_INFO_MOVIE_ID_ID_TITLE.MOVIE_ID
;

-- Insert into norms for Non-MCV MOVIE_INFO_MOVIE_ID_ID_TITLE PRODUCTION_YEAR MOVIE_ID NONMCV
-- Among all non-MCV values in Degree_MOVIE_INFO_MOVIE_ID_ID_TITLE_PRODUCTION_YEAR_MOVIE_ID, find max of each Lp
WITH perValue AS (
  SELECT
    PRODUCTION_YEAR,
    COUNT(*) AS l0, SUM(POWER(deg, 1)) AS l1, SUM(POWER(deg, 2)) AS l2, SUM(POWER(deg, 3)) AS l3, SUM(POWER(deg, 4)) AS l4, SUM(POWER(deg, 5)) AS l5, SUM(POWER(deg, 6)) AS l6, SUM(POWER(deg, 7)) AS l7, SUM(POWER(deg, 8)) AS l8, SUM(POWER(deg, 9)) AS l9, SUM(POWER(deg, 10)) AS l10, MAX(deg) AS l_inf
  FROM Degree_MOVIE_INFO_MOVIE_ID_ID_TITLE_PRODUCTION_YEAR_MOVIE_ID
  GROUP BY PRODUCTION_YEAR
  HAVING PRODUCTION_YEAR IS NOT NULL
)
INSERT INTO norms(
    
    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    fk_join_var_name,
    pk_relation_name,
    pk_join_var_name,
    
  pred_value_id,
  l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_INFO' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'NONMCV' AS aggregator_name,
    'PRODUCTION_YEAR' AS pred_col_name,
    'MOVIE_ID' AS fk_join_var_name,
    'TITLE' AS pk_relation_name,
    'ID' AS pk_join_var_name,
    
  NULL              AS pred_value_id,
  MAX(l0) AS l0, MAX(l1) AS l1, MAX(l2) AS l2, MAX(l3) AS l3, MAX(l4) AS l4, MAX(l5) AS l5, MAX(l6) AS l6, MAX(l7) AS l7, MAX(l8) AS l8, MAX(l9) AS l9, MAX(l10) AS l10, MAX(l_inf) AS l_inf
FROM perValue
HAVING COUNT(*) > 0  -- If you need to ensure some rows are returned
;

DROP TABLE IF EXISTS Degree_MOVIE_INFO_MOVIE_ID_ID_TITLE_PRODUCTION_YEAR_MOVIE_ID;

-- Create aggregator table Degree_MOVIE_INFO_MOVIE_ID_ID_TITLE_PRODUCTION_YEAR_MOVIE_ID for Histogram MOVIE_INFO_MOVIE_ID_ID_TITLE.TITLE_PRODUCTION_YEAR -- FKPK_RANGE
CREATE TEMP TABLE Degree_MOVIE_INFO_MOVIE_ID_ID_TITLE_PRODUCTION_YEAR_MOVIE_ID AS
SELECT
    MOVIE_INFO_MOVIE_ID_ID_TITLE.TITLE_PRODUCTION_YEAR AS PRODUCTION_YEAR,
    MOVIE_INFO_MOVIE_ID_ID_TITLE.MOVIE_ID AS MOVIE_ID,
    COUNT(*) AS deg
FROM MOVIE_INFO_MOVIE_ID_ID_TITLE

WHERE MOVIE_INFO_MOVIE_ID_ID_TITLE.MOVIE_ID IS NOT NULL
GROUP BY
    MOVIE_INFO_MOVIE_ID_ID_TITLE.TITLE_PRODUCTION_YEAR, MOVIE_INFO_MOVIE_ID_ID_TITLE.MOVIE_ID
;

INSERT INTO norms(

    relation_name,
    join_var_name,
    aggregator_name,
    pred_col_name,
    fk_join_var_name,
    pk_relation_name,
    pk_join_var_name,
    
pred_value_id,
l0, l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l_inf
)
SELECT
    
    'MOVIE_INFO' AS relation_name,
    'MOVIE_ID' AS join_var_name,
    'RANGE' AS aggregator_name,
    'PRODUCTION_YEAR' AS pred_col_name,
    'MOVIE_ID' AS fk_join_var_name,
    'TITLE' AS pk_relation_name,
    'ID' AS pk_join_var_name,
    
    bucket_id     AS pred_value,
    COUNT(*) AS l0,
    SUM(POWER(deg, 1)) AS l1,
    SUM(POWER(deg, 2)) AS l2,
    SUM(POWER(deg, 3)) AS l3,
    SUM(POWER(deg, 4)) AS l4,
    SUM(POWER(deg, 5)) AS l5,
    SUM(POWER(deg, 6)) AS l6,
    SUM(POWER(deg, 7)) AS l7,
    SUM(POWER(deg, 8)) AS l8,
    SUM(POWER(deg, 9)) AS l9,
    SUM(POWER(deg, 10)) AS l10,
    MAX(deg) AS l_inf
FROM 
(
  SELECT
        MOVIE_ID, bucket_id, SUM(deg) as deg
        FROM Degree_MOVIE_INFO_MOVIE_ID_ID_TITLE_PRODUCTION_YEAR_MOVIE_ID d
        JOIN TITLE_PRODUCTION_YEAR_histogram h
        ON (h.lower_bound IS NULL OR d.PRODUCTION_YEAR >= h.lower_bound)
        AND (h.upper_bound IS NULL OR d.PRODUCTION_YEAR <= h.upper_bound)
    WHERE MOVIE_ID IS NOT NULL AND bucket_id IS NOT NULL
    GROUP BY MOVIE_ID, bucket_id
)
GROUP BY bucket_id
ORDER BY bucket_id
;

