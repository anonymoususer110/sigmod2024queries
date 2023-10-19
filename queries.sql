--
-- SQL queries used in the experiments of UDF-OP paper.
--
-- The queries in this file follow MonetDB's SQL dialect. 
-- The same queries, with minor syntactic modifications, 
-- were used to run experiments in the other data engines 
-- reported in the paper (SQLLite, DuckDB, PostgreSQL, 
-- Greenplum, PySpark, dbX, etc.). These could also become 
-- available upon request. 
--


-- 
-- Q1 (get_population_stats_enhanced)
--
SELECT t.state_short, sum(t.crime_index) AS crime_index
FROM get_population_stats_table(
    (
        SELECT total_population, 
                total_adult_population, 
                number_of_robberies, 
                replace_o_a(slice(to_lower(strip(state_full)))) AS state_short
        FROM us_cities 
        WHERE total_population > 500000
    )
) AS t
GROUP BY t.state_short;


--
-- Q2 (zillow)
--
SELECT sum(bathrooms) AS sum_ba, 
        sum(sqft) AS sum_sqft, 
        count(url) AS urls, 
        count(offer) AS offers, 
        count(zip_code) AS zip_codes
FROM 
    (
        SELECT t.bedrooms, 
                extract_ba(t.facts_and_features) AS bathrooms, 
                extract_sqft(t.facts_and_features) AS sqft,
                extract_pcode(t.postal_code) AS zip_code, 
                replace_o_a(strip(to_lower(url))) AS url, 
                extract_offer(title) AS offer
        FROM (
                SELECT extract_bd(facts_and_features) AS bedrooms, 
                        extract_price(price) AS price_n, * 
                FROM zillow
            ) AS t
        WHERE t.bedrooms < 10 AND t.price_n > 100000 AND t.price_n < 20000000
    ) AS t
GROUP BY t.bedrooms;


--
-- Q3 (orders scalar fusion)
--
SELECT replace_o_a(slice(to_lower(strip(o_comment)))) 
FROM orders;


--
-- Q4 (orders scalar-aggregate fusion)
--
SELECT distinct_word_count(replace_o_a(slice(to_lower(strip(o_comment)))))
FROM orders 
GROUP BY o_shippriority;


--
-- Q5 (orders scalar-table fusion)
--
SELECT replace_o_a(slice(to_lower(strip(t.attr1)))),
        replace_o_a(slice(to_lower(strip(t.attr2))))
FROM create_table((SELECT o_comment FROM orders)) AS t;

--
-- Q6 (orders scalar-aggregate-table fusion)
--
SELECT 
    distinct_word_count(replace_o_a(slice(to_lower(strip(t.attr1)))), 
    distinct_word_count(replace_o_a(slice(to_lower(strip(t.attr2)))))
FROM create_table((SELECT o_comment FROM orders)) AS t
GROUP BY t.group_attr;


--
-- Q7 (split_arrays)
--
SELECT name, COUNT(*) 
FROM split_arrays_benchmark((SELECT name, vals FROM array_values)) 
WHERE vals between 1000 and 2000 
GROUP BY name ORDER BY name;


--
-- Q8 (words)
--
SELECT COUNT(*)
FROM contains_database_benchmark((SELECT words FROM words));


--
-- Q9 (get_population_stats)
--
SELECT t.state_short, 
        sum(t.crime_index) AS crime_index
FROM get_population_stats_table(
        (
            SELECT total_population, 
                   total_adult_population, 
                   number_of_robberies, 
                   slice(to_lower(state_full)) AS state_short
            FROM us_cities
            WHERE total_population > 500000
        )
) AS t
GROUP BY t.state_short;


--
-- Q10 (data_cleaning)
--
SELECT clean_incident_zips(incident_zip) 
FROM requests_311;


--
-- Q11 (small)
--
SELECT sum(extract_price(strip_dollar_sign(price)))
FROM
    (
        SELECT extract_bd(facts_and_features) AS bedrooms, price 
        FROM zillow
    ) AS T
WHERE T.bedrooms < 10
GROUP BY T.bedrooms;


-- 
-- Q12 (complex)
-- 
SELECT sum_int(to_int(strip_commas(strip_dollar_sign(price)))),
        sum_int(add_one(sub_one(bedrooms))),
        sum_int(add_one(sub_one(extract_ba(facts_and_features)))),
        sum_int(add_one(sub_one(extract_sqft(facts_and_features)))),
        count_str(strip(replace_o_a(to_lower(city)))),
        count_str(to_lower(strip(fix_url(url)))),
        count_str(to_lower(replace_o_a((extract_pcode(postal_code))))),
        count_str(to_lower(replace_o_a(extract_offer(title))))
FROM
    (
        SELECT extract_bd(facts_and_features) AS bedrooms, *
        FROM zillow
    ) AS T
WHERE T.bedrooms < 10
GROUP BY T.bedrooms;


-- 
-- Q13 
-- 
WITH pairs(pubid, pubdate, projectstart, projectend, 
           funder, class, projectid, authorpair) AS 
    (
        SELECT *
        FROM j2combinations_pypy((
            SELECT 
                pubid, pubdate, projectstart, projectend,
                extractpfunder_pypy(project) AS funder, 
                extractpclass_pypy(project) AS class,
                extractpid_pypy(project) AS projectid, 
                jsortallauthors_pypy(jsortvalue_pypy(jremoveshortwords_pypy(jlower_pypy(authors))))
            FROM 
                pubs
        )) AS xx
    )
SELECT funder, class, projectid,
  SUM(CASE WHEN cleandate_pypy(pubdate) between pstartcleaned and pendcleaned 
      THEN 1 ELSE NULL END) AS authors_during,
  SUM(CASE WHEN cleandate_pypy(pubdate) < pstartcleaned 
      THEN 1 ELSE NULL END) AS authors_before,
  SUM(CASE WHEN cleandate_pypy(pubdate) > pendcleaned 
      THEN 1 ELSE NULL END) AS authors_after
FROM (
    SELECT  projectpairs.funder, projectpairs.class, 
            projectpairs.projectid,
            cleandate_pypy(projectpairs.projectstart) AS pstartcleaned, 
            cleandate_pypy(projectpairs.projectend) AS pendcleaned, 
            pairs.authorpair, 
            pairs.pubdate   
    FROM (
          SELECT * FROM pairs 
          WHERE projectstart IS NOT NULL
        ) AS projectpairs, pairs 
    WHERE projectpairs.authorpair = pairs.authorpair
) AS xx
GROUP BY funder, class, projectid;


-- 
--Q14
-- 
SELECT charcount(lower(facts_and_features)) 
FROM zillow;


--
-- Q15
--
SELECT tokencount(tokens(facts_and_features)) 
FROM zillow;

