
create table t_cathy_markova_project_sql_primary_final as
SELECT
	cp.id,    
	cpay.payroll_year,
    cpib.name AS industry,
    cpay.value AS average_wages,
	cpc.code as food_code,
    cpc.name AS food_category,
    cpc.price_value,
    cpc.price_unit,
    cp.value AS food_price,    
    TO_CHAR(cp.date_from, 'YYYY-mm-dd') AS price_measured_from,
    TO_CHAR(cp.date_to, 'YYYY-mm-dd') AS price_measured_to
    FROM
    czechia_price AS cp
JOIN czechia_price_category AS cpc
    ON cp.category_code = cpc.code
JOIN czechia_payroll AS cpay
    ON date_part('year', cp.date_from) = cpay.payroll_year
    AND cpay.value_type_code = 5958
    AND cp.region_code IS NULL
JOIN czechia_payroll_industry_branch AS cpib
    ON cpay.industry_branch_code = cpib.code
WHERE
    cpay.value_type_code = 5958;

select payroll_year, AVG(average_wages)
from t_cathy_markova_project_sql_primary_final tppf
group by payroll_year ;


--- otázka č.1: Rostou v průběhu let mzdy ve všech odvětvích, nebo v někerých klesají?

WITH cte_wage_analysis AS ( -- analýza vývoje mezd
    SELECT
        payroll_year,
        industry,
        round(AVG(average_wages) :: numeric) AS average_wage,
        round(LAG(AVG(average_wages)) OVER (PARTITION BY industry 
            ORDER BY payroll_year
        ) :: numeric) AS previous_year_wages,
       round((AVG(average_wages) - LAG(AVG(average_wages)) OVER (
            PARTITION BY industry 
            ORDER BY payroll_year
        )) / LAG(AVG(average_wages)) OVER (
            PARTITION BY industry 
            ORDER BY payroll_year
        ) * 100, 3) AS wage_growth_percentage,
        CASE 
            WHEN (AVG(average_wages) - LAG(AVG(average_wages)) OVER (
                PARTITION BY industry 
                ORDER BY payroll_year
            )) > 0 THEN 'growth'
            ELSE 'decline'
        END AS trend
    FROM t_katerina_markova_project_sql_primary_final tppf
    GROUP BY payroll_year, industry
)
SELECT *
FROM cte_wage_analysis
WHERE previous_year_wages IS NOT NULL
ORDER BY industry, payroll_year, trend

;


--- otázka č. 2 - Kolik je možné si koupit litrů mléka a kilogramů chleba za první a poslední srovnatelné období v dostupných datech cen a mezd?
create or replace view v_weekly_food_prices as -- view pro týdenní vývoj cen, nejprve seskupíme data podle týdnů
SELECT
    food_category,
    price_measured_from,
    price_measured_to,
    ROUND(AVG(food_price) :: numeric, 2) AS weekly_avg_food_price
FROM t_cathy_markova_project_sql_primary_final tppf
WHERE food_code IN ('111301', '114201') -- Filtrujeme na chléb a mléko
GROUP BY price_measured_from, price_measured_to, food_category;

CREATE or replace VIEW v_yearly_food_prices as -- view pro roční průměry cen
SELECT
    food_category,
    EXTRACT(YEAR FROM price_measured_from::DATE) AS year,
    ROUND(AVG(weekly_avg_food_price) :: numeric, 2) AS avg_annual_food_price
FROM v_weekly_food_prices
GROUP BY year, food_category; --seskupíme data podle roku, aby se dal porovnat konkrétní rok s původním payroll_year

CREATE materialized VIEW v_food_price_changes as -- view pro meziroční vývoj cen
SELECT
    food_category,
    year,
    avg_annual_food_price,
    LAG(avg_annual_food_price) OVER (PARTITION BY food_category ORDER BY year) AS previous_avg_food_price,
    ROUND(
        ((avg_annual_food_price - LAG(avg_annual_food_price) OVER (PARTITION BY food_category ORDER BY year))
        / NULLIF(LAG(avg_annual_food_price) OVER (PARTITION BY food_category ORDER BY year), 0)) * 100, 2
    ) AS percentage_price_change
FROM v_yearly_food_prices;

WITH cte_first_last_years AS ( -- získání prvního a posledního roku
    SELECT 
        food_category,
        MIN(year) AS first_year, 
        MAX(year) AS last_year
    FROM v_yearly_food_prices
    GROUP BY food_category
)
SELECT
    yd.food_category,
    yd.year AS payroll_year,
    ROUND(AVG(tppf.average_wages) :: numeric, 2) AS avg_annual_wage,
    yd.avg_annual_food_price,
    FLOOR(AVG(tppf.average_wages) / yd.avg_annual_food_price) AS purchasable_quantity
FROM v_yearly_food_prices yd
JOIN cte_first_last_years fly
    ON yd.food_category = fly.food_category 
    AND (yd.year = fly.first_year OR yd.year = fly.last_year)
JOIN t_cathy_markova_project_sql_primary_final tppf
    ON CAST(yd.year AS INTEGER) = EXTRACT(YEAR FROM tppf.price_measured_from :: DATE)  
GROUP BY yd.food_category, yd.year, yd.avg_annual_food_price
order by payroll_year, food_category;


--- otázka č. 3: Která kategorie potravin zdražuje nejpomaleji (je u ní nejnižší percentuální meziroční růst)?
select *
from v_food_price_changes vfpc ;

create or replace view v_food_price_cross_analysis as
WITH cte_lowest_price_change AS ( -- analyzuje změny cen a najde rok s nejnižším meziročním růstem pro každou food category)
    SELECT
        food_category,
        year AS year_with_lowest_increase,
        percentage_price_change,
        ROW_NUMBER() OVER (PARTITION BY food_category ORDER BY percentage_price_change ASC, year ASC) AS rn
    FROM v_food_price_changes
    WHERE percentage_price_change IS NOT NULL
),
cte_avg_price_growth AS ( -- vypočítá průměrný meziroční růst cen pro kažou food category
    SELECT
        food_category,
        COUNT(percentage_price_change) AS num_years,  -- Počet let s dostupnými daty
        AVG(percentage_price_change) AS avg_price_index
    FROM v_food_price_changes
    WHERE percentage_price_change IS NOT NULL AND percentage_price_change > 0
    GROUP BY food_category
)
SELECT 
    apg.food_category,
    apg.num_years,   -- Počet let, kde se měřila cena
    ROUND(apg.avg_price_index, 2) as avg_price_index,  -- Průměrná meziroční změna ceny
    lpc.year_with_lowest_increase, -- Rok s nejnižší meziroční změnou
    lpc.percentage_price_change AS lowest_percentage_price_change  -- Hodnota nejnižší meziroční změny
FROM cte_avg_price_growth apg
LEFT JOIN cte_lowest_price_change lpc
    ON apg.food_category = lpc.food_category AND lpc.rn = 1 -- Zajistí výběr pouze jednoho roku
ORDER BY apg.avg_price_index asc;

select *
from v_food_price_cross_analysis vfpca ;

select 
food_category,
lowest_percentage_price_change,
year_with_lowest_increase 
from v_food_price_cross_analysis vfpca 
order by lowest_percentage_price_change asc ;


--- Otázka č. 4: Existuje rok, ve kterém byl meziroční nárůst cen potravin výrazně vyšší než růst mezd (větší než 10%)?
DROP VIEW IF EXISTS v_wages_analysis_new CASCADE;

create or replace view v_wages_analysis_new as -- vytvoření view pro analýzu průměrných mezd bez rozlišení odvětví
WITH wage_analysis AS (
    SELECT
        payroll_year as year,
        round(AVG(average_wages)) AS average_wage,
        round(LAG(AVG(average_wages)) OVER (ORDER BY payroll_year)) AS previous_year_wages,
        ROUND((AVG(average_wages) - LAG(AVG(average_wages)) OVER (order by payroll_year))/ LAG(AVG(average_wages)) OVER (ORDER BY payroll_year
        ) *100, 2) as wage_growth_percentage,
        CASE 
            WHEN (AVG(average_wages) - LAG(AVG(average_wages)) OVER (
                ORDER BY payroll_year
            )) > 0 THEN 'growth'
            ELSE 'decline'
        END AS wage_trend
    FROM t_katerina_markova_project_sql_primary_final tkmpspf
    GROUP BY year
)
SELECT *
FROM wage_analysis
WHERE previous_year_wages IS NOT NULL
ORDER BY year, wage_trend;

select * from v_wages_analysis_new vwan;
where trend = 'growth';
select * from v_wages_analysis;
select * from v_prices_analysis vpa

select * from v_price_analysis_new vpan
order by year asc;
order by food_category asc, year;
select * from v_food_price_changes vfpc ;

DROP VIEW IF EXISTS v_price_analysis_new CASCADE;

create or replace view v_price_analysis_new as  -- view k posouzení vývoje průměrných cen bez rozlišení kategorie zboží 
select 
year,
round(AVG(actual_avg_food_price)) AS average_price,
        round(LAG(AVG(actual_avg_food_price)) OVER (ORDER BY year)) AS previous_year_price,
        ROUND((AVG(actual_avg_food_price) - LAG(AVG(actual_avg_food_price)) OVER (order by year))/ LAG(AVG(actual_avg_food_price)) OVER (ORDER BY year
        ) *100, 2) as price_growth_percentage,
        CASE 
            WHEN (AVG(actual_avg_food_price) - LAG(AVG(actual_avg_food_price)) OVER (
                ORDER BY year
            )) > 0 THEN 'growth'
            ELSE 'decline'
        END AS price_trend
from v_prices_analysis vpa
group by year
order by year;

select 
vpan.year as year,
vpan.price_growth_percentage,
vwan.wage_growth_percentage,
vpan.price_growth_percentage - vwan.wage_growth_percentage as price_wage_rel_difference
from v_price_analysis_new vpan 
join v_wages_analysis_new vwan
on vpan.year = vwan.year
where price_growth_percentage is not null 
and wage_growth_percentage is not null 
and price_growth_percentage  > 0
and wage_growth_percentage > 0
and vpan.price_growth_percentage > vwan.wage_growth_percentage
order by year;

--- Otázka č. 5:  Má výška HDP vliv na změny ve mzdách a cenách potravin?  Neboli, pokud HDP vzroste výrazněji v jednom roce,
--- projeví se to na cenách potravin či mzdách ve stejném nebo následujícím roce výraznějším růstem?

/*select 
country,
population-- country, population
from countries
where country = 'Czech Republic';


select 
country, year, gdp, population, gini  --country, year, gdp, population, gini
from economies
where country = 'Czech Republic'
and gdp is not null
and gini is not null
and year between '2006'and '2018'
order by year asc
;*/

drop table t_cathy_markova_project_sql_secondary_final ;

CREATE TABLE t_cathy_markova_project_SQL_secondary_final AS
SELECT 
    c.country,
    c.population,
    e.gdp,
    e.gini,
    e.year
FROM countries c
join economies e on c.country = e.country 
where c.continent = 'Europe'
    AND e.gini IS NOT NULL 
    AND e.gdp IS NOT NULL 
    AND e.year BETWEEN (
        SELECT CAST(MIN(date_part('year', price_measured_from :: date)) AS INTEGER)
        FROM t_cathy_markova_project_sql_primary_final
    ) 
    AND (
        SELECT CAST(MAX(date_part('year', price_measured_to :: date)) AS INTEGER)
        FROM t_cathy_markova_project_sql_primary_final
    )
ORDER BY e.year;

------------------------------------------------
/*select *
from t_cathy_markova_project_sql_secondary_final;

select *
from v_price_analysis_new vpa ;

select *
from v_wages_analysis_new vwan ;*/
------------------------------------------------- 

WITH cte_gdp AS ( -- varianta pro sledování změn za stejné období
    SELECT 
        tppf.year AS year,
        tppf.GDP AS actual_GDP,
        LAG(tppf.GDP :: numeric) OVER (ORDER BY tppf.year) AS previous_GDP,
        ROUND(
            (tppf.GDP :: numeric - LAG(tppf.GDP :: numeric) OVER (ORDER BY tppf.year)) / 
            NULLIF(LAG(tppf.GDP :: numeric ) OVER (ORDER BY tppf.year), 0) * 100, 
            2
        ) AS GDP_growth_percentage, 
        vpan.price_growth_percentage,
        vwan.wage_growth_percentage,
         CASE 
            WHEN (tppf.GDP - LAG(tppf.GDP) OVER (ORDER BY tppf.year)) > 0 
            THEN 'growth'
            ELSE 'decline'
        END AS GDP_trend,
        vpan.price_trend,
        vwan.wage_trend
    FROM t_cathy_markova_project_sql_secondary_final tppf
    left JOIN v_price_analysis_new vpan ON vpan.year = tppf.year
    left JOIN v_wages_analysis_new vwan ON vwan.year = tppf.year 
    where country = 'Czech Republic'
)
SELECT *
FROM cte_gdp
where gdp_growth_percentage is not null 
and price_growth_percentage is not null
and wage_growth_percentage is not null;


WITH cte_gdp_next AS (
    SELECT 
        tppf.year AS year,
        tppf.GDP AS actual_GDP,
        LAG(tppf.GDP :: numeric) OVER (ORDER BY tppf.year) AS previous_GDP,
        ROUND(
            (tppf.GDP :: numeric - LAG(tppf.GDP :: numeric) OVER (ORDER BY tppf.year)) / 
            NULLIF(LAG(tppf.GDP :: numeric) OVER (ORDER BY tppf.year), 0) * 100, 
            2
        ) AS GDP_growth_percentage, 
        vpan.price_growth_percentage,
        vwan.wage_growth_percentage,
        LEAD(vpan.price_growth_percentage) OVER (ORDER BY vpan.year) AS next_year_price_growth, -- Posun cenového růstu o 1 rok
        LEAD(vwan.wage_growth_percentage) OVER (ORDER BY vwan.year) AS next_year_wage_growth, -- Posun mzdového růstu o 1 rok
        vpan.price_trend,
        vwan.wage_trend,   
        CASE 
            WHEN (tppf.GDP - LAG(tppf.GDP) OVER (ORDER BY tppf.year)) > 0 
            THEN 'growth'
            ELSE 'decline'
        END AS GDP_trend,
        LEAD(vpan.price_trend) OVER (ORDER BY vpan.year) AS next_year_price_trend, -- Posun trendu růstu cen o 1 rok
        LEAD(vwan.wage_trend) OVER (ORDER BY vwan.year) AS next_year_wage_trend -- Posun trendu růstu mezd o 1 rok 
    FROM t_cathy_markova_project_sql_secondary_final tppf
    FULL OUTER JOIN v_price_analysis_new vpan ON vpan.year = tppf.year
    FULL OUTER JOIN v_wages_analysis_new vwan ON vwan.year = tppf.year 
    where country = 'Czech Republic'
)
SELECT *
FROM cte_gdp_next
WHERE GDP_growth_percentage IS NOT NULL 
and price_growth_percentage is not null
AND next_year_price_growth IS NOT NULL 
AND next_year_wage_growth IS NOT NULL;
