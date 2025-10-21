#Validation of Data - Identifying and removing duplicates.

#Table without duplicates
create table all_companies_joined_data.valid_comp_data as
	SELECT 
			key_id,
			Name,
			Symbol,
			country,
			Industry,
			employees_count,
			`price (USD)`,
			revenue_ttm,
			earnings_ttm
	FROM(
		SELECT *, DENSE_RANK() OVER(ORDER BY Name,earnings_ttm ASC) + 200 AS key_id ,
        ROW_NUMBER() OVER(PARTITION BY name) AS row_num
		FROM all_companies_joined_data.combined_comp_data
	) rowed WHERE rowed.row_num < 2;
    
select * from all_companies_joined_data.valid_comp_data
;


#DIMENSION TABLE: COMPANY NAME WITH NAME_ID AS THE PRIMARY KEY
CREATE TABLE all_companies_joined_data.d_comp_name_data AS
	SELECT	name_id,
			key_id,
			name,
			symbol
	FROM (SELECT 
				key_id,
				CONCAT(LEFT(symbol,3) , (ROW_NUMBER() OVER(ORDER BY name ASC)+ 100)) AS name_id, #Primary Key
				name,
				symbol
			FROM all_companies_joined_data.valid_comp_data) AS sub;
            
            
select * from all_companies_joined_data.d_comp_name_data;






SELECT DISTINCT country FROM all_companies_joined_data.valid_comp_data;

#DIMENSION TABLE: COUNTRY AND CONTINENTS DATA
CREATE TABLE all_companies_joined_data.location_comp_data AS
	SELECT
	country,
	continent,
	CONCAT( UPPER(LEFT(country,3)), (ROW_NUMBER() OVER(ORDER BY country ASC) + 100)) AS loc_id
	FROM (SELECT 
				DISTINCT vcd.country,
					cond.continent
			FROM all_companies_joined_data.valid_comp_data vcd
			INNER JOIN all_companies_joined_data.continent_data cond
			USING(country)) AS contvcd_data;


SELECT * FROM all_companies_joined_data.cont_loc_data;

#DIMENSION TABLE: INDUSTRY

CREATE TABLE all_companies_joined_data.d_ind_type AS
	SELECT  		
		industry,
		CONCAT(UPPER(LEFT(industry,4)), (ROW_NUMBER() OVER(ORDER BY industry ASC) + 200)) AS ind_id
	FROM (SELECT DISTINCT industry FROM all_companies_joined_data.valid_comp_data) AS ind_tab;


#DIMENSION TABLE: CONTINENT
CREATE TABLE all_companies_joined_data.cont_loc_data AS 
	SELECT 	
			continent,	
			UPPER(LEFT(continent, 3)) AS con_id
	FROM (SELECT DISTINCT continent FROM all_companies_joined_data.continent_data) AS cont_sub;


#FACT TABLE: COMPRISE OF loc_id, name_id, earnings, revenues, employees_count, prices (usd)

CREATE TABLE all_companies_joined_data.f_fincomp_data AS
	SELECT
		cld.con_id,
		lcd.loc_id,
		cnd.name_id,
        dit.ind_id,
		vcd.key_id,
		vcd.earnings_ttm,
		vcd.revenue_ttm,
		vcd.employees_count,
		vcd.`price (USD)` AS price_usd
	FROM all_companies_joined_data.valid_comp_data vcd
	LEFT JOIN all_companies_joined_data.location_comp_data lcd
	ON vcd.country = lcd.country
	LEFT JOIN all_companies_joined_data.d_comp_name_data cnd
		USING(NAME)
	LEFT JOIN all_companies_joined_data.cont_loc_data cld
		ON lcd.continent = cld.continent
	LEFT JOIN all_companies_joined_data.d_ind_type dit
		ON vcd.industry = dit.industry
	;
    
SELECT * FROM all_companies_joined_data.f_fincomp_data;
select * from all_companies_joined_data.d_ind_type;


#DEFINING PRIMARY AND FOREIGN KEYS

#PRIMARY KEYS-------------------------------------------------
ALTER TABLE all_companies_joined_data.d_comp_name_data
ADD PRIMARY KEY (name_id);

ALTER TABLE all_companies_joined_data.location_comp_data
ADD PRIMARY KEY (loc_id);

ALTER TABLE all_companies_joined_data.cont_loc_data
ADD PRIMARY KEY (con_id);

ALTER TABLE all_companies_joined_data.d_ind_type
ADD PRIMARY KEY (ind_id);

#FOREIGN KEYS --------------------------------------------------
ALTER TABLE all_companies_joined_data.f_fincomp_data
ADD CONSTRAINT foreign_locid
FOREIGN KEY (loc_id)
REFERENCES all_companies_joined_data.location_comp_data(loc_id);

ALTER TABLE all_companies_joined_data.f_fincomp_data
ADD CONSTRAINT foreign_nameid
FOREIGN KEY (name_id)
REFERENCES all_companies_joined_data.d_comp_name_data(name_id);

ALTER TABLE all_companies_joined_data.f_fincomp_data
ADD CONSTRAINT foreign_conid
FOREIGN KEY (con_id)
REFERENCES all_companies_joined_data.cont_loc_data(con_id);


ALTER TABLE all_companies_joined_data.f_fincomp_data
ADD CONSTRAINT foreign_ind_id
FOREIGN KEY (ind_id)
REFERENCES all_companies_joined_data.d_ind_type(ind_id);


SELECT * FROM all_companies_joined_data.d_comp_name_data;





SELECT * FROM all_companies_joined_data.d_comp_name_data;
SELECT * FROM all_companies_joined_data.cont_loc_data;
SELECT * FROM all_companies_joined_data.f_fincomp_data;


SELECT 
	SUM(earnings_ttm) AS total_earnings
FROM all_companies_joined_data.f_fincomp_data
WHERE con_id = 'AFR';


CREATE TABLE all_companies_joined_data.rank_number AS
	SELECT 
		rank_number,
        name_id
	FROM 
		(SELECT 
			name_id,
			price_usd,
			DENSE_RANK() OVER(ORDER BY price_usd DESC) AS rank_number
		FROM all_companies_joined_data.f_fincomp_data) AS rank_no;
        
        
select * from all_companies_joined_data.rank_number;


			
		
        
    
    

