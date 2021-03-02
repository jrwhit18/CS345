-- James White, Jiusheng Zhang, Charles Tirrell
-- Assigment #3
-- 12/2/2021

-- Tom Zhang, Danial Shahidi, Daniel Grant 
-- 1. For each country, count the total number of deaths and the number of days
-- passed since the first day of the outbreak up to the day of the highest
-- stringency index before it decreased.
with stringency(max_stringency, location) as -- get the max stringency for ever country
	(select
		max(stringency_index), location
	from
		covid
	group by
		location)
select
	sum(new_deaths) as total_deaths, covid.location, count(new_deaths) as days_passed
from
	covid, stringency
where
	new_cases != 0 and -- only count from where the pandemic starts
	stringency_index = stringency.max_stringency and -- find the country specific stringncey
	covid.location = stringency.location -- make sure locations match
group by
	covid.location;

-- Tom Zhang, Danial Shahidi, Daniel Grant 
-- 2. For the countries that have vaccines, compare the average daily 
-- new cases of the days before the first vaccine dose administered 
-- and the days after that. Also, construct a 95% confidence interval for each.

-- as for the confidence interval part we have no idea what that is supposed to mean.


-- we stared off by constructing all the things we will need to compare
-- the cases before the vaccine and after 
with before(cases, location) as
(select 
	avg(new_cases), location
from
	covid
where
	total_vaccinations = 0 or -- if total vaccines is null or 0 it it before the vaccine for that country
	total_vaccinations is null
group by
	location),

after(cases, location) as (select
	avg(new_cases), location
from
	covid
where
	total_vaccinations != 0 -- if it is not 0 then it they do have the vaccine
group by
	location),

-- now we need ot know when the first vaccine was given for each country
first_day_vaccines(vacs, date, location) as 
(select
	min(total_vaccinations), date, location
from
	covid
where
	total_vaccinations != 0 or
	total_vaccinations is not null
group by
	date, location)

-- we construct the table where they are equal on location, this is why I used join using (location)
select
	before.location, before.cases as before, after.cases as after, first_day_vaccines.date as vaccination_date
from
	before join after using (location) 
	join first_day_vaccines using (location);



-- James, Jiusheng, Charlie
-- 1. Write a query to find all Asian countries where the ratio
-- of female smokers to male smokers is less than the average ratio of
-- female smokers to male smokers in European countries in 2020
-- (exclude all countries whose extreme poverty rate is below the Asian average).


-- first lets get the asian countries smokers and locations
with asian(female_smokers, male_smokers, location) as 
(select
	female_smokers, male_smokers, location
from
	covid
where
	continent = 'Asia' and
	date like '%2020%' and -- we also only want 2020 data
	extreme_poverty < (select -- note that we exclude all asian countries where the extreme_poverty 
				avg(extreme_poverty) -- is lower than the average
			from
				covid
			where
				continent = 'Asia')),
-- same thing with Europe but we only need the average
european(average_smokers) as 
(select 
	avg(female_smokers / male_smokers)
from
	covid
where
	continent = 'Europe' and
	date like '%2020%' and
	extreme_poverty < (select
				avg(extreme_poverty)
			from
				covid
			where
				continent = 'Asia'))

select
	distinct asian.location
from
	asian,european
where -- And here we have the comparison between the averages
	(asian.female_smokers / asian.male_smokers) < european.average_smokers;

-- James, Jiusheng, Charlie
--2. Write a query to find the fraction of cases that are deaths in the past year using the total deaths smoothed
select
	-- get total deaths smoothed
	sum(new_deaths_smoothed) / 
	-- get total new cases smoothed
	sum(new_cases_smoothed) as fraction
from
	covid
where
	-- only conssider data in 2020
	date like '%2020%';

-- Ricalton, Nigro
--1. Find the median age of each country 
-- ordered by ascending survival rate 
-- (that is calculated as (1 - (total deaths / total cases)) 
-- using the most recent date).
with most_recent as
	-- get the most recent data
	(select
		location, max(date) as date
	from
		covid
	group by
		location)
select
	-- (location, median_age, survival_rate)
	location, median_age, (1 - (total_deaths / total_cases)) as survival_rate
from
	-- get the most recent data
	covid natural join most_recent
where
	-- get rid of tuples where survival_rate may be null
	total_deaths is not null and
	total_cases is not null
order by
	survival_rate;

--2. Which country has the highest GDP per capita with no people vaccinated as of the most recent date?
with no_vaccination as
	(select
		-- countries with no people vaccinated as of the most recent date
		location, gdp_per_capita, max(date) as date
	from
		covid
	where
		-- no vaccination
		total_vaccinations is null or 
		total_vaccinations = 0
	group by
		location, gdp_per_capita),
	max_gdp as
	(select
		-- get the max gdp of these countries
		max(gdp_per_capita)
	from
		no_vaccination)
select
	location, gdp_per_capita
from
	covid natural join no_vaccination, max_gdp
where
	-- get the target country
	covid.gdp_per_capita = max_gdp.max;

-- ARMAN AND WILL
-- 1- Computing the total cases, total death and average stringency_index of all the countries starting the
-- first day a country started vaccinating.
with first_day as -- get the first day a country started vaccinating

  (select 
	min(date) -- the earliest date
  from 
	covid
  where 
	total_vaccinations > 0) -- when the total vaccinations is not 0

-- get the total cases, total deaths, and average stringency for each country 
select 
	location, sum(new_cases) as case_sum, sum(new_deaths) as death_sum, avg(stringency_index) as avg_stringency_index
from 
	covid
where 
	date >= (select date from first_day) --only take the sums and avg where the date is after (and including) the first day
group by 
	location;

-- 2- a query to find the ratio between total death and total cases for all the countries
-- that have a higher male smokers ratio than the average (worldwide average)
with avg_male_smoker as -- get worldwide avg for male smokers
   (
     select
	avg(male_smokers) as avg -- get the avg over male smokers 
     from 
	covid
   )

select 
	location, sum(new_deaths) / sum(new_cases) as deaths_to_cases_ratio -- get the cases to deaths ratio for each location 
from 
	covid, avg_male_smoker
where 
	avg_male_smoker.avg < male_smokers -- where male smokers ratio is greater than average 
group by 
	location;

-- Corinna, Kim
--1. On what date did each country surpass 200 cases per million? 
-- On what date did they begin to have net zero new cases (less than 0 new cases per day), if applicable?
with over_200 as 
	-- the first dates that cases were over 200 per million
	(select
		location, min(date) as Over_200_per_million
	from 
		covid
	where
		total_cases_per_million > 200
	group by
		location),
		
	zero as 
	-- the first dates that have net zero new cases or less
	(select
		location, min(date) as negative_new_cases
	from
		covid
	where
		-- exclude dates before the outbreak
		total_cases_per_million > 200 and 
		new_cases <= 0
	group by
		location)
select
	*
from
	-- preseve the results where there is no value for negative_new_cases
	over_200 left outer join zero using (location)
order by
	location;

--2. In countries being vaccinated, are ICU admissions dropping? 
-- This requires comparison between pre-vaccine (averaging a month before general deployment) and 
-- post-vaccine (averaging the month since general deployment) ICU admissions.
with pre_vaccine as
	-- icu data before vaccination
	(select
		-- (location, month, Avg_wk_icu_month_pre_vaccine)
		location, extract (month from cast(date as date)) as month, avg(weekly_icu_admissions) as Avg_wk_icu_month_pre_vaccine
	from
		covid
	where
		weekly_icu_admissions is not null and
		(total_vaccinations is null or
		total_vaccinations = 0)
	group by
		location, month),
	-- icu data after vaccination
	post_vaccine as
	(select
		-- (location, month,  Avg_wk_icu_month_post_vaccine)
		location, extract (month from cast(date as date)) as month, avg(weekly_icu_admissions) as Avg_wk_icu_month_post_vaccine
	from
		covid
	where
		weekly_icu_admissions is not null and
		total_vaccinations > 0
	group by
		location, month)
select
	-- (location, avg1, avg2, avg2 - avg1)
	location, Avg_wk_icu_month_pre_vaccine, Avg_wk_icu_month_post_vaccine, 
	-- compute the differenct between pre and post
	(Avg_wk_icu_month_post_vaccine - Avg_wk_icu_month_pre_vaccine) as pre_post_vaccine_difference
from
	pre_vaccine natural join post_vaccine;



