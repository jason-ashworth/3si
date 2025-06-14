/*
	Question#2 part1. How many unique addresses are there in each file?
	Answer
		- headstart_wa: 320
		- ncesdata_2D86566_wa: 2235

	Explanation of Decisions
	*	Setup reusable CTEs for each dataset
	*	Populate CTEs with distinct address records
	*	I chose "group by" over DISTINCT for performance
	*	I visually inspected the data and chose to filter zipFour
	*		This helps eliminate "dirty" data that could exclude desired results 
*/
;with headstart as (
	select
		addressLineOne
	,	addressLineTwo
	,	city
	,	state
	,	zipFive
	,	case
			when zipFour < 1 then null
			else zipFour
		end as zipFour
	,	county
	from
		headstart_wa
	group by
		addressLineOne
	,	addressLineTwo
	,	city
	,	state
	,	zipFive
	,	zipFour
	,	county
)
,ncesdata as (
	select
		Street_Address
	,	City
	,	State
	,	ZIP
	,	ZIP_4_digit
	from
		ncesdata_2D86566_wa
	group by
		Street_Address
	,	City
	,	State
	,	ZIP
	,	ZIP_4_digit
)

/* Query to gather unique address record counts per dataset */
select
	'headstart' as dataset_name
,	count(*) as unique_adresses
from
	headstart -- 320
union
select
	'ncesdata' as dataset_name
,	count(*) as unique_adresses
from
	ncesdata -- 2235


/*
	Question#2 part2. What is the overlap?  (How many of the same address exists in both files?)
	Answer
		- When including *all* available mappable fields from each dataset: 19
		- When including all *except zip_4_digit_adr*: 49
		- When including all *except zip_4_digit_adr, zip_adr*: 54

	Explanation of Decisions
	*	Includes the same from part1
	*	Removed some columns from the setup
	*		The headstart data has more fields for address than ncesdata
*/


;with headstart as (
	select
		addressLineOne as street_adr
	--,	addressLineTwo --> excluded because ncesdata does not have a line 2 for address records
	,	city as city_adr
	,	state as state_adr
	,	zipFive as zip_adr
	,	case
			when zipFour < 1 then null
			else zipFour
		end as zip_4_digit_adr
	--,	county --> excluded because ncesdata does not have a county for address records
	from
		headstart_wa
	group by
		addressLineOne
	,	city
	,	state
	,	zipFive
	,	case
			when zipFour < 1 then null
			else zipFour
		end
)
,ncesdata as (
	select
		Street_Address as street_adr
	,	City as city_adr
	,	State as state_adr
	,	ZIP as zip_adr
	,	ZIP_4_digit as zip_4_digit_adr
	from
		ncesdata_2D86566_wa
	group by
		Street_Address
	,	City
	,	State
	,	ZIP
	,	ZIP_4_digit
)

select
	count(*) as number_of_overlap
from
	headstart h
join
	ncesdata n
	on	n.street_adr = h.street_adr
	and	n.city_adr = h.city_adr
	and	n.state_adr = h.state_adr -- 54
	-- comment out AND conditions below for different results
	and	n.zip_adr = h.zip_adr -- 49
	and	n.zip_4_digit_adr = h.zip_4_digit_adr -- 19


/*
	Question#2 part 3. Create a query that includes everything from the Headstart data and an indicator whether there is a match in the NCES data
	Answer (below)

	Explanation of Decisions
	*	Includes the same from part 1 and part 2
	*	I added an order by to the results for quicker visual analysis
*/


;with headstart as (
	select
		addressLineOne as street_adr
	--,	addressLineTwo --> excluded because ncesdata does not have a line 2 for address records
	,	city as city_adr
	,	state as state_adr
	,	zipFive as zip_adr
	,	case
			when zipFour < 1 then null
			else zipFour
		end as zip_4_digit_adr
	--,	county --> excluded because ncesdata does not have a county for address records
	from
		headstart_wa
	group by
		addressLineOne
	,	city
	,	state
	,	zipFive
	,	case
			when zipFour < 1 then null
			else zipFour
		end
)
,ncesdata as (
	select
		Street_Address as street_adr
	,	City as city_adr
	,	State as state_adr
	,	ZIP as zip_adr
	,	ZIP_4_digit as zip_4_digit_adr
	from
		ncesdata_2D86566_wa
	group by
		Street_Address
	,	City
	,	State
	,	ZIP
	,	ZIP_4_digit
)

select
	h.*
,	case
		when n.street_adr is null
			then 'false'
		else
			'true'
	end as is_match_indicator
from
	headstart h
left join
	ncesdata n
	on	n.street_adr = h.street_adr
	and	n.city_adr = h.city_adr
	and	n.state_adr = h.state_adr
	-- comment out AND conditions below for different results
	and	n.zip_adr = h.zip_adr
	and	n.zip_4_digit_adr = h.zip_4_digit_adr
order by
	is_match_indicator desc
,	street_adr