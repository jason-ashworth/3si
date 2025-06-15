/* Initial scratch queries */
-- What am I looking at here?
select
	*
from
	ProblemOne;

-- Any clues when looking for keywords from the assignment's problem statement? - Not so much
select
	*
from
	ProblemOne
where
	column3 in (
		'specific'
	,	'answer'
	,	'you'
	,	'hidden'
	,	'message'
	)
	or	column6 in (
		'specific'
	,	'answer'
	,	'you'
	,	'hidden'
	,	'message'
	);

-- I thought about asking Jenny, but she wasn't home
select
	*
from
	ProblemOne
where
	column1 = 8675309

-- A couple of real attempts at building keys or part of keys to look for in other rows
-- Neither of these are the most performant and the results did not seem to get me any closer to an answer
------ temp tables and joins approach
if object_id('tempdb..#answers') is not null drop table #answers
if object_id('tempdb..#resulting_rows') is not null drop table #resulting_rows

select * into #resulting_rows from ProblemOne where 1 = 0 -- create an empty replica of the text file (ProblemOne)

select
	row_number() over (order by column1) as id
,	right(
		cast(column1 as nvarchar(200))
		, 1
	) +
	right(
		cast(column2 as nvarchar(200))
		, 1
	) +
	right(
		cast(column4 as nvarchar(max))
		, 1
	) +
	right(
		cast(column5 as nvarchar(200))
		, 1
	) as gen_key
,	column1 as c1
,	column2 as c2
,	column3 as c3
,	column4 as c4
,	column5 as c5
,	column6 as c6
into
	#answers
from
	ProblemOne
where
	lower(column3) = 'answer'

insert into #resulting_rows (
	column1
,	column2
,	column3
,	column4
,	column5
,	column6
)

select
	p.*
from
	ProblemOne p
join
	#answers a
	on	cast(p.column1 as nvarchar(max)) like '%' + a.gen_key + '%'

select * from #resulting_rows

------ temp tables and looping approach
if object_id('tempdb..#answers') is not null drop table #answers
if object_id('tempdb..#resulting_rows') is not null drop table #resulting_rows

select * into #resulting_rows from ProblemOne where 1 = 0 -- create an empty repliace of the text file (ProblemOne)

select
	row_number() over (order by column1) as id
,	column1 as c1
,	column2 as c2
,	column3 as c3
,	column4 as c4
,	column5 as c5
,	column6 as c6
into
	#answers
from
	ProblemOne
where
	lower(column3) = 'answer'

declare @counter int = 1
while @counter <= (select max(id) from #answers)
begin
	declare
		@c1 nvarchar(1)
	,	@c2 nvarchar(1)
	,	@c4 nvarchar(1)
	,	@c5 nvarchar(1)

	select @c1 = right(
		cast(c1 as nvarchar(max)), 1)
	from
		#answers
	where
		id = @counter

	select @c2 = right(
		cast(c2 as nvarchar(max)), 1)
	from
		#answers
	where
		id = @counter

	select @c4 = right(
		cast(c4 as nvarchar(max)), 1)
	from
		#answers
	where
		id = @counter

	select @c5 = right(
		cast(c5 as nvarchar(max)), 1)
	from
		#answers
	where
		id = @counter

	declare @key nvarchar(4)
	select @key = @c1 + @c2 + @c4 + @c5

	insert into #resulting_rows
	select * from ProblemOne
	where cast(column1 as nvarchar(200)) like '%' + @key + '%'
end

select * from #resulting_rows

-- As I began to realize I was running out of the suggested 30 minutes...
-- Alas, it dawned on me
select
	*
from
	ProblemOne
where
	column1 = 42 -- This is the answer to the ultimate question of life, the universe, and everything (Hitchhiker's Guide to the Galaxy)