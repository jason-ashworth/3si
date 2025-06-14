declare
	@start datetime
,	@end datetime
,	@diff datetime
,	@cr char(1)

set @cr = char(13)

select @start = getdate()
select distinct
	addressLineOne
,	addressLineTwo
,	city
,	state
,	case
		when zipFour < 1 then null
		else zipFour
	end as zipFour
,	county
from
	headstart_wa
select @end = getdate()
select @diff = @end - @start
print('start distinct: ' + convert(varchar, @start, 21))
print('end distinct: ' + convert(varchar, @end, 21))
print('elapsed time distinct: ' + convert(varchar, @diff, 21))

print(@cr)

select @start = getdate()
select
	addressLineOne
,	addressLineTwo
,	city
,	state
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
,	zipFour
,	county
select @end = getdate()
select @diff = @end - @start
print('start group by: ' + convert(varchar, @start, 21))
print('end group by: ' + convert(varchar, @end, 21))
print('elapsed time distinct: ' + convert(varchar, @diff, 21))