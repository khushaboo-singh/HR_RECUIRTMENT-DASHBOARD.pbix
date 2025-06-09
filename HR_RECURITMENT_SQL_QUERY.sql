create database hr_recruritmnt;

use hr_recruritmnt;

create table hr_rct (
Position varchar(max),full_name varchar(max),Gender varchar(max),Salary	varchar(max),Department varchar(max),DepartmentName varchar(max),
Division varchar(max),AssignmentCategory varchar(max),Title varchar(max),HiringAnalyst varchar(max),VacancyStatus varchar(max),
VacancyDate varchar(max),BudgetDate	varchar(max),PostingDate varchar(max),InterviewDate varchar(max),OfferDate varchar(max),
AcceptanceDate varchar(max),SecurityCheckDate varchar(max),	HireDate varchar(max));

select * from hr_rct;

bulk insert hr_rct
from 'C:\Users\Acer\Downloads\hr_recruitement.csv'
with 
	(fieldterminator = ',',
	rowterminator = '\n',
	firstrow=2,
	maxerrors=20);

SELECT * FROM HR_RCT WHERE ISDATE (BUDGETDATE) = 0

select case when isdate(vacancydate) = 0 then 'Date is not valid'
			when isdate(budgetdate) = 0 then 'Date is not valid'
			when isdate(postingdate) = 0 then 'Date is not valid'
			when isdate(interviewdate) = 0 then 'Date is not valid'
			when isdate(offerdate) = 0 then 'Date is not valid'
			when isdate(acceptancedate) = 0 then 'Date is not valid'
			when isdate(securitycheckdate) = 0 then 'Date is not valid'
			when isdate(hiredate) = 0 then 'Date is not valid'
			else 'Date is valid' end as Date_inspection
			from hr_rct;
--all datee columns are in proper date format

alter table hr_rct
alter column vacancydate date;

alter table hr_rct
alter column budgetdate date;

alter table hr_rct
alter column postingdate date;

alter table hr_rct
alter column offerdate date;

alter table hr_rct
alter column vacancydate date;

alter table hr_rct
alter column acceptancedate date;

alter table hr_rct
alter column securitycheckdate date;

alter table hr_rct
alter column hiredate date;

select column_name, data_type
from information_schema.columns;

alter table hr_rct
alter column salary money;

--checking anomalies in salary column

UPDATE hr_rct
SET salary = REPLACE(salary, SUBSTRING(salary, PATINDEX('%[^0-9]%', salary), 1), '')
WHERE ISNUMERIC(salary) = 0;


select gender as 'gend_distribution', DEPARTMENT,count(position) as 'vacancies-DISTRIBUTED'
FROM HR_RCT
GROUP BY GENDER,DEPARTMENT
ORDER BY GENDER;

---PIVOT
select department,
		isnull([M],0) as 'male_vacancy',
		isnull([F],0) as 'female_vacancy'
from
	(select Department, gender, count(position) as 'vacancies'
		from hr_rct
		group by gender, Department)
as base_table
pivot
	(sum(vacancies) for gender in ([M],[F])) as pivot_table
	order by male_vacancy;


select * from hr_rct;

select Department,count(position) as 'total_av_position', sum(case when vacancystatus = 'filled' then 1 else 0 end) as 'filled_position',
				sum(case when vacancystatus = 'vacant' then 1 else 0 end) as 'vacant_position'
				from hr_rct
				group by department
				order by filled_position desc;

--let's check the hiring duration in the data
-- vacancydate (fron 1st to last vacancy released date)
--vacancy duration in years and months

select concat(datediff(month,min(vacancydate),max(vacancydate))/12, 'years',' ',datediff(month,min(vacancydate),max(vacancydate))%12, 'months',' ')
from hr_rct

--there is 62 years and 1 month hiring duration

select min(vacancydate) as '1stvacancydate', max(vacancydate) as 'lastvacancydate'
from hr_rct;

--1st vacancydate-1954-10-11 and lastvacancydate: 2016-11-26

--last 5 years of recuritment analysis summary
select hiredate, dateadd(year, -5,hiredate) from hr_rct; /* 2010-01-30*/

with maxhire as (
	select max(hiredate) as max_date from hr_rct
	)
select
h.departmentname,h.gender,h.Position,
coalesce (avg(case when h.hiredate >= dateadd(year,-1,m.max_date) 
		then h.salary end),0) as 'avgrecentlyhired',
coalesce (avg(case when h.hiredate < dateadd(year, -5, m.max_date)
		then h.salary end),0) as 'hired5yearback'
from hr_rct h
cross join
maxhire m
group by
h.departmentname,h.gender,h.Position
having 
COALESCE(AVG(CASE 
        WHEN h.hiredate >= DATEADD(YEAR, -1, m.max_date) 
        THEN h.salary 
    END), 0) > 0;

---Genderwise pay gap analysis

WITH gender_avg_sal AS (
    SELECT gender, AVG(salary) AS avg_salary
    FROM hr_rct GROUP BY gender)
SELECT 
    gvs.gender AS genM, 
    gvs1.gender AS genF,
    gvs.avg_salary - gvs1.avg_salary AS sal_gap
FROM 
    gender_avg_sal gvs, 
	---cross join gender_avg_sas gvs1
    gender_avg_sal gvs1 ----result is similar to cross join
WHERE 
    gvs.gender = 'M' 
    AND gvs1.gender = 'F';

-- 4528.1347

---department wise avg_time for hiring
with hiredays as (select department, full_name, datediff(day,postingdate,hiredate)as 'daystohire'
from hr_rct)

select department,avg(daystohire) as 'avg_daystohire'
from hiredays
group by department;
--58 days is avg hiring day

--department wise salary analysis
select departmentname,
	avg(salary) as avgsal,
	min(salary) as minsal,
	max(salary) as maxsal
from hr_rct
group by departmentname;

--pipeline of whole recuirtment process
select * from hr_rct;

WITH RECT_PIPELINE as (select full_name,
	datediff(day,vacancydate,budgetdate) as 'vacancytobudget',
	datediff(day,budgetdate, postingdate) as 'budgettoposting',
	datediff(day,postingdate,interviewdate) as 'postingtointerview',
	datediff(day,offerdate,acceptancedate) as 'offertoacceptance',
	datediff(day,acceptancedate,securitycheckdate) as 'accepttosecurity',
	datediff(day,securitycheckdate,hiredate) as 'Securitytohire'
	from hr_rct)

select full_name, avg(vacancytobudget) as 'avg_vacancytobudget',
				AVG(budgettoposting) as 'avg_budgettoposting',
				AVG(postingtointerview) as 'avg_postingtointerview',
				AVG(offertoacceptance) as 'avg_offertoacceptance',
				AVG(accepttosecurity) as 'avg_acceptancetosecurity',
				AVG(Securitytohire) as 'avg_securitytohire'
from RECT_PIPELINE
where full_name is not null
group by full_name;

---Department wise recuritment efficiency 

select departmentName, datediff(day, vacancydate, hiredate) as 'hire_days' 
from hr_rct;

select distinct title from hr_rct;
----and title wise their first relased(position reequired) by department

select title, min(postingdate) as '1stpostingdate', year(min(postingdate)) as 'the_yeear', 
datename(month, min(postingdate)) as 'releasedmonth', datename(weekday, min(postingdate)) as 'the_day'
from hr_rct
group by title;

--time takenby the vaxancy to release offer date

with vacancy_to_offertime as (select departmentName, title, datediff(day, vacancydate, offerdate) as 'days_to_offer'
from hr_rct)

select departmentName,title, min(days_to_offer) as 'min_time',
							avg(days_to_offer) as 'avg_time',
							max(days_to_offer) as 'max_date'
from vacancy_to_offertime
group by departmentname, title
order by departmentname;
	

select distinct HiringAnalyst from hr_rct;     --we have 20 hiring analyst

----efficiency of hiring analyst

WITH Analyst_analysis AS (
    SELECT 
        hiringanalyst, 
        DATEDIFF(day, postingdate, hiredate) AS hiredays
    FROM 
        hr_rct
)
SELECT 
    hiringanalyst, 
    AVG(hiredays) AS avg_time, 
    MIN(hiredays) AS min_time, 
    MAX(hiredays) AS max_time,
    COUNT(*) AS total_hiring
FROM 
    Analyst_analysis
GROUP BY 
    hiringanalyst
ORDER BY 
    total_hiring DESC;

--- Analysis for time taken by the vacancy to release the offer

with vacancytime_to_offer as (
							select DepartmentName,title, datediff(day, vacancydate, offerdate) as 'Days_to_offer'
							from hr_rct)
select DepartmentName,
		min(days_to_offer) as 'min_time',
		avg(days_to_offer) as 'avg_time',
		max(days_to_offer) as 'max_time'
from vacancytime_to_offer
group by DepartmentName, title
order by departmentname;

select * from hr_rct;

---Analysis for the variance in recruitment duration by position and department (variance = std_deviation)

---Trnd analysis for the recruitment, how recruitment takes place during recruitment journey
---Trnd ovr time 
--time interval

select dEPARTMENT, year(hiredate) as 'the_yearofhire', datename(quarter,hiredate) as 'the_hired_qtr', 
		datename(month,hiredate) as 'the_month_of_hire',
		count(*) as 'Total_hiring'
		from hr_rct
		where VacancyStatus = 'Filled'
		group by year(hiredate), datename(quarter,hiredate), DEPARTMENT,datename(month,hiredate)
		order by the_yearofhire;  ---Evry hiring happened during 1st quarter and 1st month every year


select postingdate, datename(month,postingdate) as 'posting_month'
from hr_rct
order by posting_month; 

--so all posting month are in oct/novor dec and hiring month is jan

---seasonality for a decade group (10yrs hiring group)


WITH hiringtrend AS (
    SELECT department, 
        YEAR(hiredate) AS the_year, 
        DATENAME(month, hiredate) AS month_name,
        COUNT(*) AS total_hiring
    FROM hr_rct  WHERE vacancystatus = 'Filled'
	grouP BY department,  DATENAME(month, hiredate),  YEAR(hiredate)
)
SELECT  department,  the_year,  total_hiring,  month_name,
    CASE 
        WHEN the_year BETWEEN 1954 AND 1964 THEN '1954-1964'
        WHEN the_year BETWEEN 1965 AND 1975 THEN '1965-1975'
        WHEN the_year BETWEEN 1976 AND 1986 THEN '1976-1986'
        WHEN the_year BETWEEN 1987 AND 1997 THEN '1987-1997'
        WHEN the_year BETWEEN 1998 AND 2008 THEN '1998-2008'
        ELSE '2009-2019'  END AS yearsegment
FROM hiringtrend;

----

select position, count(full_name) from hr_rct
where AcceptanceDate is not null
group by position
having count(full_name) >1;  ---- we don't have any candidate who is holding multiple offer before theia acceptancedate

---calculate the avg time to fulfill the post by the firing analyst and also thir acceptance rate

select hiringanalyst, title, avg(datediff(day, postingdate,hiredate)) as 'avg_time_taken',
count(position) as 'total_vacancies',
cast(sum(case when acceptancedate is not null then 1 else 0 end) as float)/ count(*) * 100 as 'acceptancerate'
from hr_rct
group by HiringAnalyst, Title
order by acceptancerate;

----Analysis for the position which exceeded the budget time( considering budget time is time alloted to full fill the position)

select position, department,datediff(day, vacancydate,hiredate) as 'time_to_fill',
	datediff(day, vacancydate,budgetdate) as 'budgettimetofill'
	from hr_rct
where datediff(day, vacancydate, hiredate) > datediff(day, vacancydate,budgetdate)
order by budgettimetofill desc;

/* we are getting 8998 position record, that means every position exceeded its budget date */

---LET'S find out the minimum & maximum time to fullfill as per the position

select top 1 position, datediff(day,vacancydate,hiredate) as timetofill
from hr_rct
where hiredate is not null
order by timetofill desc;
----the position took maximum days to fill is 14230 and days to fill is 127 days

select top 1 position, datediff(day,vacancydate,hiredate) as timetofill
from hr_rct
where hiredate is not null
order by timetofill;
--- the position took minimum time to fill - 16562 and time taken-39 days

---summarizing it in one query

with timetofillcte as
(select position, datediff(day,vacancydate,hiredate) as timetofill
from hr_rct)
select position, timetofill
from timetofillcte
where timetofill = (select max(timetofill) from timetofillcte)

union all

select position, timetofill
from timetofillcte
where timetofill = (select min(timetofill) from timetofillcte)

---the vacant position is the part of attrition (we can assume because those title were present there)

--- the organisation wants to create an attrition report by department with their avg salary

select departmentname, avg(salary) as 'avg_sal', count(*) as 'total_position',
		sum(case when VacancyStatus = 'vacant' then 1 else 0 end) as 'vacant_pos',
		round(cast(sum(case when vacancystatus = 'vacant' then 1 else 0 end) as float)/count(*),2) AS 'ATTRITION_RATE'
		from hr_rct
		GROUP BY departmentname
		having round(cast(sum(case when vacancystatus = 'vacant' then 1 else 0 end) as float)/count(*),2) > 0.0
		order by ATTRITION_RATE desc;

/* dept of police and dept of gen services has attrition rate more than other department */

select department, avg(salary) as 'avg_sal' from hr_rct
group by department;

select * from hr_rct
where department = 'DHS' and salary > 89217.169;

---
with avg_salarybydept as (select departmentname, avg(salary) as avg_sal from hr_rct
							group by DepartmentName)

select h.departmentname, avg_sal, count(position) as 'totalhire',
		round(count(case when h.salary > av.avg_sal then 1 end) * 100, 2)/ count(*) as 'percmore>avg_sal'
from hr_rct h
join avg_salarybydept av
on h.DepartmentName = av.DepartmentName
group by h.departmentname, av.avg_sal
order by  avg_sal desc;









 





