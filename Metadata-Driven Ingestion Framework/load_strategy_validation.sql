insert into `sample_superstore`.`superstore`.`src_people`
values ('Robert Joe', 'South', current_timestamp(), 'U')
;

select * from `sample_superstore`.`superstore`.`src_people` order by LOAD_DTE DESC;

select 'append' as src, * from  `superstore_silver`.`silver`.`people_append`;
select 'override' as src, * from  `superstore_silver`.`silver`.`people_override`;
select 'scd1' as src, * from  `superstore_silver`.`silver`.`people_scd1`;
select 'scd2' as src, * from  `superstore_silver`.`silver`.`people_scd2`;

select * from sample_superstore.superstore.job_audit_metadata
order by job_start_time_timestamp desc
limit 4;
