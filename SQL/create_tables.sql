-- DROP TABLE IF EXISTS public.advisor;
-- DROP TABLE IF EXISTS public.classroom;
-- DROP TABLE IF EXISTS public.course;
-- DROP TABLE IF EXISTS public.department;
-- DROP TABLE IF EXISTS public.instructor;
-- DROP TABLE IF EXISTS public.prereq;
-- DROP TABLE IF EXISTS public.section;
-- DROP TABLE IF EXISTS public.student;
-- DROP TABLE IF EXISTS public.takes;
-- DROP TABLE IF EXISTS public.teaches;
-- DROP TABLE IF EXISTS public.time_slot;

-- -- Table: public.advisor
-- CREATE TABLE IF NOT EXISTS public.advisor (
--     s_id VARCHAR(5),
--     i_id VARCHAR(5)
-- );

-- -- Table: public.classroom
-- CREATE TABLE IF NOT EXISTS public.classroom (
--     building VARCHAR(15),
--     room_number VARCHAR(7),
--     capacity NUMERIC(4)
-- );

-- -- Table: public.course
-- CREATE TABLE IF NOT EXISTS public.course (
--     course_id VARCHAR(8),
--     title VARCHAR(50),
--     dept_name VARCHAR(20),
--     credits NUMERIC(2)
-- );

-- -- Table: public.department
-- CREATE TABLE IF NOT EXISTS public.department (
--     dept_name VARCHAR(20),
--     building VARCHAR(15),
--     budget NUMERIC(12,2)
-- );

-- -- Table: public.instructor
-- CREATE TABLE IF NOT EXISTS public.instructor (
--     id VARCHAR(5),
--     dept_name VARCHAR(20),
--     name VARCHAR(20),
--     salary NUMERIC(8,2)
-- );

-- -- Table: public.prereq
-- CREATE TABLE IF NOT EXISTS public.prereq (
--     course_id VARCHAR(8),
--     prereq_id VARCHAR(8)
-- );

-- -- Table: public.section
-- CREATE TABLE IF NOT EXISTS public.section (
--     course_id VARCHAR(8),
--     sec_id VARCHAR(8),
--     semester VARCHAR(6),
--     year NUMERIC(4),
--     building VARCHAR(15),
--     room_number VARCHAR(7),
--     time_slot_id VARCHAR(4)
-- );

-- -- Table: public.student
-- CREATE TABLE IF NOT EXISTS public.student (
--     id VARCHAR(5),
--     dept_name VARCHAR(20),
--     name VARCHAR(20),
--     tot_cred NUMERIC(3)
-- );

-- -- Table: public.takes
-- CREATE TABLE IF NOT EXISTS public.takes (
--     id VARCHAR(5),
--     course_id VARCHAR(8),
--     semester VARCHAR(6),
--     sec_id VARCHAR(8),
--     year NUMERIC(4),
--     grade VARCHAR(2)
-- );

-- -- Table: public.teaches
-- CREATE TABLE IF NOT EXISTS public.teaches (
--     id VARCHAR(5),
--     course_id VARCHAR(8),
--     semester VARCHAR(6),
--     sec_id VARCHAR(8),
--     year NUMERIC(4)
-- );

-- -- Table: public.time_slot
-- CREATE TABLE IF NOT EXISTS public.time_slot (
--     time_slot_id VARCHAR(4) COLLATE pg_catalog."default",
--     day VARCHAR(1) COLLATE pg_catalog."default",
--     start_hr NUMERIC(2),
--     start_min NUMERIC(2),
--     end_hr NUMERIC(2),
--     end_min NUMERIC(2)
-- );

---------------------------------------
-- VERSÃO COM TODOS OS DADOS VARCHAR!!!
---------------------------------------
DROP TABLE IF EXISTS public.advisor;
DROP TABLE IF EXISTS public.classroom;
DROP TABLE IF EXISTS public.course;
DROP TABLE IF EXISTS public.department;
DROP TABLE IF EXISTS public.instructor;
DROP TABLE IF EXISTS public.prereq;
DROP TABLE IF EXISTS public.section;
DROP TABLE IF EXISTS public.student;
DROP TABLE IF EXISTS public.takes;
DROP TABLE IF EXISTS public.teaches;
DROP TABLE IF EXISTS public.time_slot;

-- Table: public.advisor
CREATE TABLE IF NOT EXISTS public.advisor (
    s_id VARCHAR(5),
    i_id VARCHAR(5)
);

-- Table: public.classroom
CREATE TABLE IF NOT EXISTS public.classroom (
    building VARCHAR(15),
    room_number VARCHAR(7),
    capacity VARCHAR(4)
);

-- Table: public.course
CREATE TABLE IF NOT EXISTS public.course (
    course_id VARCHAR(8),
    title VARCHAR(50),
    dept_name VARCHAR(20),
    credits VARCHAR(2)
);

-- Table: public.department
CREATE TABLE IF NOT EXISTS public.department (
    dept_name VARCHAR(20),
    building VARCHAR(15),
    budget VARCHAR(12)
);

-- Table: public.instructor
CREATE TABLE IF NOT EXISTS public.instructor (
    id VARCHAR(5),
    name VARCHAR(20),
    dept_name VARCHAR(20),
    salary VARCHAR(8)
);

-- Table: public.prereq
CREATE TABLE IF NOT EXISTS public.prereq (
    course_id VARCHAR(8),
    prereq_id VARCHAR(8)
);

-- Table: public.section
CREATE TABLE IF NOT EXISTS public.section (
    course_id VARCHAR(8),
    sec_id VARCHAR(8),
    semester VARCHAR(6),
    year VARCHAR(4),
    building VARCHAR(15),
    room_number VARCHAR(7),
    time_slot_id VARCHAR(4)
);

-- Table: public.student
CREATE TABLE IF NOT EXISTS public.student (
    id VARCHAR(5),
    name VARCHAR(20),
    dept_name VARCHAR(20),
    tot_cred VARCHAR(3)
);

-- Table: public.takes
CREATE TABLE IF NOT EXISTS public.takes (
    id VARCHAR(5),
    course_id VARCHAR(8),
    semester VARCHAR(6),
    sec_id VARCHAR(8),
    year VARCHAR(4),
    grade VARCHAR(2)
);

-- Table: public.teaches
CREATE TABLE IF NOT EXISTS public.teaches (
    id VARCHAR(5),
    course_id VARCHAR(8),
    semester VARCHAR(6),
    sec_id VARCHAR(8),
    year VARCHAR(4)
);

-- Table: public.time_slot
CREATE TABLE IF NOT EXISTS public.time_slot (
    time_slot_id VARCHAR(4) COLLATE pg_catalog."default",
    day VARCHAR(1) COLLATE pg_catalog."default",
    start_hr VARCHAR(2),
    start_min VARCHAR(2),
    end_hr VARCHAR(2),
    end_min VARCHAR(2)
);