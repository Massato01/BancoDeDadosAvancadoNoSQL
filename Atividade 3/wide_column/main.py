import socket
import pandas as pd
from cassandra.cluster    import Cluster
from sqlalchemy           import create_engine, text
from sqlalchemy.orm       import sessionmaker
from sqlalchemy           import URL

# === Conectando ao Cassandra ===
hostname = socket.gethostname()
ip_address = socket.gethostbyname(hostname)
port = 9042

cluster = Cluster([ip_address], port=port)
session = cluster.connect('cc6240')

# === Create ===
def criar_tabelas():
    # instructor_teaches
    session.execute("CREATE TABLE instructor_teaches (id text, name text, dept_name text, salary float, course_id text, title text, semester text, primary key((course_id, id)));")
    session.execute("CREATE INDEX ON instructor_teaches(semester);")
    
    # course_department
    session.execute("CREATE TABLE course_department (dept_name text, building text, budget float, course_id text, title text, credits int, PRIMARY KEY (dept_name, course_id, title));")

    # course
    session.execute("CREATE TABLE course (course_id text, title text, dept_name text, credits int, PRIMARY KEY((course_id), dept_name));")
    
    # section
    session.execute("CREATE TABLE section (course_id text, sec_id text, semester text, year int, building text, room_number text, time_slot_id text, title text, PRIMARY KEY ((course_id), sec_id, semester, year));")
    session.execute("CREATE INDEX ON section(semester);")
    
    # students_course
    session.execute("CREATE TABLE students_course (dept_name text, building text, budget float, course_id text, title text, credits int, id text, name text, tot_cred  int, PRIMARY KEY ((name), title, dept_name));")
    session.execute("CREATE INDEX ON students_course(title);")

    # students_teaches
    session.execute("CREATE TABLE students_teaches (id_instructor text, name text, salary float, id_student text, name_student text, dept_name text, tot_cred int, PRIMARY KEY ((name),  name_student, dept_name));")
    session.execute("CREATE INDEX ON students_teaches(dept_name);")

    # student_advisor
    session.execute("CREATE TABLE student_advisor (id_instructor text, name_instructor text, id_student text, name_student text, PRIMARY KEY ((id_instructor, name_instructor, id_student, name_student)));")

    # classroom_section
    session.execute("CREATE TABLE classroom_section (building text, room_number text, capacity int, PRIMARY KEY ((building)));")

    # instructor_student
    session.execute("CREATE TABLE instructor_student (s_id text, i_id text, instructor_name text, student_name text, PRIMARY KEY ((instructor_name), student_name));")

    # prereq
    session.execute("CREATE TABLE prereq_course (prereq_id text, course_id text, title_prereq text, title_course text, PRIMARY KEY ((prereq_id, course_id)))")

# === Delete ===
def limpar_tabela(tabela):
    if tabela == 'student':
        students = session.execute('SELECT name FROM student;')
        for s in students:
            session.execute(f"DELETE FROM {tabela} WHERE name = '{s[0]}';")

    elif tabela == 'instructor':
        instructor = session.execute('SELECT name FROM instructor;')
        for i in instructor:
            session.execute(f"DELETE FROM {tabela} WHERE name = '{i[0]}';")
    
    elif tabela == 'course_department':
        course_department = session.execute('SELECT dept_name FROM course_department;')
        for i in course_department:
            session.execute(f"DELETE FROM {tabela} WHERE dept_name = '{i[0]}';")
    
    elif tabela == 'section':
        course_id = session.execute('SELECT course_id FROM section;')
        for i in course_id:
            session.execute(f"DELETE FROM {tabela} WHERE course_id = '{i[0]}';")
            
    elif tabela == 'students_course':
        name = session.execute('SELECT name FROM students_course;')
        for i in name:
            session.execute(f"DELETE FROM {tabela} WHERE name = '{i[0]}';")

    elif tabela == 'students_teaches':
        name = session.execute('SELECT name FROM students_teaches;')
        for i in name:
            session.execute(f"DELETE FROM {tabela} WHERE name = '{i[0]}';")

    elif tabela == 'instructor_teaches':
        name = session.execute('SELECT name FROM instructor_teaches;')
        for i in name:
            session.execute(f"DELETE FROM {tabela} WHERE name = '{i[0]}';")

def deletar_tabela(tabela):
    try:
        session.execute(f'DROP TABLE {tabela};')
    except:
        print(f'Tabela "{tabela}" não encontrada!')
        pass

# === Outras funções ===
def listar_colunas(tabela):
    instructor = session.execute(f'select * from {tabela};')
    for i in instructor:
        print(i)

def selecionar_instrutor(nomes=[]):
    prepared_statement = session.prepare("select * from instructor where name = ?;")
    for i in nomes:
        intructor = session.execute(prepared_statement, [i]).one()
        print(intructor)

def inserir_dados(name, dept_name, advisor, tot_cred):
    session.execute(f"INSERT INTO student (name, dept_name, advisor, tot_cred) VALUES ('{name}', '{dept_name}', '{advisor}', {tot_cred});")

# === Conectando ao Postgres ===
url_object = URL.create(
    "postgresql+pg8000",
    username="dcxhxgql",
    password="bPlk_dl7Xc4l0WEaPCJIYR4dnO9kGZbz",
    host="motty.db.elephantsql.com",
    database="dcxhxgql",
)

engine = create_engine(url_object)
engine = create_engine(url_object)
Session = sessionmaker(bind=engine)
session_postgres = Session()
conn = engine.connect()

# === Configurando as tabelas do Cassandra ===
deletar_tabela('course_department')
deletar_tabela('section')
deletar_tabela('students_course')
deletar_tabela('students_teaches')
deletar_tabela('course')
deletar_tabela('instructor_teaches')
deletar_tabela('student_advisor')
deletar_tabela('classroom_section')
deletar_tabela('instructor_student')
deletar_tabela('prereq_course;')
criar_tabelas()

# === Inserindo dados nas tabelas ===
limpar_tabela('instructor_teaches')

query = """
select
	i.id,
	i.name,
	i.dept_name,
	i.salary,
	c.course_id,
	c.title,
	t.semester
from
	instructor i
join
	teaches t on i.id = t.id
join
	course c on t.course_id = c.course_id;
"""
instructors = conn.execute(text(query)).all()

inst_teaches_cassandra = []
		
for inst in instructors:
    query = f"INSERT INTO instructor_teaches(id, name, dept_name, salary, course_id, title, semester) VALUES (%s, %s, %s, %s, %s, %s, %s);"
    session.execute(query, (inst[0], inst[1], inst[2], inst[3], inst[4], inst[5], inst[6]))
    inst_teaches_cassandra.append(inst)

session_postgres.close()

limpar_tabela('course_department')

query = """
SELECT 
    *
FROM
    department d 
JOIN
    course c on d.dept_name = c.dept_name;
"""
cursos = conn.execute(text(query)).all()

cursos_dept_cassandra = []
		
for curso in cursos:
    query = f"INSERT INTO course_department(dept_name, building, budget, course_id, title, credits) VALUES (%s, %s, %s, %s, %s, %s);"
    session.execute(query, (curso[0], curso[1], curso[2], curso[3], curso[4], curso[6]))
    cursos_dept_cassandra.append(curso)

session_postgres.close()

limpar_tabela('section')

query = """
SELECT 
    s.course_id,
    s.sec_id,
    s.semester,
    s.year,
    s.building,
    s.room_number,
    s.time_slot_id,
    c.title
FROM
    section s
JOIN
    course c ON s.course_id = c.course_id;
"""

sections = conn.execute(text(query)).all()

section_cassandra = []
	
for section in sections:
    query = f"INSERT INTO section(course_id, sec_id, semester, year, building, room_number, time_slot_id, title) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);"
    session.execute(query, (section[0], section[1], section[2], section[3], section[4], section[5], section[6], section[7]))
    section_cassandra.append(section)

session_postgres.close()

limpar_tabela('students_course')

query = """
SELECT
    d.dept_name,
    d.building,
    d.budget,
    c.course_id,
    c.title,
    c.credits,
    s.id,
    s.name,
    s.tot_cred 
FROM
    department d  
JOIN
    course c on d.dept_name = c.dept_name 
JOIN
    student s on c.dept_name = s.dept_name;
"""
stud_course = conn.execute(text(query)).all()

stud_course_cassandra = []
		
for stud in stud_course:
    query = f"INSERT INTO students_course(dept_name, building, budget, course_id, title, credits, id, name, tot_cred) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);"
    session.execute(query, (stud[0], stud[1], stud[2], stud[3], stud[4], stud[5], stud[6], stud[7], stud[8]))
    stud_course_cassandra.append(stud)

session_postgres.close()

limpar_tabela('students_teaches')

query = """
SELECT
    i.id AS id_instructor,
    i.name,
    i.salary,
    s.id AS id_student,
    s.name,
    s.dept_name,
    s.tot_cred
FROM
    instructor i
JOIN
    student s on i.dept_name = s.dept_name;
"""
stud_teach = conn.execute(text(query)).all()

stud_teach_cassandra = []
		
for stud in stud_teach:
    query = f"INSERT INTO students_teaches(id_instructor, name, salary, id_student, name_student, dept_name, tot_cred) VALUES (%s, %s, %s, %s, %s, %s, %s);"
    session.execute(query, (stud[0], stud[1], stud[2], stud[3], stud[4], stud[5], stud[6]))
    stud_teach_cassandra.append(stud)

session_postgres.close()

limpar_tabela('student_advisor')

query = """
select
    i.id AS id_instructor,
    i.name AS name_instructor,
    s.id AS id_student,
	s.name AS name_student
from
	instructor i
join
	advisor a on i.id = a.i_id
join
	student s on a.s_id = s.id;
"""
student = conn.execute(text(query)).all()

student_advisor_cassandra = []

for stud in student:
    query = f"INSERT INTO student_advisor(id_instructor, name_instructor, id_student, name_student) VALUES (%s, %s, %s, %s);"
    session.execute(query, (stud[0], stud[1], stud[2], stud[3]))
    student_advisor_cassandra.append(stud)

session_postgres.close()

limpar_tabela('classroom_section')

query = """
select
	c.building,
	c.room_number,
	c.capacity
from
	classroom c
left join
	section s on c.building = s.building
where
	s.course_id is null;
"""
student = conn.execute(text(query)).all()

student_advisor_cassandra = []

for stud in student:
    query = f"INSERT INTO classroom_section(building, room_number, capacity) VALUES (%s, %s, %s);"
    session.execute(query, (stud[0], stud[1], stud[2]))
    student_advisor_cassandra.append(stud)

session_postgres.close()

limpar_tabela('instructor_student')

query = """
SELECT
	a.s_id,
	a.i_id,
	i.name as instructor_name,
	s.name as student_name
FROM
	advisor a
join
	instructor i on a.i_id = i.id
join
	student s on a.s_id = s.id;
"""
student = conn.execute(text(query)).all()

instructor_student_cassandra = []

for stud in student:
    query = f"INSERT INTO instructor_student(s_id, i_id, instructor_name, student_name) VALUES (%s, %s, %s, %s);"
    session.execute(query, (stud[0], stud[1], stud[2], stud[3]))
    instructor_student_cassandra.append(stud)

session_postgres.close()

limpar_tabela('prereq_course')

query = """
select 
    p.prereq_id,
    p.course_id,
    c2.title as title_course,
    c1.title as title_prereq
from
    prereq p
join
    course c1 on p.prereq_id = c1.course_id
join
    course c2 on p.course_id = c2.course_id;
"""
prereq = conn.execute(text(query)).all()

prereq_cassandra = []

for pre in prereq:
    query = f"INSERT INTO prereq_course(prereq_id, course_id, title_course, title_prereq) VALUES (%s, %s, %s, %s);"
    session.execute(query, (pre[0], pre[1], pre[2], pre[3]))
    prereq_cassandra.append(pre)

session_postgres.close()

# === QUESTÕES ===
print('------------------------------------------------------------------')
print("Questão 1 - Listar todos os cursos oferecidos por um determinado departamento\n")
def cursos_por_departamento(dept):
    cursos = session.execute(f"SELECT course_id, title, credits FROM course_department WHERE dept_name = '{dept}';")
    
    # Criar o DataFrame diretamente
    df = pd.DataFrame(cursos, columns=['course_id', 'title', 'credits'])
    
    return df

# Chamando a função e exibindo o DataFrame
df = cursos_por_departamento('Comp. Sci.')
print(df)

print('------------------------------------------------------------------')
print("Questão 2 - Recuperar todas as disciplinas de um curso específico em um determinado semestre\n")
def disciplinas_por_curso_e_semestre(curso, semestre):
    query = f"""
    SELECT
        course_id,
        semester,
        title,
        year
    FROM
        section
    WHERE
        course_id = '{curso}' AND semester = '{semestre}';
    """
    
    disciplinas = session.execute(query)
    
    df = pd.DataFrame(disciplinas, columns=['course_id', 'title', 'semester', 'year'])
    
    return df

df = disciplinas_por_curso_e_semestre('CS-101', 'Fall')
print(df)

print('------------------------------------------------------------------')
print("Questão 3 - Encontrar todos os estudantes que estão matriculados em um curso específico\n")
def estudantes_por_curso(curso):
    query = f"""
    SELECT
        course_id,
        name,
        id,
        tot_cred
    FROM
        students_course
    WHERE
        course_id = '{curso}'
    ALLOW FILTERING;
    """
    
    estudantes = session.execute(query)
    
    df = pd.DataFrame(estudantes, columns=['course_id', 'name', 'id', 'tot_cred'])
    
    return df

df = estudantes_por_curso('CS-101')
print(df)

print('------------------------------------------------------------------')
print("Questão 4 - Listar a média de salários de todos os professores em um determinado departamento\n")
def media_salario_departamento(dept):
    d = 'Comp. Sci.'
    query = f"""
    SELECT
        dept_name,
        salary
    FROM
        students_teaches
    WHERE
        dept_name = '{d}';
    """

    resultado = session.execute(query)

    data = [{'dept_name': c.dept_name, 'salary': int(c.salary)} for c in resultado]

    df = pd.DataFrame(data)
    
    return df

df = media_salario_departamento('Comp. Sci.')
print(f"Média: {df['salary'].mean()}")
print(df)

print('------------------------------------------------------------------')
print("Questão 5 - Recuperar o número total de créditos obtidos por um estudante específico\n")
def total_creditos_por_estudante(nome_estudante):
    query = f"""
    SELECT name, credits
    FROM students_course
    WHERE name = '{nome_estudante}';
    """
    
    resultado = session.execute(query)
    
    dados_lista = [(row[0], int(row[1])) for row in resultado]
    
    df = pd.DataFrame(dados_lista, columns=['nome', 'creditos'])
    
    return df

df = total_creditos_por_estudante('Tanaka')
print(f'Total de creditos: {df["creditos"].sum()}')
print(df)

print('------------------------------------------------------------------')
print("Questão 6 - Encontrar todas as disciplinas ministradas por um professor em um semestre específico\n")
def disciplinas_instrutor_semeste(instructor, semester):
    query = f"""
    SELECT
        id,
        name,
        course_id,
        semester,
        title
    FROM
        instructor_teaches
    WHERE
        name = '{instructor}'
        AND semester = '{semester}'
    ALLOW FILTERING;
    """
    
    instructor = session.execute(query)
    
    df = pd.DataFrame(instructor, columns=['id', 'name', 'course_id', 'semester', 'title'])
    
    return df

df = disciplinas_instrutor_semeste('Srinivasan', 'Fall')
print(df)

print('------------------------------------------------------------------')
print("Questão 7 - Listar todos os estudantes que têm um determinado professor como orientador\n")
def orientador_aluno(instructor):
    query = f"""
    SELECT
        name_instructor,
        name_student
    FROM
        student_advisor
    WHERE
        name_instructor = '{instructor}'
    ALLOW FILTERING;
    """
    
    students = session.execute(query)
    
    df = pd.DataFrame(students, columns=['name_student', 'name_student'])
    
    return df

df = orientador_aluno('Katz')
print(df)

print('------------------------------------------------------------------')
print("Questão 8 - Recuperar todas as salas de aula sem um curso associado\n")
def sala_curso():
    query = f"""select * from classroom_section;"""
    
    students = session.execute(query)
    
    df = pd.DataFrame(students, columns=['building', 'capacity', 'room_number'])
    
    return df

df = sala_curso()
print(df)

print('------------------------------------------------------------------')
print("Questão 9 - Encontrar todos os pré-requisitos de um curso específico\n")
def prerequisitos():
    query = f"""SELECT title_course, title_prereq FROM prereq_course;"""
    
    prereq = session.execute(query)
    
    df = pd.DataFrame(prereq, columns=['title_course', 'title_prereq'])
    
    return df

df = prerequisitos()
df

print('------------------------------------------------------------------')
print("Questão 10 - Recuperar a quantidade de alunos orientados por cada professor\n")
def orientados_professor():
    query = f"""SELECT instructor_name, count(student_name) AS qtd  FROM instructor_student GROUP BY instructor_name;"""
    
    orientados = session.execute(query)
    
    df = pd.DataFrame(orientados, columns=['instructor_name', 'qtd'])
    
    return df

df = orientados_professor()
print(df.sort_values('qtd', ascending=False))