import pandas as pd
from neo4j import GraphDatabase
from sqlalchemy           import create_engine, text
from sqlalchemy.orm       import sessionmaker
from sqlalchemy           import URL

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
session = Session()
conn = engine.connect()

# Driver NEO4J
driver = GraphDatabase.driver(uri = "bolt://localhost:7687", auth=("neo4j", "password"))

def deleteDataNeo4J():
	try:
		driver.execute_query("MATCH (s:department) DETACH DELETE s;")
		driver.execute_query("MATCH (s:classroom) DETACH DELETE s;")
		driver.execute_query("MATCH (s:prereq) DETACH DELETE s;")
		driver.execute_query("MATCH (s:course) DETACH DELETE s;")
		driver.execute_query("MATCH (s:takes) DETACH DELETE s;")
		driver.execute_query("MATCH (s:time_slot) DETACH DELETE s;")
		driver.execute_query("MATCH (s:teaches) DETACH DELETE s;")
		driver.execute_query("MATCH (s:advisor) DETACH DELETE s;")
		driver.execute_query("MATCH (s:section) DETACH DELETE s;")
		driver.execute_query("MATCH (i:instructor) DETACH DELETE i;")
		driver.execute_query("MATCH (s:student) DETACH DELETE s;")

	except Exception as e:
		print(f"Deu errado: {e}")
		
deleteDataNeo4J()

query = 'SELECT * FROM public.instructor;'
instructors = conn.execute(text(query)).all()
session.close()

instructors_list = instructors

instructors_list_commands = []

for instructor in instructors_list:
    neo4j_create_statemenet = "CREATE (:instructor { id: '" + instructor[0] + "', name: '" + instructor[1] + "', dept_name: '" + instructor[2] + "', salary: " + str(instructor[3]) + " });"
    instructors_list_commands.append(neo4j_create_statemenet)

session = driver.session()
for i in instructors_list_commands:
    session.run(i)
	
query = 'SELECT * FROM public.student;'
students = conn.execute(text(query)).all()
session.close()

student_list = students

student_list_commands = []

for stud in student_list:
    neo4j_create_statemenet = "CREATE (:student { id: '" + stud[0] + "', name: '" + stud[1] + "', dept_name: '" + stud[2] + "', tot_cred: " + str(stud[3]) + " });"
    student_list_commands.append(neo4j_create_statemenet)

session = driver.session()
for i in student_list_commands:
    session.run(i)

query = 'SELECT * FROM public.department;'
departments = conn.execute(text(query)).all()
session.close()

department_list = departments

department_list_commands = []

for dept in department_list:
    neo4j_create_statemenet = "CREATE (:department { dept_name: '" + dept[0] + "', building: '" + dept[1] + "', budget: " + str(dept[2]) + " });"
    department_list_commands.append(neo4j_create_statemenet)

session = driver.session()
for i in department_list_commands:
    session.run(i)

query = 'SELECT * FROM public.classroom;'
classrooms = conn.execute(text(query)).all()
session.close()

classroom_list = classrooms

classroom_list_commands = []

for classroom in classroom_list:
    neo4j_create_statemenet = "CREATE (:classroom { building: '" + classroom[0] + "', room_number: " + str(classroom[1]) + ", capacity: " + str(classroom[2]) + " });"
    classroom_list_commands.append(neo4j_create_statemenet)

session = driver.session()
for i in classroom_list_commands:
    session.run(i)

query = 'SELECT * FROM public.prereq;'
prereqs = conn.execute(text(query)).all()
session.close()

prereq_list = prereqs

prereq_list_commands = []

for prereq in prereq_list:
    neo4j_create_statemenet = "MATCH (c1:course { course_id: '" + prereq[0] + "' }) MATCH (c2:course { course_id: '" + prereq[1] + "' }) CREATE (c1)-[:PREREQ]->(c2);"
    prereq_list_commands.append(neo4j_create_statemenet)

session = driver.session()
for i in prereq_list_commands:
    session.run(i)

query = 'SELECT * FROM public.advisor;'
advisors = conn.execute(text(query)).all()
session.close()

advisor_list = advisors

advisor_list_commands = []

for advisor in advisor_list:
    neo4j_create_statemenet = "MATCH (s:student { id: '" + advisor[0] + "' }) MATCH (i:instructor { id: '" + advisor[1] + "' }) CREATE (i)-[:ADVISOR]->(s);"
    advisor_list_commands.append(neo4j_create_statemenet)

session = driver.session()
for i in advisor_list_commands:
    session.run(i)

query = 'SELECT * FROM public.teaches;'
teaches = conn.execute(text(query)).all()
session.close()

teach_list = teaches

teach_list_commands = []

for teach in teach_list:
    neo4j_create_statemenet = "MATCH (i:instructor { id: '" + str(teach[0]) + "' }), (s:section { sec_id: " + str(teach[2]) + ", course_id: '" + teach[1] + "', semester: '" + teach[3] + "', year: " + str(teach[4]) + " }) CREATE (i)-[:TEACHES]->(s);"
    teach_list_commands.append(neo4j_create_statemenet)

session = driver.session()
for i in teach_list_commands:
    session.run(i)

query = 'SELECT * FROM public.section;'
sections = conn.execute(text(query)).all()
session.close()

section_list = sections

section_list_commands = []

for teach in section_list:
    neo4j_create_statemenet = "CREATE (:section { course_id: '" + teach[0] + "', sec_id: " + str(teach[1]) + ", semester: '" + teach[2] + "', year: " + str(teach[3]) + ", building: '" + teach[4] + "', room_number: " + teach[5] + ", time_slot_id: '" + teach[6] + "' });"
    section_list_commands.append(neo4j_create_statemenet) 

session = driver.session()
for i in section_list_commands:
    session.run(i)

query = 'SELECT * FROM public.course;'
courses = conn.execute(text(query)).all()
session.close()

course_list = courses

course_list_commands = []

for course in course_list:
    neo4j_create_statemenet = "CREATE (:course { course_id: '" + course[0] + "', title: '" + course[1] + "', dept_name: '" + course[2] + "', credits: " + str(course[3]) + " });"
    course_list_commands.append(neo4j_create_statemenet) 

session = driver.session()
for i in course_list_commands:
    session.run(i)

query = 'SELECT * FROM public.takes;'
takes = conn.execute(text(query)).all()
session.close()

take_list = takes

take_list_commands = []

for take in take_list:
    student_id = str(take[0]) if take[0] is not None else ''
    course_id = take[1] if take[1] is not None else ''
    sec_id = str(take[2]) if take[2] is not None else ''
    semester = take[3] if take[3] is not None else ''
    year = str(take[4]) if take[4] is not None else ''
    grade = take[5] if take[5] is not None else ''

    neo4j_create_statement = (
        f"MATCH (s:student {{ id: '{student_id}' }}), "
        f"(sec:section {{ sec_id: {sec_id}, course_id: '{course_id}', semester: '{semester}', year: {year} }}) "
        f"CREATE (s)-[:TAKES {{ grade: '{grade}' }}]->(sec);"
    )
    take_list_commands.append(neo4j_create_statement)

session = driver.session()
for command in take_list_commands:
    session.run(command)
session.close()

query = 'SELECT * FROM public.time_slot;'
time_slots = conn.execute(text(query)).all()
session.close()

time_slot_list = time_slots

time_slot_list_commands = []

for time_slot in time_slot_list:
    time_slot_id = str(time_slot[0]) if time_slot[0] is not None else ''
    day = time_slot[1] if time_slot[1] is not None else ''
    start_hr = str(time_slot[2]) if time_slot[2] is not None else '0'
    start_min = str(time_slot[3]) if time_slot[3] is not None else '0'
    end_hr = str(time_slot[4]) if time_slot[4] is not None else '0'
    end_min = str(time_slot[5]) if time_slot[5] is not None else '0'

    neo4j_create_statement = (
        f"CREATE (:time_slot {{ time_slot_id: '{time_slot_id}', day: '{day}', start_hr: {start_hr}, start_min: {start_min}, "
        f"end_hr: {end_hr}, end_min: {end_min} }});"
    )
    time_slot_list_commands.append(neo4j_create_statement)

session = driver.session()
for command in time_slot_list_commands:
    session.run(command)
session.close()

# === QUESTÕES ===

print("#### Questão 1 - Listar todos os cursos oferecidos por um determinado departamento")
def cursos_deparamento(dept_name):
    # Cria a query
    query = f"MATCH (d:department {{ dept_name: '{dept_name}' }}) WITH d MATCH (c:course) RETURN d, c;"
    result, _, _= driver.execute_query(query)
    
    # Lista para armazenar os dados
    data = []

    # Processa o resultado
    for item in result:
        dept = item['d']['dept_name']
        course_id = item['c']['course_id']
        title = item['c']['title']
        data.append({'Departamento': dept, 'Curso ID': course_id, 'Título': title})

    # Cria um dataframe a partir da lista de dados
    df = pd.DataFrame(data)
    
    return df

# Exemplo de uso
df_resultado = cursos_deparamento('Comp. Sci.')
print(df_resultado)

print('#### Questão 2 - Recuperar todas as disciplinas de um curso específico em um determinado semestre')
def disciplina_cursos(course, semester):
    # Cria a query
    query = f'MATCH (s:section {{ course_id: "{course}", semester: "{semester}" }}) RETURN s;'
    result, _, _ = driver.execute_query(query)
    
    # Lista para armazenar os dados
    data = []

    # Processa o resultado
    for item in result:
        course_id = item['s']['course_id']
        semester = item['s']['semester']
        year = item['s']['year']
        building = item['s']['building']
        data.append({'Curso ID': course_id, 'Semestre': semester, 'Ano': year, 'Prédio': building})

    # Cria um dataframe a partir da lista de dados
    df = pd.DataFrame(data)
    
    return df

# Exemplo de uso
df_resultado = disciplina_cursos("CS-101", "Spring")
print(df_resultado)

print('#### Questão 3 - Encontrar todos os estudantes que estão matriculados em um curso específico')
def estudantes_curso(course):
    # Cria a query
    query = f"MATCH t=()-[:TAKES]->({{ course_id: '{course}' }}) RETURN t;"
    result, summary, keys = driver.execute_query(query)
    
    # Lista para armazenar os dados
    data = []

    # Processa o resultado
    for item in result:
        # Supondo que o relacionamento :TAKES conecte um nó de estudante a um nó de curso
        relationship = item['t'].relationships[0]
        student_node = item['t'].start_node
        course_node = item['t'].end_node
        student_id = student_node['id']
        course_id = course_node['course_id']
        data.append({'Estudante ID': student_id, 'Curso ID': course_id})

    # Cria um dataframe a partir da lista de dados
    df = pd.DataFrame(data)
    
    return df

# Exemplo de uso
df_resultado = estudantes_curso("CS-101")
print(df_resultado)

print('#### Questão 4 - Listar a média de salários de todos os professores em um determinado departamento')
def media_salario(department_name):
    # Cria a query
    query = f"MATCH (s:instructor) WHERE s.dept_name = '{department_name}' RETURN avg(s.salary) as media_salarial;"
    
    # Executa a query
    result, _, _ = driver.execute_query(query)
    
    # Lista para armazenar os dados
    data = []

    # Processa o resultado
    for item in result:
        media_salarial = item['media_salarial']
        data.append({'Departamento': department_name, 'Média Salarial': media_salarial})

    # Cria um dataframe a partir da lista de dados
    df = pd.DataFrame(data)
    
    return df

# Exemplo de uso
df_resultado = media_salario("Comp. Sci.")
print(df_resultado)

print('#### Questão 5 - Recuperar o número total de créditos obtidos por um estudante específico')
import pandas as pd
from neo4j import GraphDatabase

# Função para buscar dados e retornar um dataframe
def credito_estudante(student_name):
    # Cria a query
    query = f"MATCH (s:student) WHERE s.name = '{student_name}' RETURN s;"
    
    # Executa a query
    result, summary, keys = driver.execute_query(query)
    
    # Lista para armazenar os dados
    data = []

    # Processa o resultado
    for item in result:
        student_name = item['s']['name']
        total_cred = item['s']['tot_cred']
        data.append({'Nome do Estudante': student_name, 'Total de Créditos': total_cred})

    # Cria um dataframe a partir da lista de dados
    df = pd.DataFrame(data)
    
    return df

# Exemplo de uso
df_resultado = credito_estudante("Zhang")
print(df_resultado)

print('#### Questão 7 - Listar todos os estudantes que têm um determinado professor como orientador')
def estudantes_orientados_professor(instructor_name):
    # Cria a query
    query = f"MATCH p=({{name: '{instructor_name}'}})-[:ADVISOR]->() RETURN p;"
    result, summary, keys = driver.execute_query(query)
    
    # Lista para armazenar os dados
    data = []

    # Processa o resultado
    for item in result:
        # Supondo que o relacionamento :ADVISOR conecte um nó de instrutor a um nó de estudante
        relationship = item['p'].relationships[0]
        instructor_node = item['p'].start_node
        student_node = item['p'].end_node
        instructor_name = instructor_node['name']
        student_name = student_node['name']
        data.append({'Nome do Instrutor': instructor_name, 'Nome do Estudante': student_name})

    # Cria um dataframe a partir da lista de dados
    df = pd.DataFrame(data)
    
    return df

# Exemplo de uso
df_resultado = estudantes_orientados_professor('Katz')
print(df_resultado)

print('#### Questão 10 - Recuperar a quantidade de alunos orientados por cada professor')
import pandas as pd
from neo4j import GraphDatabase

# Função para buscar dados e retornar um dataframe
def orientandos(instructor):
    # Cria a query
    query = f"MATCH p=({{ name: '{instructor}' }})-[:ADVISOR]->() RETURN p;"
    result, summary, keys = driver.execute_query(query)
    
    # Lista para armazenar os dados
    data = []

    # Processa o resultado
    for item in result:
        # Supondo que o relacionamento :ADVISOR conecte um nó de instrutor a um nó de estudante
        relationship = item['p'].relationships[0]
        instructor_node = item['p'].start_node
        student_node = item['p'].end_node
        instructor_name = instructor_node['name']
        student_name = student_node['name']
        data.append({'Nome do Instrutor': instructor_name, 'Nome do Estudante': student_name})

    # Cria um dataframe a partir da lista de dados
    df = pd.DataFrame(data)
    
    return df

# Exemplo de uso
df_resultado = orientandos('Katz')
print(df_resultado)