import pandas as pd
from sqlalchemy           import create_engine, text, URL
from sqlalchemy.orm       import sessionmaker
from pymongo.mongo_client import MongoClient
from pymongo.server_api   import ServerApi

# === Configurando e conectando ao MongoDB ===
username = 'massatohc'
password = 'rvNoSo1NgKSAnYZd'

uri = f"mongodb+srv://{username}:rvNoSo1NgKSAnYZd@cluster0.uxhlsxi.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
conn = f"mongodb+srv://{username}:{password}@cluster0.uxhlsxi.mongodb.net/"

# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'))
db = client.college

# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)

# === Funções de utilidade do MongoDB ===
def deletar_dados_mongo(colecao):
	db[colecao].delete_many({})
      

def listar_colunas(collection):
    for c in db[collection].find({}):
        print(c)

# === Alterando Documentos do MongoDB ===
# Limpando os dados das tabelas do mongo
deletar_dados_mongo('course')
deletar_dados_mongo('department')
deletar_dados_mongo('advisor')
deletar_dados_mongo('classroom')
deletar_dados_mongo('instructor')
deletar_dados_mongo('prereq')
deletar_dados_mongo('section')
deletar_dados_mongo('student')
deletar_dados_mongo('takes')
deletar_dados_mongo('teaches')
deletar_dados_mongo('time_slot')

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

query = 'SELECT * FROM public.course;'
cursos = conn.execute(text(query)).all()

cursos_json = []
for curso in cursos:
    cursos_json.append({
        "course_id": curso[0],
        "title": curso[1],
        "dept_name": curso[2],
        "credits": int(curso[3])
    })

session.close()

query = 'SELECT * FROM public.department;'
dept = conn.execute(text(query)).all()

dept_json = []

for d in dept:
    dept_json.append({
        "dept_name": d[0],
        "building": d[1],
        "budget": int(d[2]) if d[2] % 1 == 0 else float(d[2])
    })

session.close()

query = 'SELECT * FROM public.advisor;'
advisor = conn.execute(text(query)).all()

advisor_json = []

for a in advisor:
    advisor_json.append({
        "s_id": a[0],
        "i_id": a[1]
    })

session.close()

query = 'SELECT * FROM public.classroom;'
classrooms = conn.execute(text(query)).all()

classroom_json = []

for room in classrooms:
    classroom_json.append({
        "building": room[0],
        "room_number": room[1],
        "capacity": int(room[2])
    })

session.close()

query = 'SELECT * FROM public.instructor;'
instructors = conn.execute(text(query)).all()

instructor_json = []

for instr in instructors:
    instructor_json.append({
        "id": instr[0],
        "name": instr[1],
        "dept_name": instr[2],
        "salary": int(instr[3]) if instr[3] % 1 == 0 else float(instr[3])
    })

session.close()

query = 'SELECT * FROM public.prereq;'
prereqs = conn.execute(text(query)).all()

prereq_json = []

for req in prereqs:
    prereq_json.append({
        "course_id": req[0],
        "prereq_id": req[1]
    })

session.close()

query = 'SELECT * FROM public.section;'
sections = conn.execute(text(query)).all()

section_json = []

for sec in sections:
    section_json.append({
        "course_id": sec[0],
        "sec_id": sec[1],
        "semester": sec[2],
        "year": int(sec[3]) if sec[3] % 1 == 0 else float(sec[3]),
        "building": sec[4],
        "room_number": sec[5],
        "time_slot_id": sec[6]
    })

session.close()

query = 'SELECT * FROM public.student;'
students = conn.execute(text(query)).all()

student_json = []

for stu in students:
    student_json.append({
        "id": stu[0],
        "name": stu[1],
        "dept_name": stu[2],
        "tot_cred": int(stu[3]) if stu[3] % 1 == 0 else float(stu[3])
    })

session.close()

query = 'SELECT * FROM public.takes;'
takes = conn.execute(text(query)).all()

takes_json = []

for take in takes:
    takes_json.append({
        "id": take[0],
        "course_id": take[1],
        "semester": take[2],
        "sec_id": take[3],
        "year": int(take[4]) if take[4] % 1 == 0 else float(take[4]),
        "grade": take[5]
    })

session.close()

query = 'SELECT * FROM public.teaches;'
teaches = conn.execute(text(query)).all()

teaches_json = []

for teach in teaches:
    teaches_json.append({
        "id": teach[0],
        "course_id": teach[1],
        "semester": teach[2],
        "sec_id": teach[3],
        "year": int(teach[4]) if teach[4] % 1 == 0 else float(teach[4])
    })

session.close()

query = 'SELECT * FROM public.time_slot;'
time_slots = conn.execute(text(query)).all()

time_slot_json = []

for slot in time_slots:
    time_slot_json.append({
        "time_slot_id": slot[0],
        "day": slot[1],
        "start_hr": int(slot[2]) if slot[2] % 1 == 0 else float(slot[2]),
        "start_min": int(slot[3]) if slot[3] % 1 == 0 else float(slot[3]),
        "end_hr": int(slot[4]) if slot[4] % 1 == 0 else float(slot[4]),
        "end_min": int(slot[5]) if slot[5] % 1 == 0 else float(slot[5])
    })

session.close()

# Inserindo os dados do postgres nas tabelas do mongo
db.course.insert_many(cursos_json)
db.department.insert_many(dept_json)
db.advisor.insert_many(advisor_json)
db.classroom.insert_many(classroom_json)
db.instructor.insert_many(instructor_json)
db.prereq.insert_many(prereq_json)
db.section.insert_many(section_json)
db.student.insert_many(student_json)
db.takes.insert_many(takes_json)
db.teaches.insert_many(teaches_json)
db.time_slot.insert_many(time_slot_json)

# === QUESTÕES ===
for collection in db.list_collection_names():
    print(collection)
print('------------------------------------------------------------------')
print("Questão 1 - Listar todos os cursos oferecidos por um determinado departamento\n")
def cursos_por_departamento(dept):
    course_collection = db['course']
    dept_courses = course_collection.find({'dept_name': dept})
    
    courses_data = []
    for course in dept_courses:
        courses_data.append({
            'course_id': course['course_id'],
            'title': course['title'],
            'dept_name': course['dept_name'],
            'credits': course['credits']
        })
    
    df = pd.DataFrame(courses_data)
    return df

df = cursos_por_departamento('Comp. Sci.')
print(df)

print('------------------------------------------------------------------')
print("Questão 2 - Recuperar todas as disciplinas de um curso específico em um determinado semestre\n")

def disciplinas_por_curso_e_semestre(titulo_curso, semestre):
    section_collection = db['section']
    course_collection = db['course']
    
    curso = course_collection.find_one({'title': titulo_curso})
    if not curso:
        print(f'O curso com o título "{titulo_curso}" não foi encontrado.')
        return None
    
    curso_secoes = section_collection.find({
        'course_id': curso['course_id'],
        'semester': semestre
    })
    
    secoes_data = []
    for secao in curso_secoes:
        secoes_data.append({
            'curso_title': titulo_curso,
            'semester': secao['semester'],
            'year': secao['year']
        })
    
    df = pd.DataFrame(secoes_data)
    return df


df = disciplinas_por_curso_e_semestre('Intro. to Computer Science', 'Spring')
print(df)

print('------------------------------------------------------------------')
print('Questão 3 - Encontrar todos os estudantes que estão matriculados em um curso específico\n')
def estudantes_por_curso(titulo_curso):
    course_collection = db['course']
    student_collection = db['student']
    
    curso = course_collection.find_one({'title': titulo_curso})
    if not curso:
        print(f'O curso com o título "{titulo_curso}" não foi encontrado.')
        return None
    
    estudantes = student_collection.find({'dept_name': curso['dept_name']})
    
    estudantes_data = []
    for estudante in estudantes:
        estudantes_data.append({
            'student_id': estudante['id'],
            'name': estudante['name'],
            'dept_name': estudante['dept_name'],
            'tot_cred': estudante['tot_cred']
        })
    
    df = pd.DataFrame(estudantes_data)
    return df

df = estudantes_por_curso('Intro. to Computer Science')
print(df)

print('------------------------------------------------------------------')
print('Questão 4 - Listar a média de salários de todos os professores em um determinado departamento\n')
def media_salarios_por_departamento(dept):
    instructor_collection = db['instructor']
    
    professores_departamento = instructor_collection.find({'dept_name': dept})
    
    salarios = []
    for professor in professores_departamento:
        salarios.append(int(professor['salary']))
    
    if salarios:
        media_salarios = sum(salarios) / len(salarios)
    else:
        media_salarios = None
    
    df = pd.DataFrame({'departamento': [dept], 'salario_medio': [media_salarios]})
    return df


df = media_salarios_por_departamento('Comp. Sci.')
print(df)

print('------------------------------------------------------------------')
print('Questão 5 - Recuperar o número total de créditos obtidos por um estudante específico\n')
def total_creditos_estudante(id_estudante):
    takes_collection = db['takes']
    course_collection = db['course']
    student_collection = db['student']
    
    cursos_estudante = takes_collection.find({'id': id_estudante})
    
    creditos_cursos = []
    for curso in cursos_estudante:
        curso_doc = course_collection.find_one({'course_id': curso['course_id']})
        if curso_doc:
            creditos_cursos.append(int(curso_doc['credits']))
    
    total_creditos = sum(creditos_cursos)
    
    estudante_doc = student_collection.find_one({'id': id_estudante})
    if estudante_doc:
        nome_estudante = estudante_doc['name']
    else:
        nome_estudante = None
    
    df = pd.DataFrame({'student_id': [id_estudante], 'student_name': [nome_estudante], 'tot_credits': [total_creditos]})
    return df

df = total_creditos_estudante('00128')
print(df)

print('------------------------------------------------------------------')
print('Questão 6 - Encontrar todas as disciplinas ministradas por um professor em um semestre específico\n')
def disciplinas_ministradas_por_professor(id_professor, semestre):
    teaches_collection = db['teaches']
    course_collection = db['course']
    instructor_collection = db['instructor']
    
    cursos_professor = teaches_collection.find({'id': id_professor, 'semester': semestre})
    
    professor_info = instructor_collection.find_one({'id': id_professor})
    nome_professor = professor_info['name']
    
    cursos_data = []
    for curso in cursos_professor:
        curso_doc = course_collection.find_one({'course_id': curso['course_id']})
        if curso_doc:
            cursos_data.append({
                'instructor_id': id_professor,
                'instructor_name': nome_professor,
                'course_id': curso_doc['course_id'],
                'title': curso_doc['title'],
                'dept_name': curso_doc['dept_name'],
                'credits': curso_doc['credits'],
                'semester': curso['semester'],
                'year': curso['year']
            })
    
    df = pd.DataFrame(cursos_data)
    return df

df = disciplinas_ministradas_por_professor('10101', '1')
print(df)

print('------------------------------------------------------------------')
print('Questão 7 - Listar todos os estudantes que têm um determinado professor como orientador\n')
def estudantes_com_orientador(id_orientador):
    advisor_collection = db['advisor']
    student_collection = db['student']
    instructor_collection = db['instructor']
    
    estudantes_orientador = advisor_collection.find({'i_id': id_orientador})
    
    orientador_info = instructor_collection.find_one({'id': id_orientador})
    nome_orientador = orientador_info['name']
    
    ids_estudantes = []
    for estudante in estudantes_orientador:
        ids_estudantes.append(estudante['s_id'])
    
    estudantes_data = []
    for id_estudante in ids_estudantes:
        estudante_doc = student_collection.find_one({'id': id_estudante})
        if estudante_doc:
            estudantes_data.append({
                'instructor_id': id_orientador,
                'instructor_name': nome_orientador,
                'student_id': estudante_doc['id'],
                'student_name': estudante_doc['name'],
                'department': estudante_doc['dept_name'],
                'tot_cred': estudante_doc['tot_cred'],
            })
    
    df = pd.DataFrame(estudantes_data)
    return df


df = estudantes_com_orientador('45565')
print(df)

print('------------------------------------------------------------------')
print('Questão 8 - Recuperar todas as salas de aula sem um curso associado\n')
def salas_de_aula_sem_curso_associado():
    classroom_collection = db['classroom']
    section_collection = db['section']
    
    salas_de_aula = classroom_collection.find()
    
    salas_sem_curso = []
    for sala in salas_de_aula:
        secao_associada = section_collection.find_one({'building': sala['building'], 'room_number': sala['room_number']})
        if not secao_associada:
            salas_sem_curso.append({
                'building': sala['building'],
                'room_number': sala['room_number'],
                'capacity': sala['capacity']
            })
    
    df = pd.DataFrame(salas_sem_curso)
    return df

df = salas_de_aula_sem_curso_associado()
print(df)
# Sala vazia inserida à query

print('------------------------------------------------------------------')
print('Questão 9 - Encontrar todos os pré-requisitos de um curso específico\n')
def prerequisitos_do_curso(curso_id):
    prereq_collection = db['prereq']
    course_collection = db['course']
    
    curso = course_collection.find_one({'course_id': curso_id})
    if not curso:
        print(f'O curso com o ID "{curso_id}" não foi encontrado.')
        return None
    nome_curso = curso['title']
    
    prerequisitos = prereq_collection.find({'course_id': curso_id})
    
    ids_prerequisitos = []
    for prereq in prerequisitos:
        ids_prerequisitos.append(prereq['prereq_id'])
    
    prerequisitos_data = []
    for id_prereq in ids_prerequisitos:
        
        prereq_doc = course_collection.find_one({'course_id': id_prereq})
        if prereq_doc:
            prerequisitos_data.append({
                'course_id': curso_id,
                'course_title': nome_curso,
                'prerequisite_course_id': prereq_doc['course_id'],
                'prerequisite_title': prereq_doc['title'],
                'prerequisite_dept': prereq_doc['dept_name'],
                'prerequisite_credits': prereq_doc['credits']
            })

    df = pd.DataFrame(prerequisitos_data)
    return df

df = prerequisitos_do_curso('CS-190')
print(df)

print('------------------------------------------------------------------')
print('Questão 10 - Recuperar a quantidade de alunos orientados por cada professor\n')
def alunos_por_orientador():
    advisor_collection = db['advisor']
    instructor_collection = db['instructor']
    student_collection = db['student']
    
    orientacoes = advisor_collection.find({})
    
    alunos_por_orientador = {}
    for orientacao in orientacoes:
        orientador_id = orientacao['i_id']
        aluno_id = orientacao['s_id']
        if orientador_id in alunos_por_orientador:
            alunos_por_orientador[orientador_id]['qtd_students'] += 1
            alunos_por_orientador[orientador_id]['students'].append(aluno_id)
        else:
            professor = instructor_collection.find_one({'id': orientador_id})
            alunos_por_orientador[orientador_id] = {
                'instructor_name': professor['name'],
                'qtd_students': 1,
                'students': [aluno_id]
            }
    
    orientadores_data = [{'instructor_id': k, **v} for k, v in alunos_por_orientador.items()]
    
    df = pd.DataFrame(orientadores_data)
    
    alunos_nomes = []
    for alunos_lista in df['students']:
        alunos = student_collection.find({'id': {'$in': alunos_lista}})
        alunos_nomes.append(', '.join([aluno['name'] for aluno in alunos]))
    
    df['students_names'] = alunos_nomes
    
    return df

df = alunos_por_orientador()
print(df)