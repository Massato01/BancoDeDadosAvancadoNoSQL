| INTEGRANTES                  | RA           |
| ---------------------------- | ------------ |
| Carlos Massato Horibe Chinen | 22.221.010-6 |
| Vinicius Alves Pedro         | 22.221.036-1 |
| Gabriel Nunes Missima        | 22.221.040-3 |
| Matheus T. da Silva Arcanjo  | 22.221.020-5 |

A partir das 10 queries a seguir, estruture o modelo de dados para ser armazenando usando um banco do tipo Document Store:

1. Listar todos os cursos oferecidos por um determinado departamento
2. Recuperar todas as disciplinas de um curso específico em um determinado semestre
3. Encontrar todos os estudantes que estão matriculados em um curso específico
4. Listar a média de salários de todos os professores em um determinado departamento
5. Recuperar o número total de créditos obtidos por um estudante específico
6. Encontrar todas as disciplinas ministradas por um professor em um semestre específico
7. Listar todos os estudantes que têm um determinado professor como orientador
8. Recuperar todas as salas de aula sem um curso associado
9. Encontrar todos os pré-requisitos de um curso específico
10. Recuperar a quantidade de alunos orientados por cada professor

### Config:
### MongoDB
Username: massatohc
Password: rvNoSo1NgKSAnYZd
Link: https://cloud.mongodb.com/v2/662bbf0ac30fd927e877ef86#/clusters/detail/Cluster0

### ElephantSQL
Server:	motty.db.elephantsql.com
User: dcxhxgql
Password: bPlk_dl7Xc4l0WEaPCJIYR4dnO9kGZbz
URL: postgres://dcxhxgql:bPlk_dl7Xc4l0WEaPCJIYR4dnO9kGZbz@motty.db.elephantsql.com/dcxhxgql

---

# Como executar:
* Primeiro executar o código **database/postgres/create_data_postgre.py** para criar os dados no postgre
  * Se o código executar corretamente, a seguinte mensagem será impressa no terminal:
```
CREATE executado com sucesso!
INSERT executada com sucesso!
```
* Após inserir os dados no postgres, execute o notebook **document_store/main.ipynb** ou execute o script **document_store/main.py**

* Os resultados estão em **document_store/resultados.md**
