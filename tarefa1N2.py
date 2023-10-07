import uuid
from cassandra.cluster import Cluster

# Conecta ao cluster Cassandra local 
cluster = Cluster(['localhost'])
session = cluster.connect()

# Cria o keyspace e a tabela de tarefas
session.execute("""
    CREATE KEYSPACE IF NOT EXISTS tarefas
    WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}
""")
session.execute("""
    USE tarefas
""")
session.execute("""
    CREATE TABLE IF NOT EXISTS tarefa (
        id UUID PRIMARY KEY,
        titulo TEXT,
        descricao TEXT
    )
""")

# Função para adicionar uma tarefa
def adicionar_tarefa(titulo, descricao):
    tarefa_id = uuid.uuid4()
    session.execute("""
        INSERT INTO tarefa (id, titulo, descricao)
        VALUES (%s, %s, %s)
    """, (tarefa_id, titulo, descricao))
    print("Tarefa adicionada com sucesso!")

# Função para listar todas as tarefas
def listar_tarefas():
    rows = session.execute("SELECT id, titulo FROM tarefa")
    for row in rows:
        print(f"ID: {row.id}, Título: {row.titulo}")

# Função para visualizar a descrição de uma tarefa pelo ID
def visualizar_descricao(id_tarefa):
    row = session.execute("SELECT titulo, descricao FROM tarefa WHERE id=%s", (id_tarefa,))
    if row:
        print(f"Descrição da Tarefa '{row[0].titulo}': {row[0].descricao}")
    else:
        print("Tarefa não encontrada.")

# Função para remover uma tarefa pelo ID
def remover_tarefa(id_tarefa):
    session.execute("DELETE FROM tarefa WHERE id=%s", (id_tarefa,))
    print("Tarefa removida com sucesso!")

# Loop principal
while True:
    print("\nEscolha uma opção:")
    print("1. Adicionar Tarefa")
    print("2. Listar Tarefas")
    print("3. Visualizar Descrição da Tarefa")
    print("4. Remover Tarefa")
    print("5. Sair")

    opcao = input("Digite o número da opção desejada: ")

    if opcao == '1':
        titulo = input("Digite o título da tarefa: ")
        descricao = input("Digite a descrição da tarefa: ")
        adicionar_tarefa(titulo, descricao)
    elif opcao == '2':
        listar_tarefas()
    elif opcao == '3':
        id_tarefa = uuid.UUID(input("Digite o ID da tarefa que deseja visualizar: "))
        visualizar_descricao(id_tarefa)
    elif opcao == '4':
        id_tarefa = uuid.UUID(input("Digite o ID da tarefa que deseja remover: "))
        remover_tarefa(id_tarefa)
    elif opcao == '5':
        break
    else:
        print("Opção inválida. Tente novamente.")
