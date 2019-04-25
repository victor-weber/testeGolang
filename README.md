# testeGolang
Serviço de leitura de arquivo texto, higienização e persistência dos dados no banco de dados relacional Postgres
------------

# Etapas do Projeto
- Leitura do arquivo, que deve estar no mesmo diretório da aplicação;
- Split dos dados para variáveis novas;
- Gravação dos dados brutos (como estão no arquivo) na tabela "dados_brutos";
- Validação dos dados que estão nas colunas CPF e CNPJ das lojas;
- Validação de colunas que estão com dados nulos;
- Se todos os dados estão corretos, insere na tabela "dados_limpos", fazendo a higienização dos dados (converte para Maiúscula, Remove acentos e retira a máscara dos campos que são CPF e CNPJ);
- Se nas validações há algum problema, insere na tabela "dados_excluidos";

------------

# Instalação/Execução
- Acesso de banco de dados, está configurado no arquivo conexaobanco.go e os dados default são os apontados abaixo:
 * Usuário: postgres
 * Senha: teste
 * Host: localhost
 * Porta: 5432
 * Nome do Banco: teste-postgres
- O Script de criação das tabelas está no arquivo "script_criar_banco.sql"
- Nesse repositório já existe uma aplicação compilada com o nome "importacao-arquivo-golang.exe".
Basta executar essa aplicação com o arquivo "base_teste.txt" no mesmo diretório, que a aplicação já irá processar o mesmo. Após a conclusão do processo, a aplicação fechará a janela automaticamente.
- Para execução utilizando os arquivos fontes do projeto, basta executar o seguinte comando pelo terminal: "go run .\main.go .\conexaobanco.go"