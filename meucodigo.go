package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"time"

	_ "github.com/lib/pq"
)

func main() {

	db, err := conectaBanco()
	if err != nil {
		fmt.Println(err)
		return
	}

	/*******************************************************
	** Limpa o banco para depois inserir novos registros
	*******************************************************/
	sqlDeleteBrutos := `DELETE FROM "dados_brutos" where 1=$1`
	_, err = db.Exec(sqlDeleteBrutos, 1)

	sqlDeleteLimpos := `DELETE FROM "dados_limpos" where 1=$1`
	_, err = db.Exec(sqlDeleteLimpos, 1)

	sqlDeleteExcluidos := `DELETE FROM "dados_excluidos" where 1=$1`
	_, err = db.Exec(sqlDeleteExcluidos, 1)

	conteudo, _ := lerTexto("base_teste_leve.txt")
	insereDadosBanco(conteudo)
}

// Funcao que le o conteudo do arquivo e retorna um slice the string com todas as linhas do arquivo
func lerTexto(caminhoDoArquivo string) ([]string, error) {
	// Abre o arquivo
	arquivo, err := os.Open(caminhoDoArquivo)
	// Caso tenha encontrado algum erro ao tentar abrir o arquivo retorne o erro encontrado
	if err != nil {
		return nil, err
	}
	// Garante que o arquivo sera fechado apos o uso
	defer arquivo.Close()

	// Cria um scanner que le cada linha do arquivo
	var linhas []string
	scanner := bufio.NewScanner(arquivo)
	for scanner.Scan() {
		linhas = append(linhas, scanner.Text())
	}

	// Retorna as linhas lidas e um erro se ocorrer algum erro no scanner
	return linhas, scanner.Err()
}

func insereDadosBanco(conteudo []string) {
	db, err := conectaBanco()
	if err != nil {
		fmt.Println(err)
		return
	}
	tx, err := db.Begin()
	if err != nil {
		fmt.Println(err)
		return
	}
	currentTimeInicio := time.Now()
	horaInicio := currentTimeInicio.Format("2006-01-02 15:04:05")

	for indice, linha := range conteudo {
		if indice > 0 {
			//fmt.Println("Linha:", linha)

			cpfBruto := strings.Trim(linha[0:19], " ")
			privateBruto := strings.Trim(linha[19:31], " ")
			incompletoBruto := strings.Trim(linha[31:43], " ")
			dataUltimaCompraBruto := strings.Trim(linha[43:65], " ")
			ticketMedioBruto := strings.Trim(linha[65:87], " ")
			ticketUltimaCompraBruto := strings.Trim(linha[87:111], " ")
			lojaMaisFrequenteBruto := strings.Trim(linha[111:131], " ")
			lojaUltimaCompraBruto := strings.Trim(linha[131:len(linha)], " ")

			sqlStatement := `
			INSERT INTO dados_brutos (id,cpf, private, incompleto, data_ultima_compra, ticket_medio, ticket_ultima_compra, loja_mais_frequente, loja_ultima_compra)
			VALUES (nextval('dados_brutos_sq'),$1, $2, $3, $4, $5, $6, $7, $8)`
			_, err = db.Exec(sqlStatement, cpfBruto, privateBruto, incompletoBruto, dataUltimaCompraBruto, ticketMedioBruto, ticketUltimaCompraBruto, lojaMaisFrequenteBruto, lojaUltimaCompraBruto)

			if err != nil {
				panic(err)
			}

			fmt.Println(indice)

			if len(cpfBruto) == 14 && ((dataUltimaCompraBruto != "NULL") || (ticketMedioBruto != "NULL") ||
				(ticketUltimaCompraBruto != "NULL") || (lojaMaisFrequenteBruto != "NULL") ||
				(lojaUltimaCompraBruto != "NULL")) {

				//fmt.Println("Eita", len(cpfBruto))

				cpf := removeMascarasCpfCnpj(strings.ToUpper(cpfBruto))
				private := strings.ToUpper(privateBruto)
				incompleto := strings.ToUpper(incompletoBruto)
				dataUltimaCompra := strings.ToUpper(dataUltimaCompraBruto)
				ticketMedio := strings.ToUpper(ticketMedioBruto)
				ticketUltimaCompra := strings.ToUpper(ticketUltimaCompraBruto)
				lojaMaisFrequente := removeMascarasCpfCnpj(strings.ToUpper(lojaMaisFrequenteBruto))
				lojaUltimaCompra := removeMascarasCpfCnpj(strings.ToUpper(lojaUltimaCompraBruto))

				sqlStatement := `
				INSERT INTO dados_limpos (id,cpf, private, incompleto, data_ultima_compra, ticket_medio, ticket_ultima_compra, loja_mais_frequente, loja_ultima_compra)
				VALUES (nextval('dados_limpos_sq'),$1, $2, $3, $4, $5, $6, $7, $8)`
				_, err = db.Exec(sqlStatement, cpf, private, incompleto, dataUltimaCompra, ticketMedio, ticketUltimaCompra, lojaMaisFrequente, lojaUltimaCompra)

				if err != nil {
					panic(err)
				}
			} else {
				sqlStatement := `
				INSERT INTO dados_excluidos (id,cpf, private, incompleto, data_ultima_compra, ticket_medio, ticket_ultima_compra, loja_mais_frequente, loja_ultima_compra)
				VALUES (nextval('dados_excluidos_sq'),$1, $2, $3, $4, $5, $6, $7, $8)`
				_, err = db.Exec(sqlStatement, cpfBruto, privateBruto, incompletoBruto, dataUltimaCompraBruto, ticketMedioBruto, ticketUltimaCompraBruto, lojaMaisFrequenteBruto, lojaUltimaCompraBruto)

				if err != nil {
					panic(err)
				}
			}

		}
	}
	err = tx.Commit()
	if err != nil {
		fmt.Println(err)
		return
	}
	currentTimeFim := time.Now()
	horaFim := currentTimeFim.Format("2006-01-02 15:04:05")
	fmt.Println("Inicio", horaInicio)
	fmt.Println("Fim", horaFim)

	sql := `select count(1) from "dados_brutos"`
	count := 0
	err = db.Get(&count, sql)

	if err != nil {
		fmt.Println("Erro no Select", err)
	} else {
		fmt.Println("Registros Inseridos", count)
	}
}

func removeAcentosDados(value string) string {
	// Create replacer with pairs as arguments.
	rep := strings.NewReplacer("Á", "A", "é", "e", "ô", "o")
	// Replace all pairs.
	return rep.Replace(value)
}

func removeMascarasCpfCnpj(value string) string {
	r := strings.NewReplacer("/", "", ".", "", "-", "")
	// Replace all pairs.
	return r.Replace(value)
}
