package main

import (
	"bufio"
	"fmt"
	"math"
	"os"
	"strings"
	"time"

	_ "github.com/lib/pq"
	"gopkg.in/Nhanderu/brdoc.v1"
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
	fmt.Println("Limpando tabelas")
	sqlDeleteBrutos := `DELETE FROM "dados_brutos" where 1=$1`
	_, err = db.Exec(sqlDeleteBrutos, 1)

	sqlDeleteLimpos := `DELETE FROM "dados_limpos" where 1=$1`
	_, err = db.Exec(sqlDeleteLimpos, 1)

	sqlDeleteExcluidos := `DELETE FROM "dados_excluidos" where 1=$1`
	_, err = db.Exec(sqlDeleteExcluidos, 1)

	conteudo, _ := lerTexto("base_teste.txt")
	insereDadosBanco(conteudo)
}

// Funcao que le o conteudo do arquivo e retorna um slice the string com todas as linhas do arquivo
func lerTexto(caminhoDoArquivo string) ([]string, error) {
	fmt.Println("Lendo Arquivo", caminhoDoArquivo)
	arquivo, err := os.Open(caminhoDoArquivo)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}

	var linhas []string
	scanner := bufio.NewScanner(arquivo)
	for scanner.Scan() {
		linhas = append(linhas, scanner.Text())
	}
	defer arquivo.Close()
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
		tx.Rollback()
		fmt.Println(err)
		return
	}

	horaInicio := time.Now().Format("2019-04-23 15:04:05")
	tamanhoTotal := (len(conteudo) - 1)
	for indice, linha := range conteudo {
		if indice > 0 {
			var percentual float64
			percentual = ((float64(indice) / float64(tamanhoTotal)) * 100)

			fmt.Println("Executando...", math.Floor(percentual*100)/100, "%")

			cpfBruto := strings.Trim(linha[0:19], " ")
			privateBruto := strings.Trim(linha[19:31], " ")
			incompletoBruto := strings.Trim(linha[31:43], " ")
			dataUltimaCompraBruto := strings.Trim(linha[43:65], " ")
			ticketMedioBruto := strings.Trim(linha[65:87], " ")
			ticketUltimaCompraBruto := strings.Trim(linha[87:111], " ")
			lojaMaisFrequenteBruto := strings.Trim(linha[111:131], " ")
			lojaUltimaCompraBruto := strings.Trim(linha[131:len(linha)], " ")

			sqlStatement := `
			INSERT INTO dados_brutos (cpf, private, incompleto, data_ultima_compra, ticket_medio, ticket_ultima_compra, loja_mais_frequente, loja_ultima_compra)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`
			_, err = db.Exec(sqlStatement, cpfBruto, privateBruto, incompletoBruto, dataUltimaCompraBruto, ticketMedioBruto, ticketUltimaCompraBruto, lojaMaisFrequenteBruto, lojaUltimaCompraBruto)

			if err != nil {
				fmt.Println(err)
				panic(err)
			}

			cpfValido := brdoc.IsCPF(cpfBruto)
			lojaMaisFrequenteValida := brdoc.IsCNPJ(lojaMaisFrequenteBruto)
			lojaUltimaComraValida := brdoc.IsCNPJ(lojaMaisFrequenteBruto)

			if len(cpfBruto) == 14 && cpfValido && lojaMaisFrequenteValida && lojaUltimaComraValida && ((dataUltimaCompraBruto != "NULL") || (ticketMedioBruto != "NULL") ||
				(ticketUltimaCompraBruto != "NULL") || (lojaMaisFrequenteBruto != "NULL") ||
				(lojaUltimaCompraBruto != "NULL")) {

				cpf := higienizaDados(cpfBruto)
				private := higienizaDados(privateBruto)
				incompleto := higienizaDados(incompletoBruto)
				dataUltimaCompra := higienizaDados(dataUltimaCompraBruto)
				ticketMedio := higienizaDados(ticketMedioBruto)
				ticketUltimaCompra := higienizaDados(ticketUltimaCompraBruto)
				lojaMaisFrequente := higienizaDados(lojaMaisFrequenteBruto)
				lojaUltimaCompra := higienizaDados(lojaUltimaCompraBruto)

				sqlStatement := `
				INSERT INTO dados_limpos (cpf, private, incompleto, data_ultima_compra, ticket_medio, ticket_ultima_compra, loja_mais_frequente, loja_ultima_compra)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`
				_, err = db.Exec(sqlStatement, cpf, private, incompleto, dataUltimaCompra, ticketMedio, ticketUltimaCompra, lojaMaisFrequente, lojaUltimaCompra)

				if err != nil {
					fmt.Println(err)
					panic(err)
				}
			} else {
				sqlStatement := `
				INSERT INTO dados_excluidos (cpf, private, incompleto, data_ultima_compra, ticket_medio, ticket_ultima_compra, loja_mais_frequente, loja_ultima_compra)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`
				_, err = db.Exec(sqlStatement, cpfBruto, privateBruto, incompletoBruto, dataUltimaCompraBruto, ticketMedioBruto, ticketUltimaCompraBruto, lojaMaisFrequenteBruto, lojaUltimaCompraBruto)

				if err != nil {
					fmt.Println(err)
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
	horaFim := time.Now().Format("2019-04-23 15:04:05")
	fmt.Println("Inicio", horaInicio)
	fmt.Println("Fim", horaFim)

	fmt.Println("Registros Brutos", contaDadosBrutos())
	fmt.Println("Registros Limpos", contaDadosLimpos())
	fmt.Println("Registros Excluídos", contaDadosExcluidos())
}

func higienizaDados(dado string) string {
	dadoTratado := converteMaiucula(dado)
	dadoTratado = removeMascarasCpfCnpj(dadoTratado)
	dadoTratado = removeAcentosDados(dadoTratado)
	return dadoTratado
}

func converteMaiucula(dado string) string {
	return strings.ToUpper(dado)
}

func removeAcentosDados(dado string) string {
	rep := strings.NewReplacer("Á", "A", "Â", "A", "Ã", "A", "Ä", "A")
	rep = strings.NewReplacer("É", "E", "È", "E", "Ê", "E")
	rep = strings.NewReplacer("Í", "I", "Ì", "I")
	rep = strings.NewReplacer("Ó", "O", "Ò", "O", "Ô", "O", "Õ", "O", "Ö", "O")
	rep = strings.NewReplacer("Ú", "U", "Ù", "U", "Ü", "U")
	rep = strings.NewReplacer("Ç", "C")
	return rep.Replace(dado)
}

func removeMascarasCpfCnpj(dado string) string {
	r := strings.NewReplacer("/", "", ".", "", "-", "")
	return r.Replace(dado)
}

func contaDadosBrutos() int {
	db, err := conectaBanco()
	if err != nil {
		fmt.Println(err)
		return 0
	}
	sql := `select count(1) from "dados_brutos"`
	countBrutos := 0
	err = db.Get(&countBrutos, sql)
	return countBrutos
}

func contaDadosExcluidos() int {
	db, err := conectaBanco()
	if err != nil {
		fmt.Println(err)
		return 0
	}
	sql := `select count(1) from "dados_excluidos"`
	countExcluidos := 0
	err = db.Get(&countExcluidos, sql)
	return countExcluidos
}

func contaDadosLimpos() int {
	db, err := conectaBanco()
	if err != nil {
		fmt.Println(err)
		return 0
	}
	sql := `select count(1) from "dados_limpos"`
	countLimpos := 0
	err = db.Get(&countLimpos, sql)
	return countLimpos
}
