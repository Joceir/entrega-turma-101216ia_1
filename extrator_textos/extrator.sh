#!/bin/bash
#use ./extrator.sh arquivo_de_saida_.txt
#script para extrair textos de todos os capitulos dos livros lit2go.ok
#considera somente o texto do livro excluindo indice e informacoes adicionais antes de cada capitulo
#executar o script dentro do diretorio com o nome do livro

origem=$(pwd)
cont="0"
touch $1

find -name index.html | sort -n -k 2 -t / | sed 's/index.html// g' > capitulos.txt

while read linha; do
	if [ "$cont" -gt 0 ]; then
		cd "$linha"
		cat index.html | sed 's/^ *//' | sed '1,/<\/audio>$/ d' | sed '/^<\/div>/, $ d' | sed 's/<[^>]*>//g ; s/&[^;]*;/ /g' | sed -r '/^[\s\t *]*$/d' >> "$origem/$1"
		cd "$origem"
	fi
	cont=$((cont + 1))
done < capitulos.txt
rm capitulos.txt
