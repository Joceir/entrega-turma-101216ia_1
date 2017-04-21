--Use pig WordcountLivro.pig
--Carrega o arquivo separando os registros (line) pelo '.' arquivo no HDFS /user/cloudera/pig/Alice.txt
file = load 'pig/Alice.txt' USING PigStorage('.') as (line);
--Transforma todos os caracteres em minusculos
lwfile = foreach file generate LOWER(line) as (line);
--Limpa o texto removendo acentuacao e tabulacoes, cria dois registros {line: chararray,chararray}
clean_data = foreach lwfile generate(line), REPLACE(line, '[^a-z ]', '');
--Regrava o texto limpo em um unico registro
lines = foreach clean_data generate $1 as (line);
--Elimina palavras de um ou dois caracteres (nao relevantes) 
clean_words = foreach lines generate(line), REPLACE(line , '\\b[a-z]{1,2}\\b', '');
--Regrava o texto com palavras de ate 2 eliminadas, cria dois registros {line: chararray,chararray}
clean_lines = foreach clean_words generate $1 as (line);
--Tokeniza (separa) as palavras por espaco e grava os registros (word)
words = foreach clean_lines generate flatten(TOKENIZE(line)) as (word);
--Agrupa palavras iguais
grpd = group words by word;
--Conta as palavras iguais
cntd = foreach grpd generate group, COUNT(words) as qtd;
--Ordena contagem decrescente
ordering = ORDER cntd BY qtd DESC;
--Limita resultados em 1500 palavras
result = LIMIT ordering 1500;
--Armazena resultado no HDFS /user/cloudera/pig/Alice_wc
store result into 'pig/Alice_wc';

