--Use pig WordcountLivro.pig
--Carrega o arquivo separando os registros (line) pelo '.' arquivo no HDFS /user/cloudera/pig/Alice.txt
file = load 'pig/Alice.txt' USING PigStorage('.') as (line);
--Carrega arquivo com conjunto de palavras irrelevantes em ingles
stopwords = load 'pig/stop-word-list.csv' USING PigStorage() as (stopword:chararray);
--Normaliza o texto em lowercase, elimina espacos em branco e pontuacoes, Tokeniza (separa) as palavras e grava os registros (word)
words = foreach file GENERATE FLATTEN(TOKENIZE(REPLACE(LOWER(line), '[^a-z ]', ''))) as (word);
--Elimina palavras nao relevantes menor que 2 caracteres
realwords = FILTER words BY SIZE(word) > 2;
--Tokeniza e grava as palavras do arquivo stopwords
real_stopwords = foreach stopwords GENERATE FLATTEN(TOKENIZE(stopword)) as stopword;
--Right join entre real_stopwords e realwords
right_joined = JOIN real_stopwords BY stopword RIGHT OUTER, realwords BY word;
--Filtro para remover stopwords (lado esquerdo do join)
mean_words = FILTER right_joined BY (real_stopwords::stopword IS NULL);
--Remove o campo duplicado do join anterior
Alice_realwords = foreach mean_words GENERATE realwords::word as word;
--Agrupa palavras iguais
grouped = GROUP Alice_realwords BY word;
--Conta as palavras iguais
counted = foreach grouped GENERATE group as word, COUNT(Alice_realwords) as wordcount;
--Ordena contagem decrescente
ordered = ORDER counted BY wordcount DESC;
--Limita resultados em 1500 palavras
result = LIMIT ordered 1500;
--Armazena resultado no HDFS /user/cloudera/pig/book_1500_words
store result into 'pig/book_1500_words';

