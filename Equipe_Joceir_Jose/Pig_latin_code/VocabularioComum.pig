--Use pig WordcountLivro.pig
--Diretorio de entrada no HDFS /user/cloudera/pig
--Carrega o arquivo do primeiro livro em txt separando os registros (line) pelo '.' arquivo no HDFS /user/cloudera/pig/Alice.txt
Alice = load 'pig/Alice.txt' USING PigStorage('.') as (line_of_text1);
--Carrega o arquivo do segundo livro em txt separando os registros (line) pelo '.' arquivo no HDFS /user/cloudera/pig/Flatland.txt
Flatland = load 'pig/Flatland.txt' USING PigStorage('.') as (line_of_text2);
--Carrega arquivo com conjunto de palavras irrelevantes em ingles
stopwords = load 'pig/stop-word-list.csv' USING PigStorage() as (stopword:chararray);
--Normaliza o texto em lowercase (livro 1), elimina espacos em branco e pontuacoes, Tokeniza (separa) as palavras e grava os registros (word1)
words1 = foreach Alice GENERATE FLATTEN(TOKENIZE(REPLACE(LOWER(line_of_text1), '[^a-z ]', ''))) as (word1);
--Normaliza o texto em lowercase (livro 2), elimina espacos em branco e pontuacoes, Tokeniza (separa) as palavras e grava os registros (word2)
words2 = foreach Flatland GENERATE FLATTEN(TOKENIZE(REPLACE(LOWER(line_of_text2), '[^a-z ]', ''))) as (word2);
--Elimina palavras nao relevantes menor que 2 caracteres (livro 1)
realwords1 = FILTER words1 BY SIZE(word1) > 2;
--Elimina palavras nao relevantes menor que 2 caracteres (livro 2)
realwords2 = FILTER words2 BY SIZE(word2) > 2;
--Tokeniza e grava as palavras do arquivo stopwords
real_stopwords = foreach stopwords GENERATE FLATTEN(TOKENIZE(stopword)) as stopword;
--Inner join entre livro 1 e livro 2
inner_joined = JOIN realwords1 BY word1, realwords2 BY word2;
--Remove campo duplicado do join anterior mantendo o numero de ocorrencias comuns
common_words = foreach inner_joined GENERATE realwords1::word1 as word;
--Right join entre real_stopwords e common_words
right_joined = JOIN real_stopwords BY stopword RIGHT OUTER, common_words BY word;
--Filtro para remover stopwords (lado esquerdo do join)
mean_words = FILTER right_joined BY (real_stopwords::stopword IS NULL);
--Remove o campo duplicado do join anterior
real_common_words = foreach mean_words GENERATE common_words::word as word;
--Remove palavras duplicadas
no_duplicate = DISTINCT real_common_words;
--Ordena palavras em ordem alfabetica
ordered = ORDER no_duplicate BY word;
--Limita resultados em 1500 palavras
result = LIMIT ordered 1500;
--Armazena resultado no HDFS /user/cloudera/pig/common_1500_words_2_books
store result into 'pig/common_1500_words_2_books';
