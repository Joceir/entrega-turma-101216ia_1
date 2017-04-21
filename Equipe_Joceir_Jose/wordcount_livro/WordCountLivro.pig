--Carrega o arquivo separando os registros (line) pelo '.'
file = load 'pig/Alice8.txt' USING PigStorage('.') as (line);
--Transforma todos os caracteres em minusculos
lwfile = foreach file generate LOWER(line) as (line);
--Limpa o texto removendo acentuacao e tabulacoes (e possivel criar mais regras), cria dois registros {line: chararray,chararray}
clean_data = foreach lwfile generate(line), REPLACE(line, '[^a-z ]', '');
--Regrava o texto limpo em um unico registro
lines = foreach clean_data generate $1 as (line); 
--Tokeniza (separa) as palavras por espaco e grava os registros (word)
words = foreach lines generate flatten(TOKENIZE(line)) as (word);
grpd = group words by word;
cntd = foreach grpd generate group, COUNT(words);
store cntd into 'pig/Alice_wc';

