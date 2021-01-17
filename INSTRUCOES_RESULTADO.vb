"Para popular todos os Jsons na tabela, tive que juntá-los.
Utilizei o Python Jupyter Notebook para fazer o (MERGE)"


import json
import glob

read_files = glob.glob("ads/*.json")
output_list = []

for f in read_files:
    with open(f, "rb") as infile:
        output_list.append(json.load(infile))

with open("merged_file.json", "w") as outfile:
    json.dump(output_list, outfile)


"Após criar um único arquivo chamado merged_file.json, migrei ele para dentro do Docker
container kzas-db: "

docker cp 'merged_file.json' kzas-db:/tmp/

"Migrei também o buildings.csv:"

docker cp 'buildings.csv' kzas-db:/tmp/


"Para trazer o arquivo Json para o Postgres
primeiramente criei uma tabela chamada 'novo' e importei o arquivo merged_file.json:"

CREATE TABLE novo ( conteudo JSONB );

COPY novo (
    conteudo
)

FROM '/tmp/merged_file.json';

"Nesta etapa, analisei os primeiros 500 caracteres da tabala novo"

SELECT SUBSTRING(JSONB_PRETTY(conteudo),1,500)
  FROM novo;

"Após isto, criei uma tabela com as mesmas colunas do arquivo Json chamada 'ads'"

CREATE TABLE ads (
    idno varchar(30),
    property_type varchar(250),
    city_name varchar(250),
    state char (2),
    street varchar(100),
    sale_price varchar (20),
    neighborhood varchar(200),
    built_area_min varchar(20),
    bedrooms_min varchar(20),
    bathrooms_min varchar(20),
    parking_space_min varchar(30),
    lat double precision (20),
    lon double precision (20),
    street_number varchar(30)
);


"Nesta etapa, fiz a inserção dos dados para dentro da tabela 'ads' 
com os dados da tabela 'novo'."


INSERT INTO ads
SELECT (e -> 'idno')::varchar,
    (e -> 'property_type')::varchar,
    (e ->'city_name')::varchar,
    (e ->'state')::char,
    (e ->'street')::varchar,
    (e ->'sale_price')::varchar,
    (e ->'neighborhood')::varchar,
    (e ->'built_area_min')::varchar,
    (e ->'bedrooms_min')::varchar,
    (e ->'bathrooms_min')::varchar,
    (e ->'parking_space_min')::varchar,
    (e ->'lat')::varchar,
    (e ->'lon')::varchar,
    (e ->'street_number')::varchar
  FROM (SELECT jsonb_array_elements(conteudo) AS e
        FROM novo) AS tabela;


"Nesta etapa retirei as aspas das colunas street e street_number."

UPDATE 
    ads
SET 
    street = REPLACE (street, '"', '');

UPDATE 
    ads
SET 
    street_number = REPLACE (street_number, '"', '');



"Criando a tabela buildings com os mesmos nomes das colunas do buildings.csv"


CREATE TABLE buildings (
id int primary key,
address varchar (150),
address_number varchar(100),
neighborhood varchar(150),
city varchar(100)),
state varchar(20),
cep varchar(100),
latitude double precision,
longitude double precision
)

"Copiando os dados para dentro da tabela."

COPY buildings
(
    id, 
    address,
    address_number,
    neighborhood,
    city,
    state,
    cep,
    latitude,
    longitude
)
FROM '/tmp/buildings.csv'
DELIMITER ','
CSV HEADER;

"Deletando a tabela novo para deixar nossa Base de Dados mais limpa."

DROP TABLE novo;

"Agora que eu estava com as duas tabelas prontas, para identificar a cada anúncio, a qual construção ele pertence 
eu analisei oque as tabelas tinham em comum.. O endereço.
Então eu fiz um left join entre as colunas addres e street, para assim trazer a cada anúncio a sua respectiva
construção e o endereço ao qual pertence."

select ads.idno as id_anuncio, buildings.id as id_construcao,
ads.street as endereco, ads.street_number as numero, 
ads.property_type as tipo_Anuncio
from ads
left join buildings on (address = street);


"Extrai o resultado graficamente no DBeaver e criei uma nova tabela chamada resultado."


CREATE TABLE resultado(
    id_result serial primary key,
    id_anuncio varchar (10),
    id_construcao int,
    endereco varchar (150),
    numero varchar (30),
    tipo_Anuncio varchar(100)
);


"Tive que alterar o type do id_anuncio de 10 para 20, tem ids que são mais do que 10 caracteres"

alter table resultado
alter column id_anuncio type varchar(20);

"E por fim, fiz a ingestão dos dados que o Cliente quer."

COPY resultado
(
    id_anuncio,
    id_construcao,
    endereco,
    numero,
    tipo_Anuncio
)
FROM '/tmp/resultado.csv'
DELIMITER ','
CSV HEADER;




