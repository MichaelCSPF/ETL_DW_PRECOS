# ETL_DW_PRECOS

## Descrição do Projeto

Este projeto automatiza a coleta e o processamento de preços de produtos de um site concorrente e disponibiliza, diariamente, um Índice de Concorrência (IC) comparativo entre os preços da concorrência e os nossos.

---

## Estrutura do Projeto

```
ETL_DW_PRECOS/
├── source/
│   └── web_scrapping_page.py  # Script de Web Scraping
├── datewarehouse/
│   ├── models/
│   │   ├── STAGE/          # Models de estágio (raw)
│   │   │   └── stg_emporio_rosas.sql
│   │   └── DM/            # Models de marts (views finais)
│   │       └── IC_VIEW.sql
│   ├── seeds/                # Arquivos seed para dados estáticos
│   ├── dbt_project.yml       # Configuração do projeto DBT
│   └── profiles.yml          # Conexão com o PGAdmin/Postgres
└── README.md
|__ profiles.yml
|__ user.yml
```

---

## 1. Coleta de Dados com Web Scraping

No arquivo `web_scrapping_page.py`, utilizamos:

* **Selenium WebDriver** para navegar dinamicamente pelo site da concorrência.
* **BeautifulSoup (bs4)** para parsear o HTML e extrair os preços e informações dos produtos.
* **Pandas** para estruturar os dados extraídos em um DataFrame.

### Passos:

1. Iniciar o browser com `webdriver_manager` e Selenium.
2. Acessar a página de produtos da concorrência.
3. Iterar sobre os elementos HTML dos produtos e coletar nome, preço e outras informações.
4. Criar um DataFrame pandas com os dados coletados.
5. Exportar ou salvar o DataFrame para uso posterior pelo DBT.

---

## 2. Configuração do DBT e Integração com Postgres

Utilizamos o **DBT** para orquestrar a transformação de dados e facilitar governança e documentação.

### 2.1 profiles.yml

No `profiles.yml`, configuramos a conexão ao Postgres (via PGAdmin):

```yaml
my_project:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      user: seu_usuario
      password: sua_senha
      port: 5432
      dbname: nome_do_banco
      schema: staging
```

### 2.2 dbt\_project.yml

Aqui definimos o nome do projeto, caminhos de models e seeds:

```yaml
name: 'etl_dw_precos'
version: '1.0.0'
config-version: 2

model-paths: ['models']
seed-paths: ['seeds']
```

---

## 3. População e Transformação de Dados

### 3.1 Seeds

* Adicionamos arquivos estáticos dentro de `seeds/` (ex: categorias, mapeamentos) para facilitar a governança.
* Executamos:

  ```bash
  ```

dbt seed

````

### 3.2 Models de Staging
Em `models/staging/competitor_prices.sql`, carregamos o DataFrame exportado pelo web scraping para a tabela `staging.competitor_prices`.

```sql
{{ config(materialized='table') }}
SELECT * FROM external_schema.competitor_prices;
````

### 3.3 Models de Marts (Views)

Criamos uma CTE em `models/marts/ic_view.sql` para calcular o Índice de Concorrência (IC):

```sql
{{ config(materialized='view') }}

WITH competitor AS (
  SELECT product_id,
         price AS competitor_price
  FROM {{ ref('staging__competitor_prices') }}
),
our_prices AS (
  SELECT product_id,
         price AS our_price
  FROM source.our_products
)
SELECT
  c.product_id,
  our_price,
  competitor_price,
  ROUND((competitor_price / our_price) * 100, 2) AS ic_percent
FROM competitor c
JOIN our_prices o USING (product_id);
```

Executamos:

```bash
dbt run
```

para materializar as tabelas e views.

---

## 4. Publicação e Atualização Diária

* O projeto está conectado a um servidor Postgres na Render, onde a view `ic_view` é atualizada diariamente.
* Agendamos uma tarefa (scheduler) para executar:

  1. O script de web scraping (`web_scrapping_page.py`).
  2. `dbt seed` (se necessário).
  3. `dbt run` para atualizar staging e marts.

Isso garante que o **Índice de Concorrência** seja recalculado diariamente com os preços mais recentes da concorrência.

---

## 5. Como Executar Localmente

1. Clone o repositório:

   ```bash
   ```

git clone [https://github.com/MichaelCSPF/ETL\_DW\_PRECOS.git](https://github.com/MichaelCSPF/ETL_DW_PRECOS.git)
cd ETL\_DW\_PRECOS

````
2. Crie e ative o ambiente virtual:
   ```bash
python -m venv .venv
source .venv/bin/activate  # ou .venv\Scripts\activate no Windows
````

3. Instale as dependências:

   ```bash
   ```

pip install -r requirements.txt

````
4. Configure o `profiles.yml` com suas credenciais de Postgres.
5. Execute o web scraping:
   ```bash
python source/web_scrapping_page.py
````

6. Execute o DBT:

   ```bash
   ```

dbt seed
dbt run
dbt test  # opcional: validações

````

---

## 6. Governança e Documentação

- Documentamos cada model no DBT usando doc blocks:
  ```sql
  /**
   * Model: staging/comppetitor_prices
   * Descrição: Carrega preços coletados do site concorrente.
   */
````

* Geramos documentação HTML:

  ```bash
  ```

dbt docs generate
dbt docs serve

```

---

## 7. Contato

Para dúvidas, sugestões ou contribuições, abra uma issue ou entre em contato com Michael CSPF.

```
