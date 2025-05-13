import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
import re
import time
from tqdm import tqdm
import datetime
import sys
import os
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Numeric, SmallInteger, DateTime, TIMESTAMP
import concurrent.futures
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager

# --- Configurações ---
TABLE_NAME = 'st_precos_emporio_rosa' 
INITIAL_URL_TEMPLATE = "https://www.emporiorosa.com.br/produtos-naturais-1.html?p={page}"
PRODUCT_CONTAINER_SELECTOR = 'section.category-products'
PRODUCT_ITEM_SELECTOR = "li.item"

USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
PAGE_LOAD_DELAY = 3
SCROLL_PAUSE_TIME = 1
MAX_PAGES_TO_SCRAPE = 30
NUM_THREADS = 8

# --- Configuração do Banco de Dados Postgre ---
db_host_env = os.getenv('DB_HOST_PROD')
db_port_env = os.getenv('DB_PORT_PROD')
db_name_env_explicit = os.getenv('DB_NAME_PROD')
db_user_env = os.getenv('DB_USER_PROD')
db_pass_env = os.getenv('DB_PASS_PROD')
db_schema_env = os.getenv('DB_SCHEMA_PROD', 'public')

actual_host = None
actual_dbname = None
engine = None 

if not all([db_host_env, db_port_env, db_user_env, db_pass_env]):
    print("AVISO: Variáveis de ambiente do banco de dados incompletas. O script continuará sem salvar no banco de dados.")
else:
    if '/' in db_host_env:
        actual_host, actual_dbname_from_host = db_host_env.split('/', 1)
        actual_dbname = actual_dbname_from_host if actual_dbname_from_host else db_name_env_explicit
    else:
        actual_host = db_host_env
        actual_dbname = db_name_env_explicit

    if not actual_dbname:
        print("AVISO: Nome do banco de dados não pôde ser determinado. O script continuará sem salvar no banco de dados.")
    else:
        DB_URL = f"postgresql://{db_user_env}:{db_pass_env}@{actual_host}:{db_port_env}/{actual_dbname}"
        print(f"Tentando conectar ao PostgreSQL com URL: postgresql://{db_user_env}:SENHA_OCULTA@{actual_host}:{db_port_env}/{actual_dbname}")
        try:
            engine = create_engine(DB_URL)
            # Testar conexão
            with engine.connect() as connection:
                print(f"Conexão com PostgreSQL ({actual_dbname}) estabelecida com sucesso via SQLAlchemy.")
        except Exception as e:
            print(f"Erro ao criar engine SQLAlchemy ou conectar ao PostgreSQL: {e}")
            engine = None # Garante que engine é None se a conexão falhar

# --- Definição da Estrutura da Tabela com SQLAlchemy ---
metadata = MetaData()

st_precos_emporio_rosa_table = Table(
    TABLE_NAME, metadata,
    Column('id', Integer, primary_key=True, autoincrement=True), # PK
    Column('nome', String(500), nullable=True),
    Column('categoria', String(100), nullable=True),
    Column('preco_de', Numeric(18, 2), nullable=True),
    Column('preco_por', Numeric(18, 2), nullable=True),
    Column('fl_promocao', SmallInteger, nullable=True), 
    Column('medidas', String(100), nullable=True),
    Column('ranking_vendas', Integer, nullable=True),
    Column('id_produto', String(100), nullable=True),
    Column('data_extracao', DateTime, nullable=True), 
    schema=db_schema_env # 
)


def salvar_db_sqlalchemy(df, table_name_to_save, db_engine, schema_to_save='public', if_exists_policy='append'):
    if db_engine is None:
        print("Motor SQLAlchemy não está configurado. Não é possível salvar no banco de dados.")
        return 0
    if df.empty:
        print("DataFrame está vazio. Nada para salvar no banco de dados.")
        return 0
        
    try:
        print(f"\n--- Iniciando salvamento de {len(df)} registros na tabela '{schema_to_save}.{table_name_to_save}' ---")
        df.columns = [col.lower() for col in df.columns]

        df.to_sql(
            name=table_name_to_save, 
            con=db_engine,
            schema=schema_to_save,
            if_exists=if_exists_policy,
            index=False,
            chunksize=1000
        )
        print(f"Salvamento concluído. {len(df)} registros processados para '{schema_to_save}.{table_name_to_save}'.")
        return len(df)
    except Exception as e:
        print(f"Erro ao salvar dados no banco de dados com SQLAlchemy: {e}")
        print("Verifique a estrutura da tabela, tipos de dados e permissões.")
        return 0

def clean_price(price_text):
    if not price_text: return None
    cleaned = re.sub(r'[^\d,.]', '', price_text)
    if ',' in cleaned and '.' in cleaned:
        if cleaned.rfind('.') > cleaned.rfind(','):
             cleaned = cleaned.replace('.', '').replace(',', '.')
        else:
             cleaned = cleaned.replace(',', '')
    elif ',' in cleaned:
        cleaned = cleaned.replace(',', '.')
    elif '.' in cleaned and cleaned.count('.') > 1:
        cleaned = cleaned.replace('.', '')
    try:
        return float(cleaned)
    except (ValueError, TypeError):
        return None

def parse_product_data(product_element_soup):
    data = {
        'nome': None, 'categoria': "Produtos Naturais", 'preco_de': None,
        'preco_por': None, 'fl_promocao': 0, 'medidas': None, 'id_produto': None
    }
    data['id_produto'] = product_element_soup.get('data-product-id')
    name_tag = product_element_soup.select_one('h2.product-name a')
    if name_tag: data['nome'] = name_tag.get('title', name_tag.get_text(strip=True))
    price_box = product_element_soup.select_one('div.price-box')
    if price_box:
        old_price_tag = price_box.select_one('p.old-price span.price')
        if old_price_tag: data['preco_de'] = clean_price(old_price_tag.get_text(strip=True))
        special_price_tag = price_box.select_one('p.special-price span.price')
        if special_price_tag: data['preco_por'] = clean_price(special_price_tag.get_text(strip=True))
        else:
            regular_price_tag = price_box.select_one('span.regular-price span.price')
            if regular_price_tag: data['preco_por'] = clean_price(regular_price_tag.get_text(strip=True))
    if data['preco_de'] is None and data['preco_por'] is not None: data['preco_de'] = data['preco_por']
    if data['preco_de'] is not None and data['preco_por'] is not None and data['preco_por'] < data['preco_de']:
        data['fl_promocao'] = 1
    if data['nome'] and data['preco_por'] is not None: return data
    return None

def process_page(page_num, url_template, container_selector, item_selector, load_delay, scroll_pause, user_agent_string):
    page_products = []
    current_url = url_template.format(page=page_num)
    thread_driver = None
    try:
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--window-size=1280,720")
        chrome_options.add_argument(f"user-agent={user_agent_string}")
        chrome_options.add_argument("--disable-notifications")
        chrome_options.add_argument("--disable-popup-blocking")
        chrome_options.add_argument("--log-level=3")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        service = ChromeService(ChromeDriverManager().install())
        thread_driver = webdriver.Chrome(service=service, options=chrome_options)
        thread_driver.implicitly_wait(5)
        thread_driver.get(current_url)
        WebDriverWait(thread_driver, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, container_selector))
        )
        time.sleep(load_delay)
        try:
            thread_driver.execute_script("window.scrollTo(0, document.body.scrollHeight*0.5);")
            time.sleep(scroll_pause / 2) 
            thread_driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(scroll_pause / 2)
        except Exception: pass
        page_html = thread_driver.page_source
        soup = BeautifulSoup(page_html, 'html.parser')
        product_list_container = soup.select_one(container_selector)
        product_elements_soup = []
        if product_list_container:
            product_elements_soup = product_list_container.select(item_selector)
        for product_el_soup in product_elements_soup:
            product_data = parse_product_data(product_el_soup)
            if product_data: page_products.append(product_data)
        return {'page_num': page_num, 'products': page_products, 'url': current_url}
    except TimeoutException:
        print(f"[Thread {page_num}] Timeout: {current_url}")
        return {'page_num': page_num, 'products': [], 'url': current_url, 'error': 'TimeoutException'}
    except Exception as e:
        print(f"[Thread {page_num}] Erro ({current_url}): {e}")
        return {'page_num': page_num, 'products': [], 'url': current_url, 'error': str(e)}
    finally:
        if thread_driver: thread_driver.quit()

# --- Script Principal ---
if __name__ == "__main__":
    print(f"--- Iniciando script {TABLE_NAME} com {NUM_THREADS} Threads ---")
    script_start_time = time.time()
    if engine:
        try:
            print(f"Verificando/Criando tabela '{db_schema_env}.{TABLE_NAME}' no banco de dados...")
            metadata.create_all(engine)
            print(f"Tabela '{db_schema_env}.{TABLE_NAME}' pronta.")
        except Exception as e:
            print(f"Erro ao tentar criar/verificar a tabela '{db_schema_env}.{TABLE_NAME}': {e}")
            print("O script continuará, mas a inserção de dados pode falhar se a tabela não existir ou tiver estrutura incorreta.")
    else:
        print("AVISO: Engine SQLAlchemy não inicializado. A criação da tabela e a inserção de dados no banco serão ignoradas.")

    try:
        print("Verificando/Instalando ChromeDriver globalmente...")
        ChromeDriverManager().install()
        print("ChromeDriver pronto.")
    except Exception as e:
        print(f"Aviso: Não foi possível pré-instalar o ChromeDriver: {e}")

    pages_to_process = list(range(1, MAX_PAGES_TO_SCRAPE + 1))
    all_page_results_unordered = []

    print(f"\n--- Iniciando scraping de {len(pages_to_process)} páginas com {NUM_THREADS} threads ---")
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        future_to_page = {
            executor.submit(
                process_page, page_num, INITIAL_URL_TEMPLATE, PRODUCT_CONTAINER_SELECTOR, 
                PRODUCT_ITEM_SELECTOR, PAGE_LOAD_DELAY, SCROLL_PAUSE_TIME, USER_AGENT
            ): page_num for page_num in pages_to_process
        }
        for future in tqdm(concurrent.futures.as_completed(future_to_page), total=len(pages_to_process), desc="Progresso Páginas"):
            page_num_completed = future_to_page[future]
            try:
                result = future.result()
                if result:
                    all_page_results_unordered.append(result)
                    if result.get('error'):
                         print(f"Página {result['page_num']} ({result['url']}) teve um erro: {result['error']}")
            except Exception as exc:
                print(f"Página {page_num_completed} gerou uma exceção grave no executor: {exc}")
                all_page_results_unordered.append({'page_num': page_num_completed, 'products': [], 'error': str(exc)})

    print("\n--- Coleta de todas as threads concluída ---")
    all_page_results_ordered = sorted(all_page_results_unordered, key=lambda x: x['page_num'])
    
    all_products_data_list_of_dicts = []
    current_global_rank = 1
    for page_result in all_page_results_ordered:
        if page_result.get('products'):
            for product_data_item in page_result['products']:
                product_data_item['ranking_vendas'] = current_global_rank
                product_data_item['data_extracao'] = datetime.datetime.now() # Adicionado aqui
                all_products_data_list_of_dicts.append(product_data_item)
                current_global_rank += 1
    
    print(f"Total de produtos extraídos para processamento: {len(all_products_data_list_of_dicts)}")
    produtos_inseridos_total = 0

    if all_products_data_list_of_dicts:
        df_products = pd.DataFrame(all_products_data_list_of_dicts)
        
        df_products.columns = [col.lower() for col in df_products.columns]

        expected_cols = [col.name for col in st_precos_emporio_rosa_table.columns if col.name != 'id'] 
        for col_name in expected_cols:
            if col_name not in df_products.columns:
                print(f"AVISO: Coluna '{col_name}' esperada na tabela não encontrada no DataFrame. Será inserida como NULL se permitido pela tabela.")
                df_products[col_name] = None 

        df_products = df_products.reindex(columns=expected_cols)


        if engine:
            produtos_inseridos_total = salvar_db_sqlalchemy(
                df_products, 
                TABLE_NAME, # já está em minúsculas
                engine, 
                schema_to_save=db_schema_env,
                if_exists_policy='append'
            )
        else:
            print("Não foi possível salvar os dados no banco, pois o engine SQLAlchemy não está configurado.")

    else:
        print("Nenhum produto foi extraído para inserção.")

    script_end_time = time.time()
    total_time = script_end_time - script_start_time
    print("\n--- Extração Concluída ---")
    print(f"Tempo total: {total_time:.2f} seg ({total_time/60:.2f} min)")
    pages_attempted = len(pages_to_process)
    successful_pages_with_data = sum(1 for r in all_page_results_ordered if not r.get('error') and r.get('products'))
    pages_with_errors = sum(1 for r in all_page_results_ordered if r.get('error'))
    print(f"Páginas que deveriam ser processadas: {pages_attempted}")
    print(f"Páginas processadas com sucesso e com produtos: {successful_pages_with_data}")
    print(f"Páginas que resultaram em erro durante o scraping: {pages_with_errors}")
    print(f"Total de produtos inseridos na tabela '{db_schema_env}.{TABLE_NAME}': {produtos_inseridos_total}")
    print("--- Fim do script ---")