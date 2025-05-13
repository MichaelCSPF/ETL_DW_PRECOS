import requests
from bs4 import BeautifulSoup
import pyodbc
import json
import re
import time
from tqdm import tqdm
import datetime
import sys
import os
import concurrent.futures # Adicionado

# --- Selenium Imports ---
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager

# --- Configurações ---
CREDENTIALS_PATH = r'C:\Users\Michael\DW\DATA_MART_VENDAS\ETL_PYTHON\credencial_banco_st.json'
TABLE_NAME = 'ST_PRECOS_EMPORIO_ROSA'
INITIAL_URL_TEMPLATE = "https://www.emporiorosa.com.br/produtos-naturais-1.html?p={page}" # Template da URL
PRODUCT_CONTAINER_SELECTOR = 'section.category-products'
PRODUCT_ITEM_SELECTOR = "li.item"

USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
PAGE_LOAD_DELAY = 5 # Reduzido, já que WebDriverWait espera
SCROLL_PAUSE_TIME = 1 # Reduzido, ajustar conforme necessidade de lazy-loading
MAX_PAGES_TO_SCRAPE = 30
NUM_THREADS = 8 # Número de Threads

# --- Funções Auxiliares (DB, Limpeza de Preço, Parse, Insert) ---
# load_db_config, connect_db, clean_price, insert_data_db_executemany permanecem IGUAIS ao seu último código.
# Apenas garanta que estão definidas antes de serem usadas.

def load_db_config(path):
    print(f"Tentando carregar credenciais de: {path}")
    try:
        normalized_path = os.path.normpath(path)
        with open(normalized_path, 'r') as f: config = json.load(f)
        print(f"Credenciais carregadas com sucesso de: {normalized_path}")
        if not all(k in config for k in ["server", "database", "driver"]):
            print("Erro: JSON deve conter 'server', 'database', 'driver'.")
            return None
        return config
    except FileNotFoundError:
        print(f"Erro Crítico: Credenciais não encontradas em '{os.path.normpath(path)}'")
        return None
    except json.JSONDecodeError:
        print(f"Erro Crítico: Falha ao decodificar JSON em '{os.path.normpath(path)}'.")
        return None
    except Exception as e:
        print(f"Erro Crítico ao carregar credenciais: {e}")
        return None

def connect_db(config):
    if not config: return None
    print(f"Tentando conectar ao DB: Server={config['server']}, Database={config['database']}, Driver={config['driver']}")
    try:
        conn_str = f"Driver={config['driver']};Server={config['server']};Database={config['database']};Trusted_Connection=yes;"
        conn = pyodbc.connect(conn_str, timeout=15, autocommit=False)
        print(f"Conectado ao DB '{config['database']}' em '{config['server']}' com sucesso.")
        return conn
    except pyodbc.Error as ex:
        sqlstate = ex.args[0]
        print(f"Erro Crítico Conexão DB (SQLSTATE: {sqlstate}): {ex}")
        return None
    except Exception as e:
        print(f"Erro inesperado conexão DB: {e}")
        return None

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

def parse_product_data(product_element_soup): # Argumento 'rank' removido
    data = {
        'nome': None,
        'categoria': "Produtos Naturais",
        'preco_de': None,
        'preco_por': None,
        'fl_promocao': 0,
        'medidas': None,
        # 'ranking_vendas': rank, # Removido daqui, será adicionado após a coleta de todos os dados
        'id_produto': None
    }
    data['id_produto'] = product_element_soup.get('data-product-id')
    name_tag = product_element_soup.select_one('h2.product-name a')
    if name_tag:
        data['nome'] = name_tag.get('title', name_tag.get_text(strip=True))

    price_box = product_element_soup.select_one('div.price-box')
    if price_box:
        old_price_tag = price_box.select_one('p.old-price span.price')
        if old_price_tag:
            data['preco_de'] = clean_price(old_price_tag.get_text(strip=True))
        
        special_price_tag = price_box.select_one('p.special-price span.price')
        if special_price_tag:
            data['preco_por'] = clean_price(special_price_tag.get_text(strip=True))
        else:
            regular_price_tag = price_box.select_one('span.regular-price span.price')
            if regular_price_tag:
                data['preco_por'] = clean_price(regular_price_tag.get_text(strip=True))
    
    if data['preco_de'] is None and data['preco_por'] is not None:
        data['preco_de'] = data['preco_por']

    if data['preco_de'] is not None and data['preco_por'] is not None and data['preco_por'] < data['preco_de']:
        data['fl_promocao'] = 1
    
    if data['nome'] and data['preco_por'] is not None:
        return data
    else:
        return None

def insert_data_db_executemany(conn, all_data):
    if not conn or not all_data:
        print("Aviso: Nenhum dado para inserir.")
        return 0
    cursor = None
    inserted_count = 0
    current_time = datetime.datetime.now()
    sql = f"""
        INSERT INTO {TABLE_NAME} 
        (NOME, CATEGORIA, PRECO_DE, PRECO_POR, FL_PROMOCAO, MEDIDAS, RANKING_VENDAS, ID_PRODUTO, DATA_EXTRACAO) 
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    params_list = []
    for item in all_data:
        if item.get('nome') is None or item.get('preco_por') is None or item.get('ranking_vendas') is None:
            print(f"Aviso: Item incompleto ignorado na preparação para DB: {item}")
            continue
        params = (
            item.get('nome'),
            item.get('categoria'),
            item.get('preco_de'),
            item.get('preco_por'),
            item.get('fl_promocao'),
            item.get('medidas'),
            item.get('ranking_vendas'), # ranking_vendas agora está presente
            item.get('id_produto'),
            current_time
        )
        params_list.append(params)

    if not params_list:
        print("Nenhum dado válido preparado para inserção.")
        return 0

    try:
        cursor = conn.cursor()
        print(f"\n--- Iniciando inserção em lote de {len(params_list)} registros na tabela {TABLE_NAME} ---")
        cursor.fast_executemany = True
        cursor.executemany(sql, params_list)
        conn.commit()
        inserted_count = len(params_list)
        print(f"Inserção em lote concluída. {inserted_count} registros potencialmente afetados.")
    except pyodbc.Error as ex:
        print(f"Erro Crítico executemany: {ex}")
        print("Verifique nomes/tipos das colunas SQL e a definição da tabela.")
        try:
            conn.rollback()
            print("Rollback realizado.")
        except pyodbc.Error as rb_ex:
            print(f"Erro rollback: {rb_ex}")
        return 0
    except Exception as e:
        print(f"Erro inesperado executemany/commit: {e}")
        return 0
    finally:
        if cursor:
            cursor.close()
    return inserted_count

# --- Função de Processamento por Página (para Threads) ---
def process_page(page_num, url_template, container_selector, item_selector, load_delay, scroll_pause, user_agent_string):
    page_products = []
    current_url = url_template.format(page=page_num)
    # print(f"[Thread {os.getpid()}] Processando página {page_num}: {current_url}")

    thread_driver = None
    try:
        chrome_options = webdriver.ChromeOptions()
        # Descomente a linha abaixo para rodar headless (sem interface gráfica)
        # chrome_options.add_argument("--headless")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--window-size=1920,1080") # Menor resolução pode ser mais leve se headless
        chrome_options.add_argument(f"user-agent={user_agent_string}")
        chrome_options.add_argument("--disable-notifications")
        chrome_options.add_argument("--disable-popup-blocking")
        chrome_options.add_argument("--log-level=3") # Reduz logs do Chrome
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        # Tentar reutilizar o serviço do ChromeDriverManager se possível, ou garantir que não cause conflitos
        # Se ChromeDriverManager().install() for chamado em paralelo, pode haver problemas.
        # Uma solução é chamar install() uma vez no início do script principal ou usar um caminho fixo.
        # Por simplicidade aqui, cada thread chama, mas o manager deve cachear.
        service = ChromeService(ChromeDriverManager().install())
        thread_driver = webdriver.Chrome(service=service, options=chrome_options)
        thread_driver.implicitly_wait(5) # Implicit wait menor, pois usamos explicit waits

        thread_driver.get(current_url)
        WebDriverWait(thread_driver, 20).until( # Timeout de 20s para carregamento do container
            EC.presence_of_element_located((By.CSS_SELECTOR, container_selector))
        )
        time.sleep(load_delay) # Pequeno delay para JS adicional após elemento principal aparecer

        # Scroll (se necessário para lazy loading)
        try:
            # Scrolls mais curtos podem ser suficientes e mais rápidos
            thread_driver.execute_script("window.scrollTo(0, document.body.scrollHeight*0.5);")
            time.sleep(scroll_pause)
            thread_driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(scroll_pause)
        except Exception as scroll_e:
            print(f"[Thread {os.getpid()}] Erro scroll página {page_num}: {scroll_e}")

        page_html = thread_driver.page_source
        soup = BeautifulSoup(page_html, 'html.parser')
        
        product_list_container = soup.select_one(container_selector)
        product_elements_soup = []
        if product_list_container:
            product_elements_soup = product_list_container.select(item_selector)
        
        if not product_elements_soup:
             # print(f"[Thread {os.getpid()}] Nenhum produto encontrado na página {page_num}.")
             # Retornar lista vazia é importante para a lógica de fim de paginação se aplicável
             pass


        for product_el_soup in product_elements_soup:
            product_data = parse_product_data(product_el_soup) # rank não é mais passado
            if product_data:
                page_products.append(product_data)
        
        # print(f"[Thread {os.getpid()}] Página {page_num} extraiu {len(page_products)} produtos.")
        return {'page_num': page_num, 'products': page_products, 'url': current_url}

    except TimeoutException:
        print(f"[Thread {os.getpid()}] Timeout ao carregar página {page_num}: {current_url}. Pode ser o fim ou erro.")
        # Se for um timeout, provavelmente não há produtos, então retorna uma lista vazia ou None
        # É importante que o loop principal possa detectar que uma página falhou ou não retornou produtos
        return {'page_num': page_num, 'products': [], 'url': current_url, 'error': 'TimeoutException'}
    except Exception as e:
        print(f"[Thread {os.getpid()}] Erro processando página {page_num} ({current_url}): {e}")
        return {'page_num': page_num, 'products': [], 'url': current_url, 'error': str(e)}
    finally:
        if thread_driver:
            thread_driver.quit()

# --- Script Principal ---
if __name__ == "__main__":
    print(f"--- Iniciando script {TABLE_NAME} com {NUM_THREADS} Threads ---")
    script_start_time = time.time()

    db_config = load_db_config(CREDENTIALS_PATH)
    if not db_config: sys.exit("Encerrado: Falha config DB.")
    
    # Instalar o ChromeDriver uma vez aqui pode ser benéfico, embora o manager faça cache.
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
        # Submete todas as tarefas
        future_to_page = {
            executor.submit(
                process_page, page_num, INITIAL_URL_TEMPLATE, PRODUCT_CONTAINER_SELECTOR, 
                PRODUCT_ITEM_SELECTOR, PAGE_LOAD_DELAY, SCROLL_PAUSE_TIME, USER_AGENT
            ): page_num for page_num in pages_to_process
        }

        # Processa os resultados à medida que são concluídos
        for future in tqdm(concurrent.futures.as_completed(future_to_page), total=len(pages_to_process), desc="Progresso Páginas"):
            page_num_completed = future_to_page[future]
            try:
                result = future.result() # Retorna o dict: {'page_num': page_num, 'products': page_products, 'url': current_url, 'error': ...}
                if result: # Mesmo que haja erro, o dict é retornado
                    all_page_results_unordered.append(result)
                    if result.get('error'):
                         print(f"Página {result['page_num']} ({result['url']}) teve um erro: {result['error']}")
                    elif not result.get('products') and not result.get('error'): # Sem erro, mas sem produtos
                         # Isso pode indicar o fim real da paginação antes de MAX_PAGES_TO_SCRAPE
                         print(f"Página {result['page_num']} ({result['url']}) não retornou produtos. Possível fim da paginação.")
                         # Poderia adicionar lógica aqui para parar de submeter novas tarefas se X páginas vazias seguidas.
                         # Mas com `as_completed` é mais complexo.
            except Exception as exc:
                # Exceção não pega dentro de process_page (improvável se bem tratado lá)
                print(f"Página {page_num_completed} gerou uma exceção no executor: {exc}")
                all_page_results_unordered.append({'page_num': page_num_completed, 'products': [], 'error': str(exc)})


    print("\n--- Coleta de todas as threads concluída ---")

    # Ordenar resultados pela ordem original da página
    all_page_results_ordered = sorted(all_page_results_unordered, key=lambda x: x['page_num'])

    # Montar a lista final de produtos e aplicar ranking global
    all_products_data = []
    current_global_rank = 1
    actual_pages_processed_with_data = 0
    for page_result in all_page_results_ordered:
        if page_result.get('products'): # Apenas se a página retornou produtos
            actual_pages_processed_with_data +=1
            for product_data_item in page_result['products']:
                product_data_item['ranking_vendas'] = current_global_rank # Adiciona o ranking global
                all_products_data.append(product_data_item)
                current_global_rank += 1
    
    print(f"Total de páginas que retornaram algum dado de produto: {actual_pages_processed_with_data}")

    # Inserir Dados no Banco
    produtos_inseridos_total = 0
    if all_products_data:
        conn = connect_db(db_config) # Conectar ao DB somente quando for inserir
        if conn:
            produtos_inseridos_total = insert_data_db_executemany(conn, all_products_data)
            conn.close()
            print("Conexão DB fechada após inserção.")
        else:
            print("Falha ao conectar ao DB para inserção. Dados não foram salvos.")
    else:
        print("Nenhum produto foi extraído para inserção.")

    # Finalização
    script_end_time = time.time()
    total_time = script_end_time - script_start_time
    print("\n--- Extração Concluída ---")
    print(f"Tempo total: {total_time:.2f} seg ({total_time/60:.2f} min)")
    
    pages_attempted = len(pages_to_process)
    # Um cálculo mais preciso de páginas efetivamente processadas seria contar quantas não tiveram erro fatal
    successful_pages = sum(1 for r in all_page_results_ordered if not r.get('error') and r.get('products'))
    pages_with_errors = sum(1 for r in all_page_results_ordered if r.get('error'))
    
    print(f"Páginas que deveriam ser processadas: {pages_attempted}")
    print(f"Páginas processadas com sucesso e com produtos: {successful_pages}")
    print(f"Páginas que resultaram em erro: {pages_with_errors}")
    print(f"Total de produtos extraídos: {len(all_products_data)}")
    print(f"Total de produtos inseridos na tabela '{TABLE_NAME}': {produtos_inseridos_total}")
    print("--- Fim do script ---")