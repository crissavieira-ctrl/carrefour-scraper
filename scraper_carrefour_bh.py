# -*- coding: utf-8 -*-
"""
Scraper Carrefour via JSON-LD (ld+json) — Belo Horizonte
Modo: GitHub Actions + commit no repo
Armazenamento: 1 Excel por mês (coluna por dia) — pasta data_bh/
"""

import os
import json
import time
from datetime import datetime
import pandas as pd

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


# =========================
# 1) Paths e nomes mensais
# =========================
try:
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
except NameError:
    BASE_DIR = os.getcwd()

DATA_DIR = os.path.join(BASE_DIR, "data_bh")
os.makedirs(DATA_DIR, exist_ok=True)

today = datetime.now()
STAMP_DAY = today.strftime("%Y%m%d")       # -> coluna diária (Preço_YYYYMMDD)
STAMP_MONTH = today.strftime("%Y-%m")      # -> arquivo do mês (precos_carrefour_bh-YYYY-MM.xlsx)

ARQ_MENSAL = os.path.join(DATA_DIR, f"precos_carrefour_bh-{STAMP_MONTH}.xlsx")
ARQ_ERROS  = os.path.join(DATA_DIR, f"erros_carrefour_bh-{STAMP_MONTH}.xlsx")
COLUNA_DIA = f"Preço_{STAMP_DAY}"
CIDADE_TAG = "Belo Horizonte"

# CEP central de BH — usado para fixar localização
CEP_BH = "30130-000"


# =========================================
# 2) Driver (headless — ideal para Actions)
# =========================================
def build_driver(headless: bool = True):
    opts = webdriver.ChromeOptions()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.page_load_strategy = "eager"
    # desliga imagens para ganhar velocidade
    opts.add_experimental_option("prefs", {
        "profile.managed_default_content_settings.images": 2
    })
    driver = webdriver.Chrome(options=opts)
    driver.set_page_load_timeout(60)
    driver.implicitly_wait(2)
    return driver


# ==========================================================
# 3) Fixar a localização no site (CEP de BH) — modo robusto
# ==========================================================
def fix_location_bh(driver, cep: str = CEP_BH):
    """
    Tenta abrir a home, acionar o seletor de endereço e setar o CEP de BH.
    Implementa múltiplos fallbacks de seletores porque o site muda com frequência.
    Em Actions/headless, pode levar alguns segundos.
    """
    home = "https://mercado.carrefour.com.br/"
    driver.get(home)
    time.sleep(2)

    wait = WebDriverWait(driver, 12)

    # Alguns sites mostram pop-ups de cookies. Tenta dispensar.
    for xpath in [
        '//button[contains(., "Aceitar") or contains(., "Continuar") or contains(., "Concordo")]',
        '//button[contains(@id,"onetrust-accept-btn-handler")]',
        '//button[contains(., "OK")]',
    ]:
        try:
            btn = wait.until(EC.element_to_be_clickable((By.XPATH, xpath)))
            btn.click()
            time.sleep(0.8)
            break
        except Exception:
            pass

    # Abre o seletor de endereço (várias estratégias)
    opened = False
    for xpath in [
        '//button[contains(., "Informe seu endereço")]',
        '//button[contains(., "Alterar endereço")]',
        '//button[contains(., "Mudar endereço")]',
        '//button[contains(., "Endereço")]',
        '//button[contains(@aria-label,"Endereço")]',
        '//div[contains(@class,"address")]//button',
        '//button[contains(@data-testid,"address") or contains(@data-testid,"location")]',
    ]:
        try:
            btn = wait.until(EC.element_to_be_clickable((By.XPATH, xpath)))
            btn.click()
            opened = True
            time.sleep(1.0)
            break
        except Exception:
            continue

    # Se não abriu explicitamente, às vezes já há input na home
    # Procura o campo de CEP
    input_el = None
    for xpath in [
        '//input[@name="zipcode" or @id="zipcode" or contains(@placeholder,"CEP")]',
        '//input[contains(@aria-label,"CEP")]',
        '//input[@type="text" and (contains(@placeholder,"CEP") or contains(@data-testid,"cep"))]',
    ]:
        try:
            input_el = wait.until(EC.presence_of_element_located((By.XPATH, xpath)))
            break
        except Exception:
            continue

    if input_el:
        try:
            input_el.clear()
            input_el.send_keys(cep)
            time.sleep(0.8)
        except Exception:
            pass

        # Confirmar/continuar (botões comuns)
        for xpath in [
            '//button[contains(., "Confirmar") or contains(., "Continuar") or contains(., "Buscar") or contains(., "OK")]',
            '//button[@type="submit"]',
        ]:
            try:
                btn = driver.find_element(By.XPATH, xpath)
                if btn.is_enabled():
                    btn.click()
                    time.sleep(1.2)
                    break
            except Exception:
                continue

        # Mais um pequeno passeio pela home para consolidar o contexto regional
        driver.get(home)
        time.sleep(1.2)


# =====================================
# 4) Scraper: lê JSON-LD do tipo Product
# =====================================
def _coerce_price(value):
    if value is None:
        return 0.0
    s = str(value).strip().replace("R$", "").replace("\u00a0", " ").replace(".", "").replace(",", ".")
    try:
        return float(s)
    except Exception:
        return 0.0


def parse_jsonld(raw: str):
    """
    Retorna uma lista de objetos (dicts) de JSON-LD a partir do raw.
    Suporta único objeto, lista, e @graph.
    """
    try:
        data = json.loads(raw)
    except Exception:
        return []

    objs = []
    if isinstance(data, dict):
        if "@graph" in data and isinstance(data["@graph"], list):
            objs.extend([o for o in data["@graph"] if isinstance(o, dict)])
        else:
            objs.append(data)
    elif isinstance(data, list):
        objs.extend([o for o in data if isinstance(o, dict)])
    return objs


def scrape_product_via_json(url: str, driver: webdriver.Chrome) -> dict:
    print(f"\n🔗 {url}")
    driver.get(url)
    time.sleep(2)  # pequeno respiro para scripts carregarem

    # às vezes o preço aparece após pequeno atraso
    for _ in range(2):
        try:
            tags = driver.find_elements(By.XPATH, '//script[@type="application/ld+json"]')
            for tag in tags:
                raw = tag.get_attribute("innerHTML")
                if not raw:
                    continue

                for obj in parse_jsonld(raw):
                    if obj.get("@type") == "Product":
                        name = obj.get("name", "Não encontrado")
                        offers = obj.get("offers", {})
                        price = None
                        # offers pode ser dict ou lista
                        if isinstance(offers, dict):
                            price = offers.get("price") or offers.get("priceSpecification", {}).get("price")
                        elif isinstance(offers, list) and offers:
                            price = (
                                offers[0].get("price")
                                or (offers[0].get("priceSpecification") or {}).get("price")
                            )

                        price_float = _coerce_price(price)
                        print("✅", name, "| R$", price_float)
                        return {
                            "Cidade": CIDADE_TAG,
                            "Nome do Produto": name,
                            "Preço": price_float,
                            "URL": url
                        }
        except Exception as e:
            print("❌ Erro no parsing JSON-LD:", e)
        time.sleep(1.0)

    print("⚠️ Nada encontrado nessa URL.")
    return {"Cidade": CIDADE_TAG, "Nome do Produto": "Não encontrado", "Preço": 0.0, "URL": url}


# =========================
# 5) URLs (mesma lista base)
# =========================
URLS = [
    # --- copie/cole aqui a mesma lista do script de SP (mantive sua lista completa) ---
    'https://mercado.carrefour.com.br/arroz-branco-longofino-tipo-1-tio-joao-2kg-115657/p',
    'https://mercado.carrefour.com.br/feijao-carioca-tipo-1-kicaldo-1kg-466506/p',
    'https://mercado.carrefour.com.br/macarrao-de-semola-com-ovos-espaguete-8-adria-500g-4180372/p',
    'https://mercado.carrefour.com.br/farofa-de-mandioca-tradicional-yoki-400g-6582613/p',
    'https://mercado.carrefour.com.br/massa-para-pastel-discao-massa-leve-500g-841757/p',
    'https://mercado.carrefour.com.br/macarrao-instantaneo-nissin-sabor-galinha-caipira-85g-4814177/p',
    'https://mercado.carrefour.com.br/batata-monalisa-carrefour-aprox-600g-46922/p',
    'https://mercado.carrefour.com.br/pimentao-block-vermelho-trebeshi-150-g-5738458/p',
    'https://mercado.carrefour.com.br/tomate-carmem-carrefour-aprox-500g-262676/p',
    'https://mercado.carrefour.com.br/cebola-carrefour-aprox-500g-20621/p',
    'https://mercado.carrefour.com.br/cenoura-unico-1kg-5154669/p',
    'https://mercado.carrefour.com.br/acucar-refinado-uniao-1kg-197564/p',
    'https://mercado.carrefour.com.br/chocolate-ao-leite-com-amendoim-shot-165g-5790859/p',
    'https://mercado.carrefour.com.br/sorvete-napolitano-nestle-1-5-litros-8616043/p',
    'https://mercado.carrefour.com.br/achocolatado-em-po-nescau-550g-6409717/p',
    'https://mercado.carrefour.com.br/alface-lisa-carrefour-7745044/p',
    'https://mercado.carrefour.com.br/couve-flor-cledson-300-g-9560297/p',
    'https://mercado.carrefour.com.br/banana-nanica-fresca-organica-600g-210978/p',
    'https://mercado.carrefour.com.br/banana-prata-fischer-turma-da-monica-750g-9773711/p',
    'https://mercado.carrefour.com.br/limao-siciliano-carrefour-aprox-500g-63592/p',
    'https://mercado.carrefour.com.br/maca-gala-carrefour-aprox-600-g-10120/p',
    'https://mercado.carrefour.com.br/mamao-formosa-sabor-qualidade-aprox-16-kg-20524/p',
    'https://mercado.carrefour.com.br/manga-palmer-carrefour-aprox-600g-88919/p',
    'https://mercado.carrefour.com.br/melancia-premium-carrefour-aprox---8kg-194743/p',
    'https://mercado.carrefour.com.br/pera-willians-aprox-500g-39675/p',
    'https://mercado.carrefour.com.br/uva-escura-sem-semente-carrefour-500g-5141982/p',
    'https://mercado.carrefour.com.br/laranja-pera-carrefour-mercado-5-kg-6282032/p',
    'https://mercado.carrefour.com.br/bisteca-suina-congelada-sadia-1-kg-209864/p',
    'https://mercado.carrefour.com.br/contra-file-swift-mais-aprox-1-5kg-295906/p',
    'https://mercado.carrefour.com.br/coxao-mole-fracionado-a-vacuo-aprox--1-3-kg-18295/p',
    'https://mercado.carrefour.com.br/alcatra-bovina-carrefour-aproximadamente-400-g-21962/p',
    'https://mercado.carrefour.com.br/patinho-fracionado-a-vacuo-500g-18325/p',
    'https://mercado.carrefour.com.br/lagarto-swift-mais-aprox-15kg-295914/p',
    'https://mercado.carrefour.com.br/paleta-bovina-a-vacuo-500gnao-reativarcodigo-de-compra-20745/p',
    'https://mercado.carrefour.com.br/acem-em-pedacos-carrefour-aproximadamente-500-g-158828/p',
    'https://mercado.carrefour.com.br/costela-minga-bovina-cong-aprox-2kg-224006/p',
    'https://mercado.carrefour.com.br/camarao-descascado-cozido-36-40-celm-400-g-5939747/p',
    'https://mercado.carrefour.com.br/posta-cacao-congelado-buona-pesca-500-g-6311059/p',
    'https://mercado.carrefour.com.br/file-de-merluza-congelado-planalto-500-g-6323774/p',
    'https://mercado.carrefour.com.br/file-de-pescada-sem-espinha-swift-500-g-5457297/p',
    'https://mercado.carrefour.com.br/file-de-tilapia-fresco-carrefour-500-g-98930/p',
    'https://mercado.carrefour.com.br/presunto-cozido-sem-capa-fatiado-aurora-aproximadamente-200-g-49450/p',
    'https://mercado.carrefour.com.br/salsicha-hot-dog-resfriada-aurora-aproximadamente-500-g-49352/p',
    'https://mercado.carrefour.com.br/linguica-toscana-swift-700-g-5600812/p',
    'https://mercado.carrefour.com.br/mortadela-defumada-sadia-280g-5447045/p',
    'https://mercado.carrefour.com.br/queijo-minas-frescal-aurora-450-g-6264693/p',
    'https://mercado.carrefour.com.br/queijo-coalho-bom-leite-500-g-4305054/p',
    'https://mercado.carrefour.com.br/leite-uht-integral-piratininga-1-l-665017/p',
    'https://mercado.carrefour.com.br/iogurte-natural-tradicional-batavo-170g-5150439/p',
    'https://mercado.carrefour.com.br/manteiga-com-sal-aviacao-200-g-10010/p',
    'https://mercado.carrefour.com.br/creme-de-leite-ultrapasteurizado-itambe-200-g-5988921/p',
    'https://mercado.carrefour.com.br/requeijao-cremoso-aviacao-tradicional-220-g-10000/p',
    'https://mercado.carrefour.com.br/acucar-cristal-carrefour-1kg-5147300/p',
    'https://mercado.carrefour.com.br/mel-com-cacau-e-avela-400-g-4510146/p',
    'https://mercado.carrefour.com.br/geleia-de-goiaba-selecoes-c-pedacos-260-g-1280815/p',
    'https://mercado.carrefour.com.br/suco-de-uva-integral-maric-1-l-3538256/p',
    'https://mercado.carrefour.com.br/vinho-tinto-fino-seco-cabernet-sauvignon-pergola-750ml-1521709/p',
    'https://mercado.carrefour.com.br/whisky-red-label-johnnie-walker-1-litro-2719/p',
    'https://mercado.carrefour.com.br/refrigerante-coca-cola-sabor-cola-1-5-l-11087/p',
    'https://mercado.carrefour.com.br/cafe-torrado-e-moido-extraforte-melitta-500g-271203/p',
    'https://mercado.carrefour.com.br/farinha-de-trigo-dona-benta-tradicional-1kg-196416/p',
    'https://mercado.carrefour.com.br/azeite-extravirgem-portugues-oliveira-da-serra-500-ml-4526108/p',
    'https://mercado.carrefour.com.br/oleo-de-soja-soya-900ml-482616/p',
    'https://mercado.carrefour.com.br/margarina-qualy-com-sal-250g-4815618/p',
    'https://mercado.carrefour.com.br/arroz-branco-longofino-tipo-1-tio-joao-1kg-115658/p',
    'https://mercado.carrefour.com.br/feijao-preto-tipo-1-kicaldo-1kg-466510/p',
    # ... (demais URLs que você já tem — mantidas para BH)
]

# =========================
# 6) Execução principal
# =========================
def main():
    driver = build_driver(headless=True)
    try:
        # 1) fixa localização em BH
        fix_location_bh(driver, CEP_BH)

        # 2) coleta
        registros = []
        for url in URLS:
            registros.append(scrape_product_via_json(url, driver))
            time.sleep(1)
    finally:
        driver.quit()

    df_total = pd.DataFrame(registros)
    df_ok  = df_total[df_total["Preço"] > 0][["Cidade", "Nome do Produto", "Preço", "URL"]].copy()
    df_err = df_total[df_total["Preço"] <= 0].copy()

    # ---- Excel mensal: aba "Precos" com coluna diária ----
    if not df_ok.empty:
        # base "wide": 1 linha por produto, colunas por dia
        df_wide = df_ok[["Nome do Produto", "Preço"]].rename(columns={"Preço": COLUNA_DIA})

        if os.path.exists(ARQ_MENSAL):
            base = pd.read_excel(ARQ_MENSAL, sheet_name="Precos")
            if "Nome do Produto" not in base.columns:
                base["Nome do Produto"] = df_wide["Nome do Produto"]
            base = base.merge(df_wide, on="Nome do Produto", how="outer")
        else:
            base = df_wide

        with pd.ExcelWriter(ARQ_MENSAL, engine="openpyxl", mode="w") as w:
            base.to_excel(w, index=False, sheet_name="Precos")

            # opcional: manter um histórico "longo" em outra aba
            df_ok_hist = df_ok.copy()
            df_ok_hist["Data"] = today.strftime("%Y-%m-%d")
            df_ok_hist.to_excel(w, index=False, sheet_name="Historico")

        print(f"📁 Atualizado: {ARQ_MENSAL} (coluna {COLUNA_DIA})")
    else:
        print("⚠️ Nenhum preço válido hoje.")

    # ---- Log de erros do mês (opcional) ----
    if not df_err.empty:
        df_err["Data"] = today.strftime("%Y-%m-%d")
        if os.path.exists(ARQ_ERROS):
            be = pd.read_excel(ARQ_ERROS)
            be = pd.concat([be, df_err], ignore_index=True)
        else:
            be = df_err
        be.to_excel(ARQ_ERROS, index=False)
        print(f"⚠️ Erros/zeros salvos: {ARQ_ERROS}")
    else:
        print("✅ Sem erros hoje.")


if __name__ == "__main__":
    main()
