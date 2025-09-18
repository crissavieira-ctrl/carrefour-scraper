# -*- coding: utf-8 -*-
"""
Scraper Carrefour via JSON-LD (ld+json) — Salvador
Armazenamento: 1 Excel por mês (coluna por dia) — pasta data_salvador/
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

DATA_DIR = os.path.join(BASE_DIR, "data_salvador")
os.makedirs(DATA_DIR, exist_ok=True)

today = datetime.now()
STAMP_DAY = today.strftime("%Y%m%d")       # -> coluna diária (Preço_YYYYMMDD)
STAMP_MONTH = today.strftime("%Y-%m")      # -> arquivo do mês

ARQ_MENSAL = os.path.join(DATA_DIR, f"precos_carrefour_salvador-{STAMP_MONTH}.xlsx")
ARQ_ERROS  = os.path.join(DATA_DIR, f"erros_carrefour_salvador-{STAMP_MONTH}.xlsx")
COLUNA_DIA = f"Preço_{STAMP_DAY}"
CIDADE_TAG = "Salvador"

# CEP central de Salvador p/ fixar geolocalização (Centro Histórico)
CEP_SSA = "40020-000"

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
    # desliga imagens p/ ganhar velocidade
    opts.add_experimental_option("prefs", {
        "profile.managed_default_content_settings.images": 2
    })
    driver = webdriver.Chrome(options=opts)
    driver.set_page_load_timeout(60)
    driver.implicitly_wait(2)
    return driver

# ==========================================================
# 3) Fixar a localização (CEP Salvador) — com fallbacks
# ==========================================================
def fix_location(driver, cep: str):
    home = "https://mercado.carrefour.com.br/"
    driver.get(home)
    time.sleep(2)
    wait = WebDriverWait(driver, 12)

    # cookies / consent
    for xpath in [
        '//button[contains(@id,"onetrust-accept-btn-handler")]',
        '//button[contains(., "Aceitar") or contains(., "Continuar") or contains(., "Concordo")]',
        '//button[contains(., "OK")]',
    ]:
        try:
            wait.until(EC.element_to_be_clickable((By.XPATH, xpath))).click()
            time.sleep(0.8)
            break
        except Exception:
            pass

    # abrir seletor de endereço
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
            wait.until(EC.element_to_be_clickable((By.XPATH, xpath))).click()
            time.sleep(1.0)
            break
        except Exception:
            pass

    # input CEP
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
            pass

    if input_el:
        try:
            input_el.clear()
            input_el.send_keys(cep)
            time.sleep(0.8)
        except Exception:
            pass

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
                pass

        driver.get(home)  # reforça o contexto regional
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
    time.sleep(2)
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
                        if isinstance(offers, dict):
                            price = offers.get("price") or (offers.get("priceSpecification") or {}).get("price")
                        elif isinstance(offers, list) and offers:
                            price = offers[0].get("price") or ((offers[0].get("priceSpecification") or {}).get("price"))
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
# 5) URLs (reaproveite sua lista)
# =========================
URLS = [
    # copie a MESMA lista usada em SP/BH/RJ (mantive só alguns exemplos aqui)
    'https://mercado.carrefour.com.br/arroz-branco-longofino-tipo-1-tio-joao-2kg-115657/p',
    'https://mercado.carrefour.com.br/feijao-carioca-tipo-1-kicaldo-1kg-466506/p',
    'https://mercado.carrefour.com.br/oleo-de-soja-soya-900ml-482616/p',
    # ... (demais URLs da sua lista completa)
]

# =========================
# 6) Execução principal
# =========================
def main():
    driver = build_driver(headless=True)
    try:
        fix_location(driver, CEP_SSA)
        registros = []
        for url in URLS:
            registros.append(scrape_product_via_json(url, driver))
            time.sleep(1)
    finally:
        driver.quit()

    df_total = pd.DataFrame(registros)
    df_ok  = df_total[df_total["Preço"] > 0][["Cidade", "Nome do Produto", "Preço", "URL"]].copy()
    df_err = df_total[df_total["Preço"] <= 0].copy()

    if not df_ok.empty:
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
            df_ok_hist = df_ok.copy()
