# -- coding: utf-8 --
"""
Scraper Carrefour via JSON-LD (ld+json)
Modo: GitHub Actions + commit no repo
Armazenamento: 1 Excel por mês (coluna por dia) POR CIDADE
"""

import os
import json
import time
from datetime import datetime
import pandas as pd

from selenium import webdriver
from selenium.webdriver.common.by import By
# (NOVO) Para abrir/confirmar o CEP; se não usar, o resto continua igual
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# =========================
# 0) CIDADES E CEPs
# =========================
CIDADES = {
    "Sao_Paulo": "01001-000",
    "Belo_Horizonte": "30110-002",
    "Rio_de_Janeiro": "20010-000",
    "Salvador": "40020-000",
    "Curitiba": "80010-000",
    "Porto_Alegre": "90010-150",
    "Belem": "66010-000",
}

# =========================
# 1) Paths e nomes mensais
# =========================
try:
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))   # se rodar como script
except NameError:
    BASE_DIR = os.getcwd()                                  # fallback p/ notebook/runner

DATA_DIR = os.path.join(BASE_DIR, "data")
os.makedirs(DATA_DIR, exist_ok=True)

today = datetime.now()
STAMP_DAY = today.strftime("%Y%m%d")       # -> coluna diária (Preço_YYYYMMDD)
STAMP_MONTH = today.strftime("%Y-%m")      # -> arquivo do mês (precos_carrefour_YYYY-MM.xlsx)
COLUNA_DIA = f"Preço_{STAMP_DAY}"

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
    opts.page_load_strategy = "eager"  # não espera tudo para acelerar
    # desliga imagens para ganhar velocidade
    opts.add_experimental_option("prefs", {
        "profile.managed_default_content_settings.images": 2
    })
    driver = webdriver.Chrome(options=opts)  # Selenium Manager resolve o driver
    driver.set_page_load_timeout(60)
    driver.implicitly_wait(2)
    return driver

# =====================================
# 2.1) (NOVO) Tentar definir CEP da cidade na UI
#       -> se falhar, segue do mesmo jeito de antes (sem quebrar)
# =====================================
def set_cep(driver: webdriver.Chrome, cep: str, timeout: int = 25):
    try:
        driver.get("https://mercado.carrefour.com.br/")
        wait = WebDriverWait(driver, timeout)

        # abrir modal CEP (várias opções; se nenhuma pegar, tenta via JS)
        open_btns = [
            (By.CSS_SELECTOR, 'button[aria-label*="CEP"]'),
            (By.CSS_SELECTOR, 'button[data-testid*="address"], button[data-testid*="location"]'),
            (By.XPATH, '//button[contains(., "Informe seu CEP") or contains(., "Alterar endereço") or contains(., "Trocar endereço")]'),
            (By.CSS_SELECTOR, 'button[aria-label*="endereço"]'),
        ]
        opened = False
        for how in open_btns:
            try:
                btn = wait.until(EC.element_to_be_clickable(how))
                btn.click()
                opened = True
                break
            except Exception:
                continue
        if not opened:
            driver.execute_script("""
                const b = document.querySelector('button[aria-label*="CEP"], button[data-testid*="address"], button[data-testid*="location"], button[aria-label*="endereço"]');
                if (b) b.click();
            """)
            time.sleep(1)

        # campo de CEP
        cep_input = None
        for how in [
            (By.CSS_SELECTOR, 'input[name*="cep"], input[id*="cep"], input[name*="zip"], input[id*="zip"]'),
            (By.XPATH, '//input[@type="text" and (contains(@name,"cep") or contains(@id,"cep") or contains(@name,"zip") or contains(@id,"zip") or contains(@placeholder, "CEP"))]'),
        ]:
            try:
                cep_input = wait.until(EC.visibility_of_element_located(how))
                break
            except Exception:
                continue

        if cep_input is not None:
            cep_input.clear()
            cep_input.send_keys(cep)
            time.sleep(0.6)
        else:
            driver.execute_script("""
                const i = document.querySelector('input[name*="cep"], input[id*="cep"], input[name*="zip"], input[id*="zip"]');
                if (i) { i.value = arguments[0]; i.dispatchEvent(new Event('input', {bubbles:true})); }
            """, cep)

        # confirmar
        confirmed = False
        for how in [
            (By.CSS_SELECTOR, 'button[type="submit"]'),
            (By.XPATH, '//button[contains(., "Confirmar") or contains(., "Continuar") or contains(., "OK")]'),
        ]:
            try:
                btn = wait.until(EC.element_to_be_clickable(how))
                btn.click()
                confirmed = True
                break
            except Exception:
                continue
        if not confirmed:
            driver.execute_script("""
                const b = [...document.querySelectorAll('button')].find(x => /confirmar|continuar|ok/i.test(x.textContent));
                if (b) b.click();
            """)

        time.sleep(2.0)
        print(f"📍 CEP definido: {cep}")
    except Exception as e:
        print(f"⚠️ Não consegui aplicar o CEP {cep}. Vou seguir assim mesmo. Detalhe: {e}")

# =====================================
# 3) Scraper: lê JSON-LD do tipo Product (SEU CÓDIGO ORIGINAL)
# =====================================
def scrape_product_via_json(url: str, driver: webdriver.Chrome) -> dict:
    print(f"\n🔗 {url}")
    driver.get(url)
    time.sleep(2)  # pequeno respiro para scripts carregarem

    try:
        tags = driver.find_elements(By.XPATH, '//script[@type="application/ld+json"]')
        for tag in tags:
            raw = tag.get_attribute("innerHTML")
            if not raw:
                continue

            try:
                data = json.loads(raw)
            except Exception:
                # ignora blocos inválidos
                continue

            objs = data if isinstance(data, list) else [data]
            for obj in objs:
                if isinstance(obj, dict) and obj.get("@type") == "Product":
                    name = obj.get("name", "Não encontrado")

                    # offers pode ser dict ou lista
                    offers = obj.get("offers", {})
                    price = None
                    if isinstance(offers, dict):
                        price = offers.get("price")
                    elif isinstance(offers, list) and offers:
                        price = offers[0].get("price")

                    try:
                        price_float = float(str(price).replace(",", "."))
                    except Exception:
                        price_float = 0.0

                    print("✅", name, "| R$", price_float)
                    return {"Nome do Produto": name, "Preço": price_float, "URL": url}

    except Exception as e:
        print("❌ Erro no parsing JSON-LD:", e)

    print("⚠️ Nada encontrado nessa URL.")
    return {"Nome do Produto": "Não encontrado", "Preço": 0.0, "URL": url}

# =========================
# 4) URLs (sua lista aqui)  [MANTIDA]
# =========================
URLS = [
    'https://mercado.carrefour.com.br/arroz-branco-longofino-tipo-1-tio-joao-2kg-115657/p',
    'https://mercado.carrefour.com.br/feijao-carioca-tipo-1-kicaldo-1kg-466506/p',
    # ... mantenha o restante da sua lista
]

# =========================
# 5) Execução para UMA cidade (envólucro)
#     -> reaproveita 100% da tua lógica de Excel
# =========================
def rodar_para_cidade(driver: webdriver.Chrome, cidade_slug: str, cep: str):
    # tenta aplicar CEP (mas não quebra se falhar)
    set_cep(driver, cep)
    time.sleep(1)

    # --- raspagem (igual ao teu main original) ---
    registros = []
    for url in URLS:
        registros.append(scrape_product_via_json(url, driver))
        time.sleep(1)

    df_total = pd.DataFrame(registros)
    df_ok  = df_total[df_total["Preço"] > 0][["Nome do Produto", "Preço"]].copy()
    df_err = df_total[df_total["Preço"] <= 0].copy()

    # --- paths POR CIDADE (mudança mínima) ---
    city_dir = os.path.join(DATA_DIR, cidade_slug)
    os.makedirs(city_dir, exist_ok=True)
    ARQ_MENSAL = os.path.join(city_dir, f"precos_carrefour_{STAMP_MONTH}.xlsx")
    ARQ_ERROS  = os.path.join(city_dir, f"erros_carrefour_{STAMP_MONTH}.xlsx")

    # ---- Excel mensal: aba "Precos" com coluna diária (SEU CÓDIGO) ----
    if not df_ok.empty:
        if os.path.exists(ARQ_MENSAL):
            base = pd.read_excel(ARQ_MENSAL, sheet_name="Precos")
            if "Nome do Produto" not in base.columns:
                base["Nome do Produto"] = df_ok["Nome do Produto"]
            base = base.merge(df_ok, on="Nome do Produto", how="outer")
            if "Preço" in base.columns:
                base[COLUNA_DIA] = base.pop("Preço")
        else:
            base = df_ok.rename(columns={"Preço": COLUNA_DIA})

        with pd.ExcelWriter(ARQ_MENSAL, engine="openpyxl", mode="w") as w:
            base.to_excel(w, index=False, sheet_name="Precos")
        print(f"📁 [{cidade_slug}] Atualizado: {ARQ_MENSAL} (coluna {COLUNA_DIA})")
    else:
        print(f"⚠️ [{cidade_slug}] Nenhum preço válido hoje.")

    # ---- Log de erros do mês (SEU CÓDIGO) ----
    if not df_err.empty:
        df_err["Data"] = today.strftime("%Y-%m-%d")
        if os.path.exists(ARQ_ERROS):
            be = pd.read_excel(ARQ_ERROS)
            be = pd.concat([be, df_err], ignore_index=True)
        else:
            be = df_err
        be.to_excel(ARQ_ERROS, index=False)
        print(f"⚠️ [{cidade_slug}] Erros/zeros salvos: {ARQ_ERROS}")
    else:
        print(f"✅ [{cidade_slug}] Sem erros hoje.")

# =========================
# 6) Execução principal (loop de cidades)
# =========================
def main():
    driver = build_driver(headless=True)
    try:
        for cidade_slug, cep in CIDADES.items():
            print(f"\n================ {cidade_slug} (CEP {cep}) ================")
            rodar_para_cidade(driver, cidade_slug, cep)
    finally:
        driver.quit()

if __name__ == "__main__":
    main()

