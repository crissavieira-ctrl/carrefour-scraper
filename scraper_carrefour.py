# -- coding: utf-8 --
"""
Scraper Carrefour via JSON-LD (ld+json) para múltiplas cidades
- Modo: GitHub Actions + commit no repo
- Armazenamento: 1 Excel por cidade/mês (coluna por dia)
"""

import os, json, time, re
from datetime import datetime
import pandas as pd

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# =========================
# 0) Cidades e CEPs-alvo
# =========================
CIDADES = {
    "Belo_Horizonte": "30110-002",
    "Rio_de_Janeiro": "20010-000",
    "Salvador": "40020-000",
    "Curitiba": "80010-000",
    "Porto_Alegre": "90010-150",
    "Belem": "66010-000",
    "Recife": "50010-040",
}

# =========================
# 1) Paths e nomes mensais
# =========================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
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
    opts.page_load_strategy = "eager"
    opts.add_experimental_option("prefs", {"profile.managed_default_content_settings.images": 2})
    driver = webdriver.Chrome(options=opts)
    driver.set_page_load_timeout(60)
    driver.implicitly_wait(2)
    return driver

# ======================================================
# 3) Definir CEP na UI
# ======================================================
def set_cep(driver: webdriver.Chrome, cep: str, timeout: int = 20):
    driver.get("https://mercado.carrefour.com.br/")
    wait = WebDriverWait(driver, timeout)

    opened = False
    open_btn_selectors = [
        ("css", 'button[aria-label*="CEP"]'),
        ("css", 'button[data-testid*="address"], button[data-testid*="location"]'),
        ("xpath", '//button[contains(., "Informe seu CEP")]'),
        ("xpath", '//button[contains(., "Alterar endereço") or contains(., "Trocar endereço")]'),
        ("css", 'button[aria-label*="endereço"]'),
    ]
    for how, sel in open_btn_selectors:
        try:
            elem = wait.until(
                EC.element_to_be_clickable(
                    (By.CSS_SELECTOR, sel) if how == "css" else (By.XPATH, sel)
                )
            )
            elem.click()
            opened = True
            break
        except Exception:
            continue
    if not opened:
        driver.execute_script("""
            var btn = document.querySelector('button[aria-label*="CEP"], button[data-testid*="address"], button[data-testid*="location"], button[aria-label*="endereço"]');
            if (btn) btn.click();
        """)
        time.sleep(1)

    input_locators = [
        (By.CSS_SELECTOR, 'input[name*="cep"], input[id*="cep"], input[name*="zip"], input[id*="zip"]'),
        (By.XPATH, '//input[@type="text" and (contains(@name,"cep") or contains(@id,"cep") or contains(@name,"zip") or contains(@id,"zip"))]'),
    ]
    cep_input = None
    for by, sel in input_locators:
        try:
            cep_input = wait.until(EC.visibility_of_element_located((by, sel)))
            break
        except Exception:
            continue

    if cep_input is None:
        driver.execute_script("""
            const i = document.querySelector('input[name*="cep"], input[id*="cep"], input[name*="zip"], input[id*="zip"]');
            if (i) { i.value = arguments[0]; i.dispatchEvent(new Event('input', {bubbles:true})); }
        """, cep)
    else:
        cep_input.clear()
        cep_input.send_keys(cep)
        time.sleep(0.6)

    confirm_selectors = [
        (By.CSS_SELECTOR, 'button[type="submit"]'),
        (By.XPATH, '//button[contains(., "Confirmar") or contains(., "Continuar") or contains(., "OK")]'),
    ]
    clicked = False
    for by, sel in confirm_selectors:
        try:
            btn = wait.until(EC.element_to_be_clickable((by, sel)))
            btn.click()
            clicked = True
            break
        except Exception:
            continue
    if not clicked:
        driver.execute_script("""
            const b = [...document.querySelectorAll('button')].find(x => /confirmar|continuar|ok/i.test(x.textContent));
            if (b) b.click();
        """)
    time.sleep(2.5)

# ================================
# 4) Helpers para parse de JSON-LD
# ================================
_money_cleaner = re.compile(r"[^0-9,.\-]")

def to_float_price(x):
    if x is None:
        return 0.0
    s = _money_cleaner.sub("", str(x)).strip()
    s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return 0.0

def extract_price(offers):
    if not offers:
        return 0.0
    cand = []
    if isinstance(offers, dict):
        cand.append(offers.get("price"))
        cand.append(offers.get("lowPrice"))
        spec = offers.get("priceSpecification") or {}
        if isinstance(spec, dict):
            cand.append(spec.get("price"))
    elif isinstance(offers, list):
        for it in offers:
            if not isinstance(it, dict):
                continue
            cand.extend([it.get("price"), it.get("lowPrice")])
            spec = it.get("priceSpecification") or {}
            if isinstance(spec, dict):
                cand.append(spec.get("price"))
    for c in cand:
        v = to_float_price(c)
        if v > 0:
            return v
    return to_float_price(cand[0] if cand else None)

def find_products_in_json(data):
    found = []
    if isinstance(data, dict):
        if data.get("@type") == "Product":
            found.append(data)
        if "@graph" in data and isinstance(data["@graph"], list):
            for obj in data["@graph"]:
                if isinstance(obj, dict) and obj.get("@type") == "Product":
                    found.append(obj)
    elif isinstance(data, list):
        for obj in data:
            if isinstance(obj, dict) and obj.get("@type") == "Product":
                found.append(obj)
    return found

# =====================================
# 5) Scraper
# =====================================
def scrape_product_via_json(url: str, driver: webdriver.Chrome) -> dict:
    try:
        driver.get(url)
        time.sleep(2)
        tags = driver.find_elements(By.XPATH, '//script[@type="application/ld+json"]')
        for tag in tags:
            raw = tag.get_attribute("innerHTML")
            if not raw:
                continue
            try:
                data = json.loads(raw)
                candidates = find_products_in_json(data)
            except Exception:
                candidates = []
                parts = re.split(r'\n(?=\s*[{[]")', raw)
                for p in parts:
                    try:
                        d = json.loads(p)
                        candidates.extend(find_products_in_json(d))
                    except Exception:
                        continue
            for obj in candidates:
                name = obj.get("name", "Não encontrado")
                price_float = extract_price(obj.get("offers"))
                return {"Nome do Produto": name, "Preço": float(price_float), "URL": url}
    except Exception as e:
        print(f"❌ Erro ao carregar {url}: {e}")
    return {"Nome do Produto": "Não encontrado", "Preço": 0.0, "URL": url}

# =========================
# 6) URLs
# =========================
URLS = [
    'https://mercado.carrefour.com.br/arroz-branco-longofino-tipo-1-tio-joao-2kg-115657/p',
    'https://mercado.carrefour.com.br/feijao-carioca-tipo-1-kicaldo-1kg-466506/p'
    # ... (sua lista completa de URLs) ...
]

# ===========================================
# 7) Persistência
# ===========================================
def salvar_excel_cidade(cidade_slug: str, df_ok: pd.DataFrame, df_err: pd.DataFrame):
    city_dir = os.path.join(DATA_DIR, cidade_slug)
    os.makedirs(city_dir, exist_ok=True)

    arq_mensal = os.path.join(city_dir, f"precos_carrefour_{STAMP_MONTH}.xlsx")
    arq_erros  = os.path.join(city_dir, f"erros_carrefour_{STAMP_MONTH}.xlsx")

    if not df_ok.empty:
        if os.path.exists(arq_mensal):
            base = pd.read_excel(arq_mensal, sheet_name="Precos")
            if "Nome do Produto" not in base.columns:
                base["Nome do Produto"] = df_ok["Nome do Produto"]
            nova = df_ok[["Nome do Produto", "Preço"]].copy()
            base = base.merge(nova, on="Nome do Produto", how="outer")
            if "Preço" in base.columns:
                base.rename(columns={"Preço": COLUNA_DIA}, inplace=True)
        else:
            base = df_ok[["Nome do Produto", "URL", "Preço"]].rename(columns={"Preço": COLUNA_DIA})

        with pd.ExcelWriter(arq_mensal, engine="openpyxl", mode="w") as w:
            base.to_excel(w, index=False, sheet_name="Precos")
        print(f"✔ [{cidade_slug}] Excel atualizado: {arq_mensal} (coluna {COLUNA_DIA})")
    else:
        print(f"⚠️ [{cidade_slug}] Nenhum preço válido hoje.")

    if not df_err.empty:
        df_err = df_err.copy()
        df_err["Data"] = today.strftime("%Y-%m-%d")
        if os.path.exists(arq_erros):
            be = pd.read_excel(arq_erros)
            be = pd.concat([be, df_err], ignore_index=True)
        else:
            be = df_err
        be.to_excel(arq_erros, index=False)
        print(f"⚠️ [{cidade_slug}] Erros/zeros salvos: {arq_erros}")
    else:
        print(f"✅ [{cidade_slug}] Sem erros hoje.")

# =========================
# 8) Execução principal
# =========================
def main():
    driver = build_driver(headless=True)
    try:
        for cidade_slug, cep in CIDADES.items():
            print(f"\n================ {cidade_slug} (CEP {cep}) ================")
            set_cep(driver, cep)
            time.sleep(1.0)

            registros = []
            for url in URLS:
                try:
                    rec = scrape_product_via_json(url, driver)
                    if "Preço" not in rec:
                        rec["Preço"] = 0.0
                    if "Nome do Produto" not in rec:
                        rec["Nome do Produto"] = "Não encontrado"
                    if "URL" not in rec:
                        rec["URL"] = url
                    registros.append(rec)
                except Exception as e:
                    registros.append({"Nome do Produto": "Erro", "Preço": 0.0, "URL": url, "Erro": str(e)})
                time.sleep(0.8)

            df_total = pd.DataFrame(registros)
            for col, default in [("Nome do Produto", "Não encontrado"), ("Preço", 0.0), ("URL", "")]:
                if col not in df_total.columns:
                    df_total[col] = default

            df_ok  = df_total[df_total["Preço"].fillna(0) > 0][["Nome do Produto", "Preço", "URL"]].copy()
            df_err = df_total[df_total["Preço"].fillna(0) <= 0].copy()

            print(f"[{cidade_slug}] Capturados: {len(df_total)} | OK: {len(df_ok)} | Erros/zeros: {len(df_err)}")
            if not df_ok.empty:
                print(df_ok.head(3).to_string(index=False))

            salvar_excel_cidade(cidade_slug, df_ok, df_err)
    finally:
        driver.quit()

if __name__ == "__main__":
    main()



