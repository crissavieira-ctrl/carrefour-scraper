# -- coding: utf-8 --
"""
Scraper Carrefour via JSON-LD (ld+json)
Modo: GitHub Actions + commit no repo
Armazenamento: 1 Excel por mês por cidade (coluna por dia)
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
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
except NameError:
    BASE_DIR = os.getcwd()

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
    # desliga imagens para ganhar velocidade
    opts.add_experimental_option("prefs", {
        "profile.managed_default_content_settings.images": 2
    })
    driver = webdriver.Chrome(options=opts)  # Selenium Manager resolve o driver
    driver.set_page_load_timeout(60)
    driver.implicitly_wait(2)
    return driver

# =========================================
# 2.1) Definir CEP da cidade na UI
# =========================================
def set_cep(driver: webdriver.Chrome, cep: str, timeout: int = 25):
    driver.get("https://mercado.carrefour.com.br/")
    wait = WebDriverWait(driver, timeout)

    # Tentar abrir o modal do CEP
    opened = False
    open_btns = [
        ("css", 'button[aria-label*="CEP"]'),
        ("css", 'button[data-testid*="address"], button[data-testid*="location"]'),
        ("xpath", '//button[contains(., "Informe seu CEP")]'),
        ("xpath", '//button[contains(., "Alterar endereço") or contains(., "Trocar endereço")]'),
        ("css", 'button[aria-label*="endereço"]'),
    ]
    for how, sel in open_btns:
        try:
            locator = (By.CSS_SELECTOR, sel) if how == "css" else (By.XPATH, sel)
            btn = wait.until(EC.element_to_be_clickable(locator))
            btn.click()
            opened = True
            break
        except Exception:
            continue
    if not opened:
        # plano B via JS
        driver.execute_script("""
            const b = document.querySelector('button[aria-label*="CEP"], button[data-testid*="address"], button[data-testid*="location"], button[aria-label*="endereço"]');
            if (b) b.click();
        """)
        time.sleep(1)

    # Campo de CEP
    cep_input = None
    for by, sel in [
        (By.CSS_SELECTOR, 'input[name*="cep"], input[id*="cep"], input[name*="zip"], input[id*="zip"]'),
        (By.XPATH, '//input[@type="text" and (contains(@name,"cep") or contains(@id,"cep") or contains(@name,"zip") or contains(@id,"zip") or contains(@placeholder, "CEP"))]'),
    ]:
        try:
            cep_input = wait.until(EC.visibility_of_element_located((by, sel)))
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

    # Confirmar
    confirmed = False
    for by, sel in [
        (By.CSS_SELECTOR, 'button[type="submit"]'),
        (By.XPATH, '//button[contains(., "Confirmar") or contains(., "Continuar") or contains(., "OK")]'),
    ]:
        try:
            btn = wait.until(EC.element_to_be_clickable((by, sel)))
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

    time.sleep(2.5)
    print(f"📍 CEP definido: {cep}")

# =====================================
# 3) Scraper: lê JSON-LD do tipo Product
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
                continue

            objs = data if isinstance(data, list) else [data]
            for obj in objs:
                if isinstance(obj, dict) and obj.get("@type") == "Product":
                    name = obj.get("name", "Não encontrado")
                    offers = obj.get("offers", {})
                    price = None
                    if isinstance(offers, dict):
                        # tenta price, lowPrice, ou priceSpecification.price
                        price = offers.get("price") or offers.get("lowPrice")
                        spec = offers.get("priceSpecification") or {}
                        if price is None and isinstance(spec, dict):
                            price = spec.get("price")
                    elif isinstance(offers, list) and offers:
                        cand = []
                        for it in offers:
                            if isinstance(it, dict):
                                cand.extend([it.get("price"), it.get("lowPrice")])
                        price = next((c for c in cand if c), None)

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
# 4) URLs (sua lista aqui)
# =========================
URLS = [
    'https://mercado.carrefour.com.br/arroz-branco-longofino-tipo-1-tio-joao-2kg-115657/p',
    'https://mercado.carrefour.com.br/feijao-carioca-tipo-1-kicaldo-1kg-466506/p',
    # ... (mantenha o restante da sua lista)
]

# =========================================
# 5) Salvar Excel (por cidade/mês)
# =========================================
def salvar_excel_cidade(cidade_slug: str, df_ok: pd.DataFrame, df_err: pd.DataFrame):
    city_dir = os.path.join(DATA_DIR, cidade_slug)
    os.makedirs(city_dir, exist_ok=True)

    arq_mensal = os.path.join(city_dir, f"precos_carrefour_{STAMP_MONTH}.xlsx")
    arq_erros  = os.path.join(city_dir, f"erros_carrefour_{STAMP_MONTH}.xlsx")

    print(f"📝 Caminho do Excel mensal: {os.path.abspath(arq_mensal)}")

    # Montar base (catálogo do mês)
    base = None
    if os.path.exists(arq_mensal):
        try:
            base = pd.read_excel(arq_mensal, sheet_name="Precos", engine="openpyxl")
            if "Nome do Produto" not in base.columns:
                nomes_ref = pd.concat([df_ok[["Nome do Produto"]], df_err[["Nome do Produto"]]], ignore_index=True).dropna().drop_duplicates()
                base = nomes_ref
        except Exception as e:
            print(f"⚠️ Erro lendo {arq_mensal}: {e}. Vou reconstruir a base.")
            base = None

    if base is None:
        nomes_ref = pd.concat([df_ok[["Nome do Produto"]], df_err[["Nome do Produto"]]], ignore_index=True).dropna().drop_duplicates()
        if nomes_ref.empty:
            nomes_ref = pd.DataFrame({"Nome do Produto": []})
        base = nomes_ref.copy()

    # Construir coluna do dia alinhada por nome
    if COLUNA_DIA in base.columns:
        base.drop(columns=[COLUNA_DIA], inplace=True)

    col_dia = pd.Series(index=base["Nome do Produto"], dtype="float64", name=COLUNA_DIA)
    if not df_ok.empty:
        valores = df_ok.set_index("Nome do Produto")["Preço"]
        col_dia.loc[valores.index] = valores
    base = base.merge(col_dia.reset_index(), on="Nome do Produto", how="left")

    # Escrever planilha
    try:
        with pd.ExcelWriter(arq_mensal, engine="xlsxwriter", mode="w") as w:
            base.to_excel(w, index=False, sheet_name="Precos")
        print(f"✅ [{cidade_slug}] Excel atualizado com a coluna {COLUNA_DIA}. Linhas: {len(base)}")
    except Exception as e:
        print(f"❌ Falha ao escrever {arq_mensal}: {e}")

    # Log de erros do mês
    if not df_err.empty:
        df_err = df_err.copy()
        df_err["Data"] = today.strftime("%Y-%m-%d")
        try:
            if os.path.exists(arq_erros):
                be = pd.read_excel(arq_erros, engine="openpyxl")
                be = pd.concat([be, df_err], ignore_index=True)
            else:
                be = df_err
            with pd.ExcelWriter(arq_erros, engine="xlsxwriter", mode="w") as w:
                be.to_excel(w, index=False, sheet_name="Erros")
            print(f"⚠️ [{cidade_slug}] Erros/zeros adicionados: {os.path.abspath(arq_erros)} (+{len(df_err)})")
        except Exception as e:
            print(f"❌ Falha ao escrever log de erros {arq_erros}: {e}")
    else:
        print(f"✅ [{cidade_slug}] Sem erros hoje.")

# =========================
# 6) Execução principal
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
                registros.append(scrape_product_via_json(url, driver))
                time.sleep(0.8)

            df_total = pd.DataFrame(registros)
            df_ok  = df_total[df_total["Preço"].fillna(0) > 0][["Nome do Produto", "Preço"]].copy()
            df_err = df_total[df_total["Preço"].fillna(0) <= 0].copy()

            print(f"[{cidade_slug}] Capturados: {len(df_total)} | OK: {len(df_ok)} | Erros/zeros: {len(df_err)}")
            if not df_ok.empty:
                print(df_ok.head(3).to_string(index=False))

            salvar_excel_cidade(cidade_slug, df_ok, df_err)
    finally:
        driver.quit()

if __name__ == "__main__":
    main()
